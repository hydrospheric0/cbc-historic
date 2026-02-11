import './style.css';
import 'leaflet/dist/leaflet.css';
import L from 'leaflet';
import leafletMarker2x from 'leaflet/dist/images/marker-icon-2x.png?url';
import leafletMarker from 'leaflet/dist/images/marker-icon.png?url';
import leafletMarkerShadow from 'leaflet/dist/images/marker-shadow.png?url';

let plotlyPromise = null;
let plotlyRef = null;
async function getPlotly() {
  if (!plotlyPromise) {
    plotlyPromise = import('plotly.js-basic-dist-min').then((m) => {
      plotlyRef = m?.default ?? m;
      return plotlyRef;
    });
  }
  return plotlyPromise;
}

let sqlPromise = null;
function getSql() {
  if (!sqlPromise) {
    sqlPromise = Promise.all([import('sql.js'), import('sql.js/dist/sql-wasm.wasm?url')]).then(
      ([sqlModule, wasmUrlModule]) => {
        const initSqlJs = sqlModule?.default ?? sqlModule;
        const sqlWasmUrl = wasmUrlModule?.default ?? wasmUrlModule;
        return initSqlJs({
          locateFile: () => sqlWasmUrl,
        });
      }
    );
  }
  return sqlPromise;
}

const app = document.querySelector('#app');

const _urlParams = new URLSearchParams(window.location.search);
const POPOUT_MODE = _urlParams.get('popout');
const POPOUT_ID = _urlParams.get('popoutId');
const IS_POPOUT_TABLE = POPOUT_MODE === 'table';
const IS_POPOUT_PLOT = POPOUT_MODE === 'plot';
const IS_POPOUT = IS_POPOUT_TABLE || IS_POPOUT_PLOT;

const POPOUT_CHANNEL_NAME = 'cbc-historic.popout.v1';
const popoutChannel = typeof BroadcastChannel !== 'undefined' ? new BroadcastChannel(POPOUT_CHANNEL_NAME) : null;
const pendingPopouts = new Map();

try {
  document.body.classList.toggle('is-popout', IS_POPOUT);
  document.body.classList.toggle('is-popout-table', IS_POPOUT_TABLE);
  document.body.classList.toggle('is-popout-plot', IS_POPOUT_PLOT);
} catch {
}

function makePopoutId() {
  try {
    if (globalThis.crypto?.randomUUID) return globalThis.crypto.randomUUID();
  } catch {
  }
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function buildPopoutUrl(mode, id) {
  const u = new URL(window.location.href);
  u.searchParams.set('popout', mode);
  u.searchParams.set('popoutId', id);
  return u.toString();
}

function safeClone(obj) {
  try {
    if (typeof structuredClone === 'function') return structuredClone(obj);
  } catch {
  }
  try {
    return JSON.parse(JSON.stringify(obj));
  } catch {
    return null;
  }
}

const CBC_RESULTS_URL = 'https://netapp.audubon.org/CBCObservation/Historical/ResultsByCount.aspx';
const CBC_HISTORICAL_RESULTS_URL = 'https://netapp.audubon.org/CBCObservation/Reports/HistoricalResultsByCount.aspx';
const CBC_MAP_URL = 'https://gis.audubon.org/christmasbirdcount/';
const CBC_PORTAL_URL = 'https://netapp.audubon.org/aap/application/cbc';
const CBC_126_CIRCLES_QUERY_URL =
  'https://services1.arcgis.com/lDFzr3JyGEn5Eymu/arcgis/rest/services/CBC_126/FeatureServer/0/query';
const MAX_CSV_BYTES = 10 * 1024 * 1024;
const CURRENT_MAX_COUNT_INDEX = 125;

const SEED_CODES = ['CAPC', 'CARC', 'WAPT'];

function normalizeWorkerBaseUrl(raw) {
  const s = cleanText(raw || '');
  if (!s) return '';
  return s.replace(/\/+$/, '');
}

const DEFAULT_CBC_WORKER_BASE_URL = 'https://cbc-historic-cbc-proxy.cbc-weather.workers.dev';
const CBC_WORKER_BASE_URL = normalizeWorkerBaseUrl(import.meta?.env?.VITE_CBC_WORKER_BASE || DEFAULT_CBC_WORKER_BASE_URL);

function buildWorkerCsvDownloadUrl({ abbrev, cid, sy, ey }) {
  if (!CBC_WORKER_BASE_URL) return null;
  const code = cleanText(abbrev || '');
  const cidNum = typeof cid === 'number' ? cid : parseInt(String(cid || '').trim(), 10);
  const syNum = typeof sy === 'number' ? sy : parseInt(String(sy || '').trim(), 10);
  const eyNum = typeof ey === 'number' ? ey : parseInt(String(ey || '').trim(), 10);
  if (!code || !Number.isFinite(cidNum)) return null;
  const params = new URLSearchParams();
  params.set('abbrev', code);
  params.set('cid', String(cidNum));
  params.set('sy', String(Number.isFinite(syNum) ? syNum : 1));
  params.set('ey', String(Number.isFinite(eyNum) ? eyNum : CURRENT_MAX_COUNT_INDEX));
  return `${CBC_WORKER_BASE_URL}/cbc/circle?${params.toString()}`;
}

async function fetchCircleCsvTextFromWorker({ abbrev, cid, sy, ey }) {
  const url = buildWorkerCsvDownloadUrl({ abbrev, cid, sy, ey });
  if (!url) throw new Error('CSV proxy is not configured. Set VITE_CBC_WORKER_BASE and rebuild the site.');

  const r = await fetch(url, { method: 'GET', credentials: 'omit' });
  if (!r.ok) {
    const hint = r.status === 403 ? ' (origin not allowed by worker CORS)' : '';
    throw new Error(`CSV proxy request failed: ${r.status}${hint}`);
  }

  const len = parseInt(r.headers.get('content-length') || '', 10);
  if (Number.isFinite(len) && len > MAX_CSV_BYTES) throw new Error('CSV file is too large.');

  const text = await r.text();
  if (text.length > MAX_CSV_BYTES) throw new Error('CSV file is too large.');
  return text;
}

function openUrlInNewTab(url) {
  if (!url) return;
  const a = document.createElement('a');
  a.href = url;
  a.target = '_blank';
  a.rel = 'noopener noreferrer';
  document.body.appendChild(a);
  a.click();
  a.remove();
}

async function downloadAndIngestCircle({ abbrev, cid, name }) {
  const code = cleanText(abbrev || '');
  const cidNum = typeof cid === 'number' ? cid : parseInt(String(cid || '').trim(), 10);
  if (!code || !Number.isFinite(cidNum)) throw new Error('Missing count circle code or ID.');

  const workerUrl = buildWorkerCsvDownloadUrl({ abbrev: code, cid: cidNum, sy: 1, ey: CURRENT_MAX_COUNT_INDEX });
  if (!workerUrl) {
    throw new Error('CSV proxy is not configured. Set VITE_CBC_WORKER_BASE and rebuild the site.');
  }

  const csvText = await fetchCircleCsvTextFromWorker({ abbrev: code, cid: cidNum, sy: 1, ey: CURRENT_MAX_COUNT_INDEX });
  const filenameBase = [name, code, cidNum].filter((x) => x).join('_').replace(/\s+/g, '_');
  const f = new File([csvText], `${filenameBase || code}.csv`, { type: 'text/csv' });
  await handleFile(f);
}

function u8ToArrayBuffer(u8) {
  if (!u8) return null;
  if (u8 instanceof ArrayBuffer) return u8;
  if (ArrayBuffer.isView(u8)) {
    return u8.buffer.slice(u8.byteOffset, u8.byteOffset + u8.byteLength);
  }
  return null;
}

async function buildSqliteBytesFromParsed(parsed) {
  const SQL = await getSql();
  const db = new SQL.Database();

  db.run('CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)');
  const put = db.prepare('INSERT OR REPLACE INTO kv(key, value) VALUES(?, ?)');
  const saveJson = (key, value) => {
    put.run([key, JSON.stringify(value)]);
  };

  saveJson('countInfo', parsed?.countInfo || null);
  saveJson('ranges', parsed?.ranges || null);
  saveJson('years', parsed?.years || []);
  saveJson('yearsFull', parsed?.yearsFull || []);
  saveJson('missingYears', parsed?.missingYears || []);
  saveJson('meta', parsed?.meta || []);
  saveJson('sourceUrl', parsed?.sourceUrl || null);
  saveJson('maxCountIndex', parsed?.maxCountIndex || null);
  saveJson('species', parsed?.species || []);
  saveJson('weather', parsed?.weather || []);
  saveJson('effort', parsed?.effort || []);
  saveJson('participation', parsed?.participation || []);
  saveJson('savedAt', new Date().toISOString());
  put.free();

  const bytes = db.export();
  db.close();
  return bytes;
}

async function storeSqliteBytesForCode(code, sqliteBytes) {
  const needle = cleanText(code || '');
  const ab = u8ToArrayBuffer(sqliteBytes);
  if (!needle || !ab) throw new Error('Missing code or database bytes.');

  await idbSet(`${IDB_KEY_DB_PREFIX}${needle}`, ab);

  let idx;
  try {
    idx = await loadCountsIndex();
  } catch {
    idx = [];
  }

  const next = (idx || []).filter((r) => r?.code !== needle);
  try {
    const row = await buildIndexRowFromSqliteBytes(needle, ab);
    next.push(row);
  } catch {
    const now = Date.now();
    next.push({ code: needle, name: needle, range: '—', maxCountIndex: null, updatedAt: now });
  }

  await saveCountsIndex(next);
  renderIngestedCountsList(next);
}

async function updateStoredCircleFromWorker({ abbrev, cid, name }) {
  const code = cleanText(abbrev || '');
  const cidNum = typeof cid === 'number' ? cid : parseInt(String(cid || '').trim(), 10);
  if (!code || !Number.isFinite(cidNum)) throw new Error('Missing count circle code or ID.');

  const workerUrl = buildWorkerCsvDownloadUrl({ abbrev: code, cid: cidNum, sy: 1, ey: CURRENT_MAX_COUNT_INDEX });
  if (!workerUrl) throw new Error('CSV proxy is not configured. Set VITE_CBC_WORKER_BASE and rebuild the site.');

  setDownloadingIndicator(true);
  try {
    const csvText = await fetchCircleCsvTextFromWorker({ abbrev: code, cid: cidNum, sy: 1, ey: CURRENT_MAX_COUNT_INDEX });
    const filenameBase = [name, code, cidNum].filter((x) => x).join('_').replace(/\s+/g, '_');
    const f = new File([csvText], `${filenameBase || code}.csv`, { type: 'text/csv' });
    const parsed = await parseUploadedFile(f);
    const sqliteBytes = await buildSqliteBytesFromParsed(parsed);
    await storeSqliteBytesForCode(code, sqliteBytes);
  } finally {
    setDownloadingIndicator(false);
  }
}

const KNOWN_COUNT_IDS = {
  CAPC: 57023,
  CASM: 57049,
  CACC: 57441,
  CARC: 58818,
  WAPT: 54929,
};

function buildDefaultCsvDownloadUrl({ abbrev, cid }) {
  const code = cleanText(abbrev || '');
  const cidNum = typeof cid === 'number' ? cid : parseInt(String(cid || '').trim(), 10);
  if (!code || !Number.isFinite(cidNum)) return null;
  const params = new URLSearchParams();
  params.set('rf', 'CSV');
  params.set('cid', String(cidNum));
  params.set('sy', '1');
  params.set('ey', String(CURRENT_MAX_COUNT_INDEX));
  params.set('so', '0');
  params.set('abbrev', code);
  return `${CBC_HISTORICAL_RESULTS_URL}?${params.toString()}`;
}

function formatShortDate(ts) {
  const n = typeof ts === 'number' ? ts : parseInt(String(ts || '').trim(), 10);
  if (!Number.isFinite(n) || n <= 0) return null;
  const d = new Date(n);
  if (!Number.isFinite(d.getTime())) return null;
  const mm = d.getMonth() + 1;
  const dd = d.getDate();
  const yy = String(d.getFullYear() % 100).padStart(2, '0');
  return `${mm}/${dd}/${yy}`;
}

const mainTemplate = `
  <div class="app">
    <header class="topbar">
      <div class="topbarTitle">Christmas Bird Count Historical Data</div>
      <button id="infoBtn" class="infoButton" type="button" aria-label="Info">i</button>
    </header>

    <div class="content-row">

      <div id="layoutGrid" class="layout-grid">
        <div class="layout-cell layout-top-left">
          <div class="layout-stack">
            <div class="count-header card nav-header">
              <div class="count-header-bar">
                <div class="count-header-text">Navigation</div>
                <div></div>
              </div>
            </div>
            <div class="sidebar-card nav-card card">
              <div class="cardBody">
                <div class="count-search">
                </div>
                <div class="map-card">
                  <div class="map-body">
                    <div id="map" class="map">
                      <div class="empty">No location loaded.</div>
                    </div>
                  </div>
                </div>

                <div class="count-search">
                  <input
                    id="countSearch"
                    class="count-search-input"
                    type="text"
                    placeholder="Type count name or code (e.g. Putah, CAPC, Cosumnes)"
                    autocomplete="off"
                    spellcheck="false"
                  />
                  <div id="countSearchResults" class="count-search-results" aria-label="Search results"></div>
                </div>

                <div class="section-title">Available data</div>
                <div id="ingested" class="summary">
                  <div class="empty">None yet.</div>
                </div>

                <div class="section-title">Summary</div>
                <div id="summary" class="summary">
                  <div class="empty">No CSV loaded.</div>
                </div>

                <div class="section-title">Resources</div>
                <div class="resources-links">
                  <a class="link" href="${CBC_RESULTS_URL}" target="_blank" rel="noopener noreferrer">Download Audubon CBC Count Results</a>
                  <a class="link" href="${CBC_MAP_URL}" target="_blank" rel="noopener noreferrer">Audubon CBC map</a>
                  <a class="link" href="${CBC_PORTAL_URL}" target="_blank" rel="noopener noreferrer">Audubon Application Portal</a>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div
          id="gutterX"
          class="gutter gutter-horizontal layout-gutter-x"
          role="separator"
          aria-orientation="vertical"
          aria-label="Resize columns"
          tabindex="0"
        ></div>

        <div class="layout-cell layout-top-right">
          <div class="layout-right-top">
            <div id="countHeader" class="count-header card">
              <div class="count-header-bar">
                <div id="countHeaderText" class="count-header-text">Load a CSV or select existing count data</div>
                <div class="tabs" role="tablist" aria-label="Tables">
                  <button id="tabSpecies" class="tab-button active" type="button">Species</button>
                  <button id="tabWeather" class="tab-button" type="button">Weather</button>
                  <button id="tabEffort" class="tab-button" type="button">Effort</button>
                  <button id="tabParticipation" class="tab-button" type="button">Participation</button>
                </div>
                <div id="downloadIndicator" class="download-indicator hidden" aria-live="polite">Downloading…</div>
              </div>
            </div>

            <div class="table-pane card">
              <div id="panelHeader" class="panel-header"></div>
              <div id="panel" class="panel">
                <div class="empty">Table appears here when a CSV is loaded.</div>
              </div>
            </div>
          </div>
        </div>

        <div
          id="gutterY"
          class="gutter gutter-vertical layout-gutter-y"
          role="separator"
          aria-orientation="horizontal"
          aria-label="Resize rows"
          tabindex="0"
        ></div>

        <div class="layout-cell layout-bottom-left"></div>

        <div class="layout-cell layout-bottom-right">
          <div class="plot-pane card">
            <div class="plot-bar">
              <div class="plot-overlay-controls">
                <select id="plotSpeciesSelect" class="panel-select" data-action="plot-species-select"></select>
                <button id="plotExportOverlayBtn" class="popout-button" type="button" aria-label="Export CSV" title="Export CSV">⤓</button>
                <button id="plotPopoutOverlayBtn" class="popout-button" type="button" aria-label="Pop out plot" title="Pop out">⤢</button>
              </div>
              <div class="plot-bar-spacer"></div>
            </div>
            <div id="plot" class="plot">
              <div class="empty">Click on a species above to plot</div>
            </div>
            <div id="plotCircleHint" class="plot-circle-hint">Click on a circle to view/download data</div>
          </div>
        </div>
      </div>
    </div>

    <footer class="footerbar">
      <a class="footerLink" href="https://buymeacoffee.com/bartg" target="_blank" rel="noreferrer">
        <img
          class="bmcButton"
          src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png"
          alt="Buy Me a Coffee"
        />
      </a>
    </footer>

    <div id="infoModal" class="modalOverlay hidden" role="dialog" aria-modal="true" aria-labelledby="infoModalTitle">
      <div class="modalCard" role="document">
        <div class="modalHeader">
          <div id="infoModalTitle">Information</div>
          <button id="infoModalClose" class="modalClose" type="button" aria-label="Close">×</button>
        </div>
        <div class="modalBody">
          <p>
            The Christmas Bird Count is the nation’s longest-running community science bird project. It runs annually between December 14 and January 5. The count currently includes over 3000 count circles. More information can be found on the Audubon website.
          </p>
          <p style="margin-top: 10px; font-weight: 700;">This tool was developed to help count circle compilers and other interested parties by:</p>
          <ul>
            <li>
              Fetching historical data per circle from the Audubon CBC historical results
            </li>
            <li>Allow quick visualization of historical count data</li>
            <li>Saving a local cache for faster repeat visits</li>
          </ul>

          <p style="margin-top: 12px; font-weight: 800;">How to use</p>
          <p>
            Click a circle on the map (or search by name/code) to load data. The app downloads that circle’s data, stores it locally in your browser, and fills the tables and plot. Use the ↻ Update button in Available data to refresh a circle.
          </p>
          <p style="margin-top: 12px; font-weight: 800;">
            <a href="https://github.com/hydrospheric0/cbc-historic/" target="_blank" rel="noreferrer">View the source code on GitHub</a>
          </p>
          <p style="margin-top: 12px; font-weight: 700;">If you find this tool useful, please consider supporting its development:</p>
          <div class="sponsorBlock">
            <a class="sponsorButton" href="https://buymeacoffee.com/bartg" target="_blank" rel="noreferrer">
              <img
                class="bmcButton"
                src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png"
                alt="Buy Me a Coffee"
              />
            </a>
          </div>
        </div>
      </div>
    </div>
  </div>
`;

const tablePopoutTemplate = `
  <div class="app">
    <div class="content-row">
      <div class="table-pane card" style="width: 100%; height: 100%;">
        <div id="panelHeader" class="panel-header"></div>
        <div id="panel" class="panel">
          <div class="empty">Select a circle to load data</div>
        </div>
      </div>
    </div>
  </div>
`;

const plotPopoutTemplate = `
  <div class="app">
    <div class="content-row">
      <div class="plot-pane card" style="width: 100%; height: 100%;">
        <div class="plot-bar">
          <div class="plot-overlay-controls">
            <select id="plotSpeciesSelect" class="panel-select" data-action="plot-species-select"></select>
          </div>
          <div class="plot-bar-spacer"></div>
        </div>
        <div id="plot" class="plot">
          <div class="empty">Click on a species above to plot</div>
        </div>
      </div>
    </div>
  </div>
`;

app.innerHTML = IS_POPOUT_TABLE ? tablePopoutTemplate : IS_POPOUT_PLOT ? plotPopoutTemplate : mainTemplate;

const dropzoneEl = document.getElementById('dropzone');
const fileInputEl = document.getElementById('fileInput');
const ingestedEl = document.getElementById('ingested');
const summaryEl = document.getElementById('summary');
const mapEl = document.getElementById('map');
const countHeaderEl = document.getElementById('countHeader');
const countHeaderTextEl = document.getElementById('countHeaderText');
const downloadIndicatorEl = document.getElementById('downloadIndicator');
const panelHeaderEl = document.getElementById('panelHeader');
const panelEl = document.getElementById('panel');
const plotHeaderEl = document.getElementById('plotHeader');
const plotEl = document.getElementById('plot');
const plotPopoutOverlayBtnEl = document.getElementById('plotPopoutOverlayBtn');
const plotExportOverlayBtnEl = document.getElementById('plotExportOverlayBtn');
const plotSpeciesSelectEl = document.getElementById('plotSpeciesSelect');
const plotCircleHintEl = document.getElementById('plotCircleHint');

let plotMountEl = null;
let plotEmptyEl = null;

const countSearchEl = document.getElementById('countSearch');
const countSearchResultsEl = document.getElementById('countSearchResults');
const countSearchSelectedEl = document.getElementById('countSearchSelected');

const navCardEl = document.querySelector('.nav-card');
const mapCardEl = document.querySelector('.map-card');
const resourcesCardEl = document.querySelector('.resources-card');

let lastIngestedIndex = null;

panelHeaderEl?.addEventListener('click', (e) => {
  const dlBtn = e.target?.closest?.('[data-action="export-table"]');
  if (dlBtn) {
    e.preventDefault();
    if (!state.species) return;
    const cfg = getTableConfig(activeTab);
    const csv = rowsToCsv(cfg.rows || [], cfg.columns || []);
    const ci = state.countInfo || {};
    const base = sanitizeFilenamePart(ci.CountCode || ci.CountName || 'cbc');
    const tab = sanitizeFilenamePart(cfg.title || activeTab);
    downloadCsv(`${base}_${tab}.csv`, csv);
    return;
  }

  const popBtn = e.target?.closest?.('[data-action="popout-table"]');
  if (popBtn) {
    e.preventDefault();
    openPopout('table');
    return;
  }

  const filterBtn = e.target?.closest?.('[data-action="toggle-species-filter"]');
  if (filterBtn) {
    const which = filterBtn.getAttribute('data-filter');
    if (which === 'rare') {
      const next = !state.speciesFilterRare;
      state.speciesFilterRare = next;
      if (next) state.speciesFilterOwls = false;
    } else if (which === 'owls') {
      const next = !state.speciesFilterOwls;
      state.speciesFilterOwls = next;
      if (next) state.speciesFilterRare = false;
    }
    renderPanel(activeTab);
  }
});

plotPopoutOverlayBtnEl?.addEventListener('click', (e) => {
  e.preventDefault();
  openPopout('plot');
});

plotExportOverlayBtnEl?.addEventListener('click', (e) => {
  e.preventDefault();
  if (!state.species) return;
  const cfg = getTableConfig(activeTab);
  const csv = rowsToCsv(cfg.rows || [], cfg.columns || []);
  const ci = state.countInfo || {};
  const base = sanitizeFilenamePart(ci.CountCode || ci.CountName || 'cbc');
  const tab = sanitizeFilenamePart(cfg.title || activeTab);
  downloadCsv(`${base}_${tab}.csv`, csv);
});

function renderPlotSpeciesOverlay() {
  if (!plotSpeciesSelectEl) return;
  const rows = (state?.species || []).filter((r) => !isSpRecord(r?.Species || ''));
  const names = rows
    .map((r) => cleanText(r?.Species || ''))
    .filter((s) => s)
    .sort((a, b) => stripBracketedText(a).localeCompare(stripBracketedText(b), undefined, { sensitivity: 'base' }));

  const current = cleanText(state?.selectedSpecies || '');
  const options = ['<option value="">Select species…</option>']
    .concat(
      names.map((s) => {
        const selected = s === current ? ' selected' : '';
        return `<option value="${escapeHtml(s)}"${selected}>${escapeHtml(stripBracketedText(s))}</option>`;
      })
    )
    .join('');
  plotSpeciesSelectEl.innerHTML = options;
}

plotSpeciesSelectEl?.addEventListener('change', (e) => {
  const sel = e.target;
  const next = cleanText(sel.value || '');
  if (!next) return;
  state.selectedSpecies = next;
  setActiveTab('species');
});

function syncSidebarHeights() {
  return;
}

L.Icon.Default.mergeOptions({
  iconRetinaUrl: leafletMarker2x,
  iconUrl: leafletMarker,
  shadowUrl: leafletMarkerShadow,
});
try {
  delete L.Icon.Default.prototype._getIconUrl;
} catch {
}

let leafletMap = null;
let leafletMapMarker = null;
let leafletCircle = null;
let leafletAllCirclesLayer = null;
let leafletCircleByCode = new Map();
let leafletBaseLayerControl = null;
let leafletBaseLayers = null;
const LEAFLET_CIRCLES_PANE = 'cbcCircles';
let circlesGeometryPromise = null;
let circlesGeometry = null;
let lastActiveCircleCode = null;

let storedCountCodes = new Set();

const GRID_COL_KEY = 'cbc-historic.grid.leftPx.v1';
const GRID_ROW_KEY = 'cbc-historic.grid.topPx.v1';

let _layoutResizeQueued = false;
function requestLayoutResize() {
  if (_layoutResizeQueued) return;
  _layoutResizeQueued = true;
  requestAnimationFrame(() => {
    _layoutResizeQueued = false;
    try {
      schedulePlotResize();
    } catch {
    }
    try {
      if (leafletMap) leafletMap.invalidateSize();
    } catch {
    }
  });
}

function readStoredPx(key) {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return null;
    const n = Number(raw);
    return Number.isFinite(n) ? n : null;
  } catch {
    return null;
  }
}

function writeStoredPx(key, value) {
  try {
    localStorage.setItem(key, String(Math.round(value)));
  } catch {
  }
}

function initGridSplit() {
  const layoutEl = document.getElementById('layoutGrid');
  const gutterX = document.getElementById('gutterX');
  const gutterY = document.getElementById('gutterY');
  if (!layoutEl || !gutterX || !gutterY) return;

  const GUTTER_PX = 8;
  const MIN_LEFT_PX = 280;
  const MIN_RIGHT_PX = 520;
  const MIN_TOP_PX = 260;
  const MIN_BOTTOM_PX = 220;

  function clampAndApply(nextLeftPx, nextTopPx) {
    const rect = layoutEl.getBoundingClientRect();
    const maxLeft = Math.max(MIN_LEFT_PX, rect.width - GUTTER_PX - MIN_RIGHT_PX);
    const maxTop = Math.max(MIN_TOP_PX, rect.height - GUTTER_PX - MIN_BOTTOM_PX);

    const leftPx = Math.min(Math.max(nextLeftPx, MIN_LEFT_PX), maxLeft);
    const topPx = Math.min(Math.max(nextTopPx, MIN_TOP_PX), maxTop);

    layoutEl.style.setProperty('--layout-gutter', `${GUTTER_PX}px`);
    layoutEl.style.setProperty('--left-w', `${Math.round(leftPx)}px`);
    layoutEl.style.setProperty('--top-h', `${Math.round(topPx)}px`);
    return { leftPx, topPx };
  }

  const rect = layoutEl.getBoundingClientRect();
  const defaultLeft = Math.round(rect.width * 0.2);
  const defaultTop = Math.round(rect.height * 0.6);
  let leftPx = readStoredPx(GRID_COL_KEY) ?? defaultLeft;
  let topPx = readStoredPx(GRID_ROW_KEY) ?? defaultTop;

  ({ leftPx, topPx } = clampAndApply(leftPx, topPx));
  requestLayoutResize();

  function startDrag(axis, ev) {
    ev.preventDefault();
    const startX = ev.clientX;
    const startY = ev.clientY;
    const startLeft = leftPx;
    const startTop = topPx;
    document.body.classList.add('resizing');

    const target = axis === 'x' ? gutterX : gutterY;
    try {
      target.setPointerCapture(ev.pointerId);
    } catch {
    }

    function onMove(e) {
      if (axis === 'x') {
        const next = startLeft + (e.clientX - startX);
        ({ leftPx, topPx } = clampAndApply(next, topPx));
      } else {
        const next = startTop + (e.clientY - startY);
        ({ leftPx, topPx } = clampAndApply(leftPx, next));
      }
      requestLayoutResize();
    }

    function onEnd(e) {
      document.body.classList.remove('resizing');
      try {
        target.releasePointerCapture(e.pointerId);
      } catch {
      }
      writeStoredPx(GRID_COL_KEY, leftPx);
      writeStoredPx(GRID_ROW_KEY, topPx);
      target.removeEventListener('pointermove', onMove);
      target.removeEventListener('pointerup', onEnd);
      target.removeEventListener('pointercancel', onEnd);
      requestLayoutResize();
    }

    target.addEventListener('pointermove', onMove);
    target.addEventListener('pointerup', onEnd);
    target.addEventListener('pointercancel', onEnd);
  }

  gutterX.addEventListener('pointerdown', (e) => startDrag('x', e));
  gutterY.addEventListener('pointerdown', (e) => startDrag('y', e));

  window.addEventListener('resize', () => {
    ({ leftPx, topPx } = clampAndApply(leftPx, topPx));
    requestLayoutResize();
  });
}
let _sidebarSyncQueued = false;
function requestSidebarSync() {
  if (_sidebarSyncQueued) return;
  _sidebarSyncQueued = true;
  requestAnimationFrame(() => {
    _sidebarSyncQueued = false;
    requestLayoutResize();
  });
}

function ensureLeafletMap() {
  if (!mapEl) return null;
  if (leafletMap) return leafletMap;
  mapEl.innerHTML = '';

  leafletMap = L.map(mapEl, {
    zoomControl: true,
    attributionControl: true,
  }).setView([37.5, -120.5], 6);

  const osm = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 22,
    maxNativeZoom: 19,
    attribution: '&copy; OpenStreetMap contributors',
  });

  const esriSatellite = L.tileLayer(
    'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
    {
      maxZoom: 22,
      maxNativeZoom: 19,
      attribution: 'Tiles &copy; Esri',
    }
  );

  const esriTopo = L.tileLayer(
    'https://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/{z}/{y}/{x}',
    {
      maxZoom: 22,
      maxNativeZoom: 19,
      attribution: 'Tiles &copy; Esri',
    }
  );

  const tileErr = {
    lastAt: 0,
    burst: 0,
    activeName: 'Esri Topo',
  };

  const noteActiveBaseLayer = (name) => {
    tileErr.activeName = name;
    tileErr.burst = 0;
    tileErr.lastAt = 0;
  };

  const switchToOsmIfActive = () => {
    if (!leafletMap) return;
    if (tileErr.activeName === 'OpenStreetMap') return;
    try {
      if (leafletMap.hasLayer(esriTopo)) leafletMap.removeLayer(esriTopo);
    } catch {
    }
    try {
      if (leafletMap.hasLayer(esriSatellite)) leafletMap.removeLayer(esriSatellite);
    } catch {
    }
    try {
      osm.addTo(leafletMap);
      noteActiveBaseLayer('OpenStreetMap');
    } catch {
    }
  };

  const attachTileErrorFallback = (layer, layerName) => {
    if (!layer || !layer.on) return;
    layer.on('tileerror', () => {
      if (tileErr.activeName !== layerName) return;
      const now = Date.now();
      if (tileErr.lastAt && now - tileErr.lastAt < 8000) tileErr.burst += 1;
      else tileErr.burst = 1;
      tileErr.lastAt = now;
      if (tileErr.burst >= 4) switchToOsmIfActive();
    });
  };

  attachTileErrorFallback(esriTopo, 'Esri Topo');
  attachTileErrorFallback(esriSatellite, 'Esri Satellite');

  leafletBaseLayers = {
    'Esri Topo': esriTopo,
    'Esri Satellite': esriSatellite,
    OpenStreetMap: osm,
  };

  esriTopo.addTo(leafletMap);
  noteActiveBaseLayer('Esri Topo');
  leafletBaseLayerControl = L.control.layers(leafletBaseLayers, null, { position: 'topright', collapsed: false });
  leafletBaseLayerControl.addTo(leafletMap);

  leafletMap.on('baselayerchange', (e) => {
    const name = e?.name || '';
    if (name === 'Esri Topo' || name === 'Esri Satellite' || name === 'OpenStreetMap') noteActiveBaseLayer(name);
  });

  try {
    if (!leafletMap.getPane(LEAFLET_CIRCLES_PANE)) {
      const pane = leafletMap.createPane(LEAFLET_CIRCLES_PANE);
      pane.style.zIndex = '450';
    }
  } catch {
  }

  try {
    setTimeout(() => {
      try {
        leafletMap?.invalidateSize();
      } catch {
      }
    }, 0);
  } catch {
  }

  requestSidebarSync();
  return leafletMap;
}

try {
  const tablePaneEl = document.querySelector('.table-pane');
  if (tablePaneEl && window.ResizeObserver) {
    const ro = new ResizeObserver(() => requestLayoutResize());
    ro.observe(tablePaneEl);
  }
} catch {
}

try {
  const plotPaneEl = document.querySelector('.plot-pane');
  if (plotPaneEl && window.ResizeObserver) {
    const ro = new ResizeObserver(() => requestLayoutResize());
    ro.observe(plotPaneEl);
  }
} catch {
}

try {
  if (plotEl && window.ResizeObserver) {
    const ro = new ResizeObserver(() => schedulePlotResize());
    ro.observe(plotEl);
  }
} catch {
}

try {
  const mapCard = document.querySelector('.map-card');
  if (mapCard && window.ResizeObserver) {
    const ro = new ResizeObserver(() => requestLayoutResize());
    ro.observe(mapCard);
  }
} catch {
}

initGridSplit();

function updateMapFromState() {
  if (!mapEl) return;
  const ci = state?.countInfo || {};

  const map = ensureLeafletMap();
  if (!map) return;

  const activeCode = getActiveCircleCode();

  void ensureAllCirclesOnMap()
    .then(() => {
      setActiveCircleStyle(activeCode);
      if (activeCode && leafletCircleByCode.has(activeCode)) {
        try {
          map.fitBounds(leafletCircleByCode.get(activeCode).getBounds(), { padding: [18, 18] });
        } catch {
        }
      }
    })
    .catch(() => {});

  setActiveCircleStyle(activeCode);

  const lat = typeof ci.Lat === 'number' ? ci.Lat : parseFloat(String(ci.Lat ?? '').trim());
  const lon = typeof ci.Lon === 'number' ? ci.Lon : parseFloat(String(ci.Lon ?? '').trim());
  const has = Number.isFinite(lat) && Number.isFinite(lon);

  if (leafletMapMarker) {
    leafletMapMarker.remove();
    leafletMapMarker = null;
  }

  if (has && activeCode && !leafletCircleByCode.has(activeCode)) {
    const center = [lat, lon];
    const radiusMeters = 7.5 * 1609.344;
    if (!leafletCircle) {
      leafletCircle = L.circle(center, {
        radius: radiusMeters,
        color: 'red',
        weight: 2,
        fill: true,
        fillColor: 'red',
        fillOpacity: 0,
        interactive: false,
        pane: LEAFLET_CIRCLES_PANE,
      }).addTo(map);
    } else {
      leafletCircle.setLatLng(center);
      leafletCircle.setRadius(radiusMeters);
    }
    try {
      map.fitBounds(leafletCircle.getBounds(), { padding: [18, 18] });
    } catch {
      map.setView(center, 10);
    }
  } else {
    if (leafletCircle) {
      leafletCircle.remove();
      leafletCircle = null;
    }
    if (!activeCode) {
      try {
        map.setView([37.5, -120.5], 4);
      } catch {
      }
    }
  }

  try {
    map.invalidateSize();
  } catch {
  }
}

let circlesIndexPromise = null;
let circlesIndex = null;
let selectedCircle = null;
let circlePickToken = 0;

function bumpCirclePickToken() {
  circlePickToken += 1;
  return circlePickToken;
}

function setDownloadingIndicator(isOn) {
  if (!downloadIndicatorEl) return;
  downloadIndicatorEl.classList.toggle('hidden', !isOn);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function fetchJsonWithRetry(url, { tries = 4 } = {}) {
  let lastErr = null;
  for (let attempt = 0; attempt < tries; attempt++) {
    try {
      const r = await fetch(url, { credentials: 'omit' });
      if (r.ok) return await r.json();
      const retryable = r.status === 429 || r.status === 502 || r.status === 503 || r.status === 504;
      if (!retryable) throw new Error(`HTTP ${r.status}`);
      lastErr = new Error(`HTTP ${r.status}`);
    } catch (e) {
      lastErr = e;
    }
    if (attempt < tries - 1) {
      const backoff = 350 * Math.pow(2, attempt);
      await sleep(backoff);
    }
  }
  throw lastErr || new Error('Request failed');
}

function getActiveCircleCode() {
  const fromPick = cleanText(selectedCircle?.Abbrev || '');
  if (fromPick) return fromPick;
  const fromState = cleanText(state?.countInfo?.CountCode || '');
  return fromState || null;
}

function setSelectedCircle(next, { fetchIfMissing = false } = {}) {
  selectedCircle = next || null;
  const token = bumpCirclePickToken();
  renderSelectedCircle();
  try {
    if (countSearchResultsEl) countSearchResultsEl.replaceChildren();
  } catch {
  }
  if (lastIngestedIndex) renderIngestedCountsList(lastIngestedIndex);
  renderCountHeader();
  updateMapFromState();

  const code = cleanText(selectedCircle?.Abbrev || '');
  const cid = selectedCircle?.Circle_id;
  const isStored = code && storedCountCodes.has(code);
  if (fetchIfMissing && code && !isStored) {
    if (!CBC_WORKER_BASE_URL) {
      clearLoadedCsvData();
      panelEl.innerHTML =
        '<div class="empty">CSV proxy is not configured. Set <b>VITE_CBC_WORKER_BASE</b> and rebuild, or use a stored circle.</div>';
      summaryEl.innerHTML =
        '<div class="empty">CSV proxy is not configured. Set <b>VITE_CBC_WORKER_BASE</b> and rebuild.</div>';
      setDownloadingIndicator(false);
      requestSidebarSync();
      return;
    }

    setDownloadingIndicator(true);
    clearLoadedCsvData();
    panelEl.innerHTML = '<div class="empty">Downloading circle data…</div>';
    summaryEl.innerHTML = '<div class="empty">Downloading…</div>';
    clearPlot('Click on a species above to plot');

    (async () => {
      const name = selectedCircle?.Name || '';
      await (async () => {
        if (token !== circlePickToken) return;
        if (cleanText(selectedCircle?.Abbrev || '') !== code) return;
        await downloadAndIngestCircle({ abbrev: code, cid, name });
      })();
    })()
      .catch((err) => {
        const msg = err?.message || String(err);
        panelEl.innerHTML = `<div class="empty">${escapeHtml(msg)}</div>`;
        summaryEl.innerHTML = `<div class="empty">${escapeHtml(msg)}</div>`;
      })
      .finally(() => {
        if (token !== circlePickToken) return;
        setDownloadingIndicator(false);
        requestSidebarSync();
      });
  } else {
    setDownloadingIndicator(false);
  }
}

function clearLoadedCsvData() {
  state = {
    ...state,
    species: null,
    weather: null,
    effort: null,
    participation: null,
    meta: null,
    sourceUrl: null,
    maxCountIndex: null,
    years: null,
    yearsFull: null,
    missingYears: null,
    filename: null,
    countInfo: null,
    ranges: null,
    selectedSpecies: null,
    speciesFilterRare: false,
    speciesFilterOwls: false,
  };
  renderSummary();
  try {
    renderPanel(activeTab);
  } catch {
  }
  clearPlot('Click on a species above to plot');
}

function setStoredCountCodesFromIndex(index) {
  const next = new Set((index || []).map((r) => cleanText(r?.code || '')).filter(Boolean));
  storedCountCodes = next;
}

async function setSelectedCircleByCode(code, { updateSearchValue = false } = {}) {
  const needle = cleanText(code || '');
  if (!needle) {
    setSelectedCircle(null);
    return;
  }
  try {
    const rows = await ensureCirclesIndexLoaded();
    const picked = rows.find((r) => r.Abbrev === needle);
    if (picked) setSelectedCircle(picked, { updateSearchValue });
  } catch {
  }
}

async function loadCirclesIndex126() {
  const all = [];
  const pageSize = 2000;
  let offset = 0;
  for (let page = 0; page < 5; page++) {
    const u = new URL(CBC_126_CIRCLES_QUERY_URL);
    u.searchParams.set('f', 'json');
    u.searchParams.set('where', '1=1');
    u.searchParams.set('outFields', 'Abbrev,Name,Circle_id');
    u.searchParams.set('returnGeometry', 'false');
    u.searchParams.set('resultOffset', String(offset));
    u.searchParams.set('resultRecordCount', String(pageSize));

    const data = await fetchJsonWithRetry(u.toString(), { tries: 4 });
    const feats = data?.features || [];
    if (!Array.isArray(feats) || feats.length === 0) break;
    for (const ft of feats) {
      const a = ft?.attributes || {};
      const abbrev = cleanText(a.Abbrev || '');
      const name = cleanText(a.Name || '');
      const cid = typeof a.Circle_id === 'number' ? a.Circle_id : parseInt(String(a.Circle_id || '').trim(), 10);
      if (!abbrev || !name || !Number.isFinite(cid)) continue;
      all.push({ Abbrev: abbrev, Name: name, Circle_id: cid });
    }
    offset += feats.length;
    if (feats.length < pageSize) break;
  }
  all.sort((x, y) => x.Name.localeCompare(y.Name));
  return all;
}

function ensureCirclesIndexLoaded() {
  if (circlesIndex) return Promise.resolve(circlesIndex);
  if (!circlesIndexPromise) {
    circlesIndexPromise = loadCirclesIndex126().then((rows) => {
      circlesIndex = rows;
      return rows;
    });
  }
  return circlesIndexPromise;
}

async function loadCirclesGeometry126() {
  const all = [];
  const pageSize = 1000;
  let offset = 0;
  for (let page = 0; page < 10; page++) {
    const u = new URL(CBC_126_CIRCLES_QUERY_URL);
    u.searchParams.set('f', 'json');
    u.searchParams.set('where', '1=1');
    u.searchParams.set('outFields', 'Abbrev,Name,Circle_id');
    u.searchParams.set('returnGeometry', 'true');
    u.searchParams.set('outSR', '4326');
    u.searchParams.set('resultOffset', String(offset));
    u.searchParams.set('resultRecordCount', String(pageSize));

    const data = await fetchJsonWithRetry(u.toString(), { tries: 4 });
    const feats = data?.features || [];
    if (!Array.isArray(feats) || feats.length === 0) break;
    for (const ft of feats) {
      const a = ft?.attributes || {};
      const g = ft?.geometry || null;

      const abbrev = cleanText(a.Abbrev || '');
      const name = cleanText(a.Name || '');
      const cid = typeof a.Circle_id === 'number' ? a.Circle_id : parseInt(String(a.Circle_id || '').trim(), 10);
      if (!abbrev || !name || !Number.isFinite(cid) || !g) continue;

      let lon = null;
      let lat = null;
      if (typeof g.x === 'number' && typeof g.y === 'number') {
        lon = g.x;
        lat = g.y;
      } else if (Array.isArray(g.rings) && g.rings.length) {
        let minX = Infinity;
        let maxX = -Infinity;
        let minY = Infinity;
        let maxY = -Infinity;
        for (const ring of g.rings) {
          if (!Array.isArray(ring)) continue;
          for (const pt of ring) {
            if (!Array.isArray(pt) || pt.length < 2) continue;
            const x = pt[0];
            const y = pt[1];
            if (typeof x !== 'number' || typeof y !== 'number') continue;
            if (x < minX) minX = x;
            if (x > maxX) maxX = x;
            if (y < minY) minY = y;
            if (y > maxY) maxY = y;
          }
        }
        if (Number.isFinite(minX) && Number.isFinite(maxX) && Number.isFinite(minY) && Number.isFinite(maxY)) {
          lon = (minX + maxX) / 2;
          lat = (minY + maxY) / 2;
        }
      }

      if (!(typeof lat === 'number' && Number.isFinite(lat) && typeof lon === 'number' && Number.isFinite(lon))) continue;
      all.push({ Abbrev: abbrev, Name: name, Circle_id: cid, Lat: lat, Lon: lon });
    }

    offset += feats.length;
    if (feats.length < pageSize) break;
  }
  all.sort((x, y) => x.Name.localeCompare(y.Name));
  return all;
}

function ensureCirclesGeometryLoaded() {
  if (circlesGeometry) return Promise.resolve(circlesGeometry);
  if (!circlesGeometryPromise) {
    circlesGeometryPromise = loadCirclesGeometry126().then((rows) => {
      circlesGeometry = rows;
      return rows;
    });
  }
  return circlesGeometryPromise;
}

async function ensureAllCirclesOnMap() {
  const map = ensureLeafletMap();
  if (!map) return;
  if (!leafletAllCirclesLayer) {
    leafletAllCirclesLayer = L.layerGroup();
    leafletAllCirclesLayer.addTo(map);
  }
  if (leafletCircleByCode.size > 0) return;

  const rows = await ensureCirclesGeometryLoaded();
  const radiusMeters = 7.5 * 1609.344;
  for (const r of rows) {
    const code = r.Abbrev;
    if (!code || leafletCircleByCode.has(code)) continue;
    const center = [r.Lat, r.Lon];

    const isStored = storedCountCodes.has(code);
    const baseColor = isStored ? 'orange' : '#4b6576';
    const baseWeight = isStored ? 3 : 1;
    const layer = L.circle(center, {
      radius: radiusMeters,
      color: baseColor,
      weight: baseWeight,
      opacity: isStored ? 1 : 0.75,
      fill: true,
      fillColor: baseColor,
      fillOpacity: 0,
      interactive: true,
      pane: LEAFLET_CIRCLES_PANE,
    });
    layer.on('click', () => {
      const picked = { Abbrev: r.Abbrev, Name: r.Name, Circle_id: r.Circle_id };
      setSelectedCircle(picked, { fetchIfMissing: true });

      if (storedCountCodes.has(code)) {
        panelEl.innerHTML = '<div class="empty">Loading stored count…</div>';
        loadStateFromSqlite(code).catch((err) => {
          const msg = err?.message || String(err);
          panelEl.innerHTML = `<div class="empty">Error: ${escapeHtml(msg)}</div>`;
        });
      } else {
        clearLoadedCsvData();
      }

      requestSidebarSync();
    });
    layer.addTo(leafletAllCirclesLayer);
    leafletCircleByCode.set(code, layer);
  }
}

function applyStoredCircleStyles() {
  for (const [code, layer] of leafletCircleByCode.entries()) {
    if (!layer) continue;
    const isActive = lastActiveCircleCode && code === lastActiveCircleCode;
    if (isActive) continue;
    const isStored = storedCountCodes.has(code);
    const color = isStored ? 'orange' : '#4b6576';
    try {
      layer.setStyle({ color, weight: isStored ? 3 : 1, opacity: isStored ? 1 : 0.75 });
      if (isStored && layer.bringToFront) layer.bringToFront();
    } catch {
    }
  }
}

function setActiveCircleStyle(activeCode) {
  const next = cleanText(activeCode || '');

  if (lastActiveCircleCode && leafletCircleByCode.has(lastActiveCircleCode)) {
    try {
      const isStored = storedCountCodes.has(lastActiveCircleCode);
      const prevColor = isStored ? 'orange' : '#4b6576';
      leafletCircleByCode.get(lastActiveCircleCode).setStyle({
        color: prevColor,
        weight: isStored ? 3 : 1,
        opacity: isStored ? 1 : 0.75,
      });
    } catch {
    }
  }

  if (next && leafletCircleByCode.has(next)) {
    try {
      const layer = leafletCircleByCode.get(next);
      layer.setStyle({ color: 'red', weight: 2, opacity: 1 });
      if (layer.bringToFront) layer.bringToFront();
    } catch {
    }
    lastActiveCircleCode = next;
  } else {
    lastActiveCircleCode = null;
  }
}

function renderSelectedCircle() {
  if (!countSearchSelectedEl) return;
  if (!selectedCircle) {
    countSearchSelectedEl.replaceChildren();
    countSearchSelectedEl.classList.add('hidden');
    return;
  }
  countSearchSelectedEl.classList.remove('hidden');
  const url =
    buildWorkerCsvDownloadUrl({ abbrev: selectedCircle.Abbrev, cid: selectedCircle.Circle_id, sy: 1, ey: CURRENT_MAX_COUNT_INDEX }) ||
    buildDefaultCsvDownloadUrl({ abbrev: selectedCircle.Abbrev, cid: selectedCircle.Circle_id });
  countSearchSelectedEl.replaceChildren();

  const line = document.createElement('div');
  line.className = 'count-selected-line';
  const name = String(selectedCircle.Name || '').trim();
  const code = String(selectedCircle.Abbrev || '').trim();
  const id = String(selectedCircle.Circle_id ?? '').trim();
  line.textContent = [name, code, id].filter((x) => x).join(' | ');
  countSearchSelectedEl.appendChild(line);
}

function renderCircleSearchResults(query, results) {
  if (!countSearchResultsEl) return;
  if (!query) {
    countSearchResultsEl.replaceChildren();
    return;
  }
  if (!results || results.length === 0) {
    countSearchResultsEl.replaceChildren(
      Object.assign(document.createElement('div'), { className: 'count-search-empty', textContent: 'No matches.' })
    );
    return;
  }
  countSearchResultsEl.replaceChildren();
  for (const r of results.slice(0, 8)) {
    const btn = document.createElement('button');
    btn.className = 'count-search-item';
    btn.type = 'button';
    btn.dataset.action = 'pick-circle';
    btn.dataset.code = r.Abbrev;
    btn.textContent = `${r.Name} (${r.Abbrev})`;
    countSearchResultsEl.appendChild(btn);
  }
}

let _searchTimer = null;
function scheduleCircleSearch() {
  if (!countSearchEl) return;
  if (_searchTimer) clearTimeout(_searchTimer);
  _searchTimer = setTimeout(async () => {
    const q = cleanText(countSearchEl.value || '');
    if (!q) {
      renderCircleSearchResults('', []);
      return;
    }
    try {
      const rows = await ensureCirclesIndexLoaded();
      const needle = q.toLowerCase();
      const hits = rows.filter((r) => {
        return r.Abbrev.toLowerCase().includes(needle) || r.Name.toLowerCase().includes(needle);
      });
      renderCircleSearchResults(q, hits);
    } catch (err) {
      const msg = err?.message || String(err);
      if (countSearchResultsEl) {
        countSearchResultsEl.replaceChildren(
          Object.assign(document.createElement('div'), { className: 'count-search-empty', textContent: msg })
        );
      }
    }
  }, 150);
}

const infoBtnEl = document.getElementById('infoBtn');
const infoModalEl = document.getElementById('infoModal');
const infoModalCloseEl = document.getElementById('infoModalClose');

function openInfoModal() {
  if (!infoModalEl) return;
  infoModalEl.classList.remove('hidden');
  infoModalCloseEl?.focus?.();
}

function closeInfoModal() {
  if (!infoModalEl) return;
  infoModalEl.classList.add('hidden');
  infoBtnEl?.focus?.();
}

infoBtnEl?.addEventListener('click', () => {
  if (!infoModalEl) return;
  const isHidden = infoModalEl.classList.contains('hidden');
  if (isHidden) openInfoModal();
  else closeInfoModal();
});

infoModalCloseEl?.addEventListener('click', () => closeInfoModal());

infoModalEl?.addEventListener('click', (e) => {
  if (e.target === infoModalEl) closeInfoModal();
});

document.addEventListener('keydown', (e) => {
  if (e.key !== 'Escape') return;
  if (!infoModalEl) return;
  if (infoModalEl.classList.contains('hidden')) return;
  closeInfoModal();
});

const IDB_NAME = 'cbc_extract_data_web';
const IDB_STORE = 'kv';
const IDB_KEY_INDEX = 'countsIndex';
const IDB_KEY_DB_PREFIX = 'countDb:';

function openIdb() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(IDB_NAME, 1);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(IDB_STORE)) {
        db.createObjectStore(IDB_STORE);
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function idbGet(key) {
  const db = await openIdb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(IDB_STORE, 'readonly');
    const store = tx.objectStore(IDB_STORE);
    const req = store.get(key);
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function idbSet(key, value) {
  const db = await openIdb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(IDB_STORE, 'readwrite');
    const store = tx.objectStore(IDB_STORE);
    const req = store.put(value, key);
    req.onsuccess = () => resolve();
    req.onerror = () => reject(req.error);
  });
}

async function idbDel(key) {
  const db = await openIdb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(IDB_STORE, 'readwrite');
    const store = tx.objectStore(IDB_STORE);
    const req = store.delete(key);
    req.onsuccess = () => resolve();
    req.onerror = () => reject(req.error);
  });
}

function safeCountKey() {
  const ci = state.countInfo || {};
  const preferred = ci.CountCode || ci.CountName || '';
  const key = sanitizeFilenamePart(preferred) || sanitizeFilenamePart(ci.CountName || 'count');
  return key || 'count';
}

async function loadCountsIndex() {
  const v = await idbGet(IDB_KEY_INDEX);
  if (!v) return [];
  if (Array.isArray(v)) return v;
  return [];
}

async function saveCountsIndex(index) {
  await idbSet(IDB_KEY_INDEX, index);
}

async function buildIndexRowFromSqliteBytes(code, buf) {
  const SQL = await getSql();
  const db = new SQL.Database(new Uint8Array(buf));
  const readJson = (key) => {
    const res = db.exec('SELECT value FROM kv WHERE key = ' + JSON.stringify(key));
    const v = res?.[0]?.values?.[0]?.[0];
    if (!v) return null;
    try {
      return JSON.parse(v);
    } catch {
      return null;
    }
  };

  const ci = readJson('countInfo') || {};
  const ranges = readJson('ranges') || {};
  const years = readJson('years') || [];
  const maxCountIndex = readJson('maxCountIndex');
  db.close();

  const name = ci.CountName || ci.CountCode || code;
  const range = normalizeRangeForTitle(ranges?.speciesYears || yearRangeStr(years));
  return {
    code,
    name,
    range,
    maxCountIndex: maxCountIndex ?? null,
    updatedAt: Date.now(),
  };
}

async function ensureSeedCountsImported() {
  const seeds = SEED_CODES;
  let idx;
  try {
    idx = await loadCountsIndex();
  } catch {
    idx = [];
  }
  const haveIndex = new Set((idx || []).map((r) => r?.code).filter(Boolean));
  const next = (idx || []).slice();

  for (const code of seeds) {
    const key = `${IDB_KEY_DB_PREFIX}${code}`;
    let buf = null;
    try {
      buf = await idbGet(key);
    } catch {
    }

    if (!buf) {
      const seedUrl = new URL(`seed/${code}.sqlite`, window.location.href).toString();
      const r = await fetch(seedUrl, { credentials: 'omit' });
      if (r.ok) {
        const ab = await r.arrayBuffer();
        await idbSet(key, ab);
        buf = ab;
      }
    }

    if (buf && !haveIndex.has(code)) {
      try {
        const row = await buildIndexRowFromSqliteBytes(code, buf);
        next.push(row);
        haveIndex.add(code);
      } catch {
      }
    }
  }

  if (next.length !== (idx || []).length) {
    await saveCountsIndex(next);
  }
}

function renderIngestedCountsList(index) {
  if (!ingestedEl) return;
  const rows = (index || []).slice().sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));
  lastIngestedIndex = index || [];
  setStoredCountCodesFromIndex(lastIngestedIndex);
  applyStoredCircleStyles();

  const selectedCode = cleanText(selectedCircle?.Abbrev || '');
  const shouldShowPending = selectedCode && !storedCountCodes.has(selectedCode);

  const activeCode = getActiveCircleCode();

  const wrap = document.createElement('div');
  wrap.className = 'table-wrap';
  const table = document.createElement('table');
  table.className = 'table';

  const thead = document.createElement('thead');
  const headRow = document.createElement('tr');
  for (const h of ['Count circle', 'Code', 'Years', 'Actions']) {
    const th = document.createElement('th');
    th.textContent = h;
    if (h === 'Actions') th.className = 'col-update';
    headRow.appendChild(th);
  }
  thead.appendChild(headRow);

  const tbody = document.createElement('tbody');

  if (shouldShowPending) {
    const tr = document.createElement('tr');
    tr.dataset.code = selectedCode;
    if (activeCode && selectedCode === activeCode) tr.classList.add('is-selected');

    const tdName = document.createElement('td');
    tdName.textContent = selectedCircle?.Name || 'Count';

    const tdCode = document.createElement('td');
    tdCode.textContent = selectedCode;

    const tdRange = document.createElement('td');
    tdRange.textContent = '—';

    const tdUpdate = document.createElement('td');
    tdUpdate.className = 'col-update';
    const cell = document.createElement('div');
    cell.className = 'update-cell';
    const meta = document.createElement('span');
    meta.className = 'update-meta';
    meta.textContent = 'Click to update';
    cell.appendChild(meta);

    const btnUp = document.createElement('button');
    btnUp.className = 'update-button';
    btnUp.type = 'button';
    btnUp.dataset.action = 'update-count';
    btnUp.dataset.code = selectedCode;
    if (Number.isFinite(Number(selectedCircle?.Circle_id))) btnUp.dataset.cid = String(selectedCircle.Circle_id);
    if (selectedCircle?.Name) btnUp.dataset.name = String(selectedCircle.Name);
    btnUp.setAttribute('aria-label', 'Update');
    btnUp.title = CBC_WORKER_BASE_URL ? 'Download and store' : 'CSV proxy is not configured';
    btnUp.textContent = '↻';
    if (!CBC_WORKER_BASE_URL) btnUp.classList.add('is-disabled');
    cell.appendChild(btnUp);

    tdUpdate.appendChild(cell);

    tr.appendChild(tdName);
    tr.appendChild(tdCode);
    tr.appendChild(tdRange);
    tr.appendChild(tdUpdate);
    tbody.appendChild(tr);
  }

  if (rows.length === 0 && !shouldShowPending) {
    ingestedEl.replaceChildren(Object.assign(document.createElement('div'), { className: 'empty', textContent: 'None yet.' }));
    return;
  }

  for (const r of rows) {
    const name = r?.name || 'Count';
    const code = r?.code || '';
    const range = r?.range || '—';
    const updatedText = formatShortDate(r?.updatedAt);

    const tr = document.createElement('tr');
    tr.dataset.code = code;
    if (activeCode && code && code === activeCode) tr.classList.add('is-selected');

    const tdName = document.createElement('td');
    const a = document.createElement('a');
    a.className = 'link';
    a.href = '#';
    a.dataset.action = 'load-count';
    a.dataset.code = code;
    a.textContent = name;
    tdName.appendChild(a);

    const tdCode = document.createElement('td');
    tdCode.textContent = code;

    const tdRange = document.createElement('td');
    tdRange.textContent = range;

    const tdUpdate = document.createElement('td');
    tdUpdate.className = 'col-update';
    const cell = document.createElement('div');
    cell.className = 'update-cell';
    if (updatedText) {
      const meta = document.createElement('span');
      meta.className = 'update-meta';
      meta.textContent = `Updated ${updatedText}`;
      cell.appendChild(meta);
    }

    const btnUp = document.createElement('button');
    btnUp.className = 'update-button';
    btnUp.type = 'button';
    btnUp.dataset.action = 'update-count';
    btnUp.dataset.code = code;
    btnUp.setAttribute('aria-label', 'Update');
    btnUp.title = 'Update';
    btnUp.textContent = '↻';

    const btnDel = document.createElement('button');
    btnDel.className = 'update-button delete-button';
    btnDel.type = 'button';
    btnDel.dataset.action = 'delete-count';
    btnDel.dataset.code = code;
    btnDel.setAttribute('aria-label', 'Delete');
    btnDel.title = 'Delete';
    btnDel.textContent = 'X';

    cell.appendChild(btnUp);
    cell.appendChild(btnDel);
    tdUpdate.appendChild(cell);

    tr.appendChild(tdName);
    tr.appendChild(tdCode);
    tr.appendChild(tdRange);
    tr.appendChild(tdUpdate);
    tbody.appendChild(tr);
  }

  table.appendChild(thead);
  table.appendChild(tbody);
  wrap.appendChild(table);
  ingestedEl.replaceChildren(wrap);

  requestSidebarSync();
}

async function refreshIngestedCounts() {
  try {
    const idx = await loadCountsIndex();
    renderIngestedCountsList(idx);
  } catch {
    if (ingestedEl) ingestedEl.innerHTML = '<div class="empty">Failed to load stored counts.</div>';
  }
}

async function saveCurrentStateToSqlite() {
  if (!state.species) return;
  const SQL = await getSql();
  const db = new SQL.Database();

  db.run('CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)');
  const put = db.prepare('INSERT OR REPLACE INTO kv(key, value) VALUES(?, ?)');
  const saveJson = (key, value) => {
    put.run([key, JSON.stringify(value)]);
  };

  saveJson('countInfo', state.countInfo || null);
  saveJson('ranges', state.ranges || null);
  saveJson('years', state.years || []);
  saveJson('yearsFull', state.yearsFull || []);
  saveJson('missingYears', state.missingYears || []);
  saveJson('meta', state.meta || []);
  saveJson('sourceUrl', state.sourceUrl || null);
  saveJson('maxCountIndex', state.maxCountIndex || null);
  saveJson('species', state.species || []);
  saveJson('weather', state.weather || []);
  saveJson('effort', state.effort || []);
  saveJson('participation', state.participation || []);
  saveJson('savedAt', new Date().toISOString());
  put.free();

  const bytes = db.export();
  db.close();

  const ci = state.countInfo || {};
  const code = safeCountKey();
  const name = ci.CountName || ci.CountCode || code;
  const range = normalizeRangeForTitle(state.ranges?.speciesYears || yearRangeStr(state.years || []));
  await idbSet(`${IDB_KEY_DB_PREFIX}${code}`, bytes.buffer);

  const idx = await loadCountsIndex();
  const now = Date.now();
  const next = idx.filter((r) => r.code !== code);
  next.push({ code, name, range, maxCountIndex: state.maxCountIndex || null, updatedAt: now });
  await saveCountsIndex(next);
  renderIngestedCountsList(next);
}

async function loadStateFromSqlite(code) {
  let buf = await idbGet(`${IDB_KEY_DB_PREFIX}${code}`);
  if (!buf && SEED_CODES.includes(code)) {
    try {
      const seedUrl = new URL(`seed/${code}.sqlite`, window.location.href).toString();
      const r = await fetch(seedUrl, { credentials: 'omit' });
      if (r.ok) {
        const ab = await r.arrayBuffer();
        buf = ab;
        try {
          await idbSet(`${IDB_KEY_DB_PREFIX}${code}`, ab);
          const idx = await loadCountsIndex();
          const next = (idx || []).filter((row) => row.code !== code);
          try {
            const row = await buildIndexRowFromSqliteBytes(code, ab);
            next.push(row);
            await saveCountsIndex(next);
          } catch {
          }
        } catch {
        }
      }
    } catch {
    }
  }
  if (!buf) throw new Error('No stored database found for that count.');
  const SQL = await getSql();
  const db = new SQL.Database(new Uint8Array(buf));

  const readJson = (key) => {
    const res = db.exec('SELECT value FROM kv WHERE key = ' + JSON.stringify(key));
    const v = res?.[0]?.values?.[0]?.[0];
    if (!v) return null;
    try {
      return JSON.parse(v);
    } catch {
      return null;
    }
  };

  const parsed = {
    countInfo: readJson('countInfo'),
    ranges: readJson('ranges'),
    years: readJson('years'),
    yearsFull: readJson('yearsFull'),
    missingYears: readJson('missingYears'),
    meta: readJson('meta'),
    sourceUrl: readJson('sourceUrl'),
    maxCountIndex: readJson('maxCountIndex'),
    species: readJson('species'),
    weather: readJson('weather'),
    effort: readJson('effort'),
    participation: readJson('participation'),
  };
  db.close();

  state = {
    ...state,
    ...parsed,
    filename: null,
    selectedSpecies: null,
    speciesFilterRare: false,
    speciesFilterOwls: false,
  };
  renderCountHeader();
  renderSummary();
  setActiveTab('species');

  await setSelectedCircleByCode(state?.countInfo?.CountCode || code, { updateSearchValue: false });
}

function escapeHtmlForPlotText(s) {
  return String(s)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

const PLOT_STYLE_BASE = {
  paper_bgcolor: 'white',
  plot_bgcolor: '#E5ECF6',
  font: { color: '#2a3f5f' },
  hovermode: 'closest',
  margin: { l: 40, r: 10, t: 10, b: 30 },
  autosize: true,
};

const AXIS_STYLE_BASE = {
  gridcolor: 'white',
  linecolor: 'white',
  ticks: '',
  title: { standoff: 15 },
  zerolinecolor: 'white',
  zerolinewidth: 2,
  automargin: true,
};

function setPlotHeader({ title } = {}) {
  currentPlotTitle = title || null;
  applyHeaderTheme();
  try {
    if (plotHeaderEl) plotHeaderEl.innerHTML = '';
  } catch {
  }
  renderPlotSpeciesOverlay();
}

const tabs = {
  species: document.getElementById('tabSpecies'),
  weather: document.getElementById('tabWeather'),
  effort: document.getElementById('tabEffort'),
  participation: document.getElementById('tabParticipation'),
};

let state = {
  species: null,
  weather: null,
  effort: null,
  participation: null,
  meta: null,
  sourceUrl: null,
  maxCountIndex: null,
  years: null,
  yearsFull: null,
  missingYears: null,
  filename: null,
  countInfo: null,
  ranges: null,
  selectedSpecies: null,
  speciesFilterRare: false,
  speciesFilterOwls: false,
};

let activeTab = 'species';

let currentPlotTitle = null;

function applyHeaderTheme() {
  const useTeal = false;
  panelHeaderEl?.classList?.toggle('teal-header', useTeal);
  plotHeaderEl?.classList?.toggle('teal-header', useTeal);
}

function schedulePlotResize() {
  if (!plotEl) return;
  requestAnimationFrame(() => {
    const P = plotlyRef;
    if (!P) return;
    try {
      const target = plotMountEl || plotEl;
      const { width, height } = currentPlotSize();
      if (width && height) {
        try {
          P.relayout(target, { width, height });
        } catch {
        }
      }
      P.Plots.resize(target);
    } catch {
    }
  });
}

function plotlyConfig() {
  return {
    responsive: true,
    displayModeBar: 'hover',
    displaylogo: false,
  };
}

function currentPlotSize() {
  if (!plotEl) return { width: null, height: null };
  const cs = window.getComputedStyle(plotEl);
  const padL = parseFloat(cs.paddingLeft || '0') || 0;
  const padR = parseFloat(cs.paddingRight || '0') || 0;
  const padT = parseFloat(cs.paddingTop || '0') || 0;
  const padB = parseFloat(cs.paddingBottom || '0') || 0;

  const width = plotEl.clientWidth - padL - padR;
  const height = plotEl.clientHeight - padT - padB;
  return {
    width: Number.isFinite(width) && width > 0 ? width : null,
    height: Number.isFinite(height) && height > 0 ? height : null,
  };
}

function ensurePlotScaffold() {
  if (!plotEl) return { mount: null, empty: null };

  if (!plotMountEl || !plotEl.contains(plotMountEl)) {
    plotMountEl = plotEl.querySelector('[data-role="plot-mount"]');
  }
  if (!plotEmptyEl || !plotEl.contains(plotEmptyEl)) {
    plotEmptyEl = plotEl.querySelector('[data-role="plot-empty"]') || plotEl.querySelector('.empty');
    if (plotEmptyEl) plotEmptyEl.setAttribute('data-role', 'plot-empty');
  }

  if (!plotMountEl) {
    plotMountEl = document.createElement('div');
    plotMountEl.className = 'plot-mount';
    plotMountEl.setAttribute('data-role', 'plot-mount');
    plotMountEl.style.width = '100%';
    plotMountEl.style.height = '100%';
    plotEl.insertBefore(plotMountEl, plotEl.firstChild);
  }

  if (!plotEmptyEl) {
    plotEmptyEl = document.createElement('div');
    plotEmptyEl.className = 'empty';
    plotEmptyEl.setAttribute('data-role', 'plot-empty');
    plotEl.insertBefore(plotEmptyEl, plotMountEl.nextSibling);
  }

  return { mount: plotMountEl, empty: plotEmptyEl };
}

async function setActiveTab(name) {
  activeTab = name;
  for (const [k, el] of Object.entries(tabs)) {
    if (el) el.classList.toggle('active', k === name);
  }
  applyHeaderTheme();
  renderPanel(name);
  await renderPlot(name);
  schedulePlotResize();
}

for (const [k, el] of Object.entries(tabs)) {
  if (!el) continue;
  el.addEventListener('click', () => {
    setActiveTab(k);
  });
}

function getPopoutSnapshot() {
  return {
    activeTab,
    state: safeClone(state),
  };
}

function openPopout(mode) {
  const id = makePopoutId();
  const url = buildPopoutUrl(mode, id);
  const win = window.open(url, `cbc-historic-${mode}-${id}`, 'popup,width=1200,height=800');
  pendingPopouts.set(id, { mode, win });
  try {
    popoutChannel?.postMessage({ type: 'snapshot', popoutId: id, mode, snapshot: getPopoutSnapshot() });
  } catch {
  }
  setTimeout(() => {
    try {
      popoutChannel?.postMessage({ type: 'snapshot', popoutId: id, mode, snapshot: getPopoutSnapshot() });
    } catch {
    }
  }, 300);
}

popoutChannel?.addEventListener('message', (ev) => {
  const msg = ev?.data;
  if (!msg || typeof msg !== 'object') return;

  if (!IS_POPOUT && msg.type === 'ready') {
    const id = msg.popoutId;
    const pending = pendingPopouts.get(id);
    if (!pending) return;
    try {
      popoutChannel?.postMessage({ type: 'snapshot', popoutId: id, mode: pending.mode, snapshot: getPopoutSnapshot() });
    } catch {
    }
    return;
  }

  if (IS_POPOUT && msg.type === 'snapshot') {
    if (!POPOUT_ID || msg.popoutId !== POPOUT_ID) return;
    const snap = msg.snapshot;
    if (!snap || typeof snap !== 'object') return;
    if (snap.state && typeof snap.state === 'object') {
      state = { ...state, ...snap.state };
    }
    if (snap.activeTab) {
      activeTab = snap.activeTab;
    }

    if (IS_POPOUT_TABLE) {
      renderCountHeader();
      renderSummary();
      renderPanel(activeTab);
      return;
    }

    if (IS_POPOUT_PLOT) {
      renderCountHeader();
      renderSummary();
      renderPlotSpeciesOverlay();
      renderPlot('species');
      return;
    }
  }
});

if (IS_POPOUT && POPOUT_ID) {
  try {
    popoutChannel?.postMessage({ type: 'ready', popoutId: POPOUT_ID, mode: POPOUT_MODE });
  } catch {
  }
}

function escapeHtml(s) {
  return String(s)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function normalizeCell(v) {
  if (v === undefined || v === null) return '';
  if (typeof v === 'string') return v;
  if (typeof v === 'number') return Number.isFinite(v) ? String(v) : '';
  return String(v);
}

function cleanText(v) {
  const s = normalizeCell(v).replace(/\r/g, '');
  return s.replace(/\s+/g, ' ').trim();
}

function yearsFromRows(rows) {
  if (!rows || rows.length === 0) return [];
  const out = [];
  for (const r of rows) {
    const y = r?.Year;
    if (typeof y === 'number' && Number.isFinite(y)) out.push(y);
    else if (typeof y === 'string' && y.trim() && Number.isFinite(parseInt(y.trim(), 10))) out.push(parseInt(y.trim(), 10));
  }
  return out;
}

function continuousYears(years) {
  const ys = (years || []).filter((y) => typeof y === 'number' && Number.isFinite(y));
  if (ys.length === 0) return [];
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const out = [];
  for (let y = minY; y <= maxY; y++) out.push(y);
  return out;
}

function missingYearsFromRanges(yearsPresent, yearsFull) {
  const present = new Set((yearsPresent || []).filter((y) => typeof y === 'number' && Number.isFinite(y)));
  return (yearsFull || []).filter((y) => !present.has(y));
}

function yearRangeStr(years) {
  if (!years || years.length === 0) return '—';
  const minY = Math.min(...years);
  const maxY = Math.max(...years);
  return minY === maxY ? String(minY) : `${minY}–${maxY}`;
}

function normalizeRangeForTitle(rangeText) {
  const s = String(rangeText || '').trim();
  if (!s) return '—';
  return s.replaceAll('–', '-');
}

function countDisplayName() {
  const ci = state.countInfo || {};
  return ci.CountName || ci.CountCode || 'Count';
}

function withCbcSuffix(name) {
  const s = String(name || '').trim();
  if (!s) return 'CBC';
  if (/\bCBC\b/i.test(s)) return s;
  return `${s} CBC`;
}

function inferCountIndexRange({ meta, weather, effort, participation }) {
  const idxs = [];

  const pushIdx = (v) => {
    const n = typeof v === 'number' ? v : parseInt(String(v || '').trim(), 10);
    if (Number.isFinite(n)) idxs.push(n);
  };

  for (const r of meta || []) pushIdx(r?.CountIndex);
  for (const r of weather || []) pushIdx(r?.CountIndex);
  for (const r of effort || []) pushIdx(r?.CountIndex);
  for (const r of participation || []) pushIdx(r?.CountIndex);

  if (!idxs.length) return null;
  return { sy: Math.min(...idxs), ey: Math.max(...idxs) };
}

function inferMaxCountIndex({ meta, weather, effort, participation }) {
  const r = inferCountIndexRange({ meta, weather, effort, participation });
  return r ? r.ey : null;
}

function buildHistoricalSourceUrl({ countInfo, sy, ey }) {
  const abbrev = cleanText(countInfo?.CountCode || '');
  if (!abbrev) return null;
  if (!Number.isFinite(sy) || !Number.isFinite(ey)) return null;

  const params = new URLSearchParams();
  params.set('rf', 'CSV');
  params.set('so', '0');
  params.set('abbrev', abbrev);
  params.set('sy', String(sy));
  params.set('ey', String(ey));

  const cid = countInfo?.CountId;
  const cidNum = typeof cid === 'number' ? cid : parseInt(String(cid || '').trim(), 10);
  const knownCid = KNOWN_COUNT_IDS[abbrev] || null;
  if (Number.isFinite(cidNum)) params.set('cid', String(cidNum));
  else if (Number.isFinite(knownCid)) params.set('cid', String(knownCid));

  return `${CBC_HISTORICAL_RESULTS_URL}?${params.toString()}`;
}

function normalizeUpdateUrl(url) {
  if (!url) return null;
  try {
    const u = new URL(String(url));
    if (u.protocol !== 'https:') return null;
    const allowedHosts = new Set([
      new URL(CBC_RESULTS_URL).host,
      new URL(CBC_HISTORICAL_RESULTS_URL).host,
    ]);
    if (!allowedHosts.has(u.host)) return null;

    u.searchParams.set('rf', 'CSV');
    u.searchParams.set('so', '0');
    u.searchParams.set('sy', '1');
    u.searchParams.set('ey', String(CURRENT_MAX_COUNT_INDEX));

    const hasCid = !!u.searchParams.get('cid');
    const abbrev = (u.searchParams.get('abbrev') || '').trim();
    if (!hasCid && abbrev && KNOWN_COUNT_IDS[abbrev]) {
      u.searchParams.set('cid', String(KNOWN_COUNT_IDS[abbrev]));
    }

    return u.toString();
  } catch {
    return null;
  }
}

function parseCount(v) {
  if (v === undefined || v === null) return 0;
  if (typeof v === 'number') return Number.isFinite(v) ? Math.round(v) : 0;
  const s = cleanText(v);
  if (!s) return 0;
  if (s.toLowerCase() === 'cw') return 0;
  if (/^-?\d+$/.test(s)) return parseInt(s, 10);
  if (/^-?\d+\.0+$/.test(s)) return Math.round(parseFloat(s));
  return 0;
}

function yearFromCountDateString(countDate) {
  const s = cleanText(countDate);
  if (!s) return null;
  const m = s.match(/\b(19\d{2}|20\d{2})\b/);
  if (!m) return null;
  const y = parseInt(m[1], 10);
  return Number.isFinite(y) ? y : null;
}

function joinWeatherWithYearInfo(weatherRows, participantsEffortRows, metaRows) {
  const peByIdx = new Map(participantsEffortRows.map((r) => [r.CountIndex, r]));
  const metaByIdx = new Map(metaRows.map((r) => [r.CountIndex, r]));

  return weatherRows.map((w) => {
    const pe = peByIdx.get(w.CountIndex);
    const meta = metaByIdx.get(w.CountIndex);
    const Year = (pe && pe.Year) || (meta && meta.Year) || null;
    const CountDate = (pe && pe.CountDate) || (meta && meta.CountDate) || null;
    return { Year, CountDate, ...w };
  });
}

function renderSummary() {
  if (!summaryEl) return;
  if (!state.species) {
    summaryEl.replaceChildren(Object.assign(document.createElement('div'), { className: 'empty', textContent: 'No CSV loaded.' }));
    updateMapFromState();
    return;
  }

  const ci = state.countInfo || {};
  const r = state.ranges || {};
  const loc =
    ci.Lon !== null && ci.Lat !== null
      ? `(${Number(ci.Lon).toFixed(6)}, ${Number(ci.Lat).toFixed(6)})`
      : '—';

  const missingYears = (state.missingYears || [])
    .filter((y) => typeof y === 'number' && Number.isFinite(y))
    .slice()
    .sort((a, b) => a - b);
  const missingYearsText = missingYears.length ? missingYears.join(', ') : 'None';

  const rows = [
    ['Count Name', ci.CountName || '—'],
    ['Count Code', ci.CountCode || '—'],
    ['Location', loc],
    ['Missing years', missingYearsText],
    ['Species years', r.speciesYears || '—'],
    ['Weather years', r.weatherYears || '—'],
    ['Effort years', r.effortYears || '—'],
    ['Participation years', r.participationYears || '—'],
  ];
  summaryEl.replaceChildren();
  for (const [k, v] of rows) {
    const row = document.createElement('div');
    row.className = 'summary-row';
    const kk = document.createElement('div');
    kk.className = 'summary-k';
    kk.textContent = k;
    const vv = document.createElement('div');
    vv.textContent = String(v ?? '—');
    row.appendChild(kk);
    row.appendChild(vv);
    summaryEl.appendChild(row);
  }

  updateMapFromState();
}

function renderCountHeader() {
  if (!countHeaderTextEl) return;
  if (plotCircleHintEl) {
    const hasCircle = !!cleanText(selectedCircle?.Abbrev || '');
    plotCircleHintEl.classList.toggle('hidden', hasCircle || !!state?.species);
  }
  if (!state.species) {
    const selCode = cleanText(selectedCircle?.Abbrev || '');
    if (selectedCircle && selCode) {
      const status = storedCountCodes.has(selCode) ? 'Stored' : 'Not downloaded yet';
      countHeaderTextEl.textContent = `${selectedCircle.Name} (${selCode}) - ${status}`;
    } else {
      countHeaderTextEl.textContent = 'Load a CSV or select existing count data';
    }
    requestSidebarSync();
    return;
  }
  const ci = state.countInfo || {};
  const name = ci.CountName || 'Count';
  const code = cleanText(ci.CountCode || '');
  const years = state.years || [];
  const range = normalizeRangeForTitle(yearRangeStr(years));
  const parts = [cleanText(name), code, cleanText(range)].filter((p) => p);
  const line = parts.join(' - ');
  countHeaderTextEl.textContent = line;
  requestSidebarSync();
}

function stripBracketedText(s) {
  const raw = String(s ?? '');
  return raw.replaceAll(/\s*\[[^\]]*\]\s*/g, ' ').replaceAll(/\s+/g, ' ').trim();
}

function isSpRecord(speciesName) {
  const s = stripBracketedText(speciesName).trim();
  if (!s) return false;
  return /(?:^|[\s,\/])sp\.?\s*$/i.test(s);
}

function renderTable(rows, columns, opts = {}) {
  if (!rows || rows.length === 0) {
    return '<div class="empty">No rows.</div>';
  }

  const cols = columns && columns.length ? columns : Object.keys(rows[0]);

  const colClass = (c) => {
    if (c === 'Species') return 'col-species';
    if (c === 'Year' || /^\d{4}$/.test(c)) return 'col-year';
    return '';
  };

  const thead = cols.map((c) => `<th class="${colClass(c)}">${escapeHtml(c)}</th>`).join('');
  const tbody = rows
    .map((r) => {
      const isSelected =
        opts.selectedSpecies &&
        opts.clickSpecies &&
        cleanText(r?.Species || '') &&
        String(r.Species) === String(opts.selectedSpecies);
      const tds = cols
        .map((c) => {
          const v = r[c];
          const isYearCol = /^\d{4}$/.test(c);
          if (opts.clickSpecies && c === 'Species') {
            const sp = r.Species;
            const spDisplay = stripBracketedText(sp);
            return `<td class="cell-link ${colClass(c)}" data-action="plot-species" data-species="${escapeHtml(
              sp === null || sp === undefined ? '' : String(sp)
            )}">${escapeHtml(spDisplay)}</td>`;
          }
          if (opts.ndForMissingYears && isYearCol) {
            const yearNum = parseInt(c, 10);
            const missingSet = opts.missingYearsSet;
            if (missingSet && missingSet.has(yearNum)) {
              return `<td class="${colClass(c)}">ND</td>`;
            }
            if (v === null || v === undefined || String(v).trim() === '') {
              return `<td class="${colClass(c)}">0</td>`;
            }
          }
          return `<td class="${colClass(c)}">${escapeHtml(v === null || v === undefined ? '' : String(v))}</td>`;
        })
        .join('');
      return `<tr class="${isSelected ? 'is-selected' : ''}">${tds}</tr>`;
    })
    .join('');

  return `
    <div class="table-wrap" data-role="table-wrap">
      <table class="table teal">
        <thead><tr>${thead}</tr></thead>
        <tbody>${tbody}</tbody>
      </table>
    </div>
  `;
}

function renderPanel(active) {
  if (!panelEl || !panelHeaderEl) return;

  if (!state.species) {
    panelHeaderEl.innerHTML = '';
    panelEl.innerHTML = '<div class="empty">Table appears here when a CSV is loaded.</div>';
    return;
  }

  const cfg = getTableConfig(active);
  const rightActions = [];

  rightActions.push(
    `<button class="popout-button" type="button" data-action="export-table" aria-label="Download table" title="Download">⤓</button>`
  );

  if (!IS_POPOUT_TABLE) {
    rightActions.push(
      `<button class="popout-button" type="button" data-action="popout-table" aria-label="Pop out table" title="Pop out">⤢</button>`
    );
  }

  let leftContent = '';
  if (active === 'species') {
     const leftButtons = [
      `<button type="button" class="tab-button${state.speciesFilterRare ? ' active' : ''}" data-action="toggle-species-filter" data-filter="rare">Rare</button>`,
      `<button type="button" class="tab-button${state.speciesFilterOwls ? ' active' : ''}" data-action="toggle-species-filter" data-filter="owls">Owls</button>`
     ];
     leftContent = `<div class="panel-actions">${leftButtons.join('')}</div>`;
  } else {
     leftContent = `
    <div class="panel-title">
      ${escapeHtml(cfg.title)}
    </div>
  `;
  }

  const actionsHtml = rightActions.length ? `<div class="panel-actions">${rightActions.join('')}</div>` : '';
  panelHeaderEl.innerHTML = `${leftContent}${actionsHtml}`;

  if (active === 'species') {
    const years = state.yearsFull || state.years || [];
    const missingYearsSet = new Set((state.missingYears || []).filter((y) => typeof y === 'number' && Number.isFinite(y)));

    let rows = state.species;
    rows = (rows || []).filter((r) => !isSpRecord(r?.Species || ''));
    if (state.speciesFilterRare || state.speciesFilterOwls) {
      rows = (rows || []).filter((r) => {
        const spName = stripBracketedText(r?.Species || '');
        if (state.speciesFilterOwls && !/\bowls?\b/i.test(spName)) return false;
        if (state.speciesFilterRare) {
          let total = 0;
          for (const y of years) total += parseCount(r?.[String(y)]);
          if (total > 2) return false;
        }
        return true;
      });
    }

    panelEl.innerHTML = renderTable(rows, ['Species', ...years.map(String)], {
      clickSpecies: true,
      ndForMissingYears: true,
      missingYearsSet,
      selectedSpecies: state.selectedSpecies,
    });
    return;
  }

  if (active === 'weather') {
    panelEl.innerHTML = renderTable(state.weather, [
      'Year',
      'CountDate',
      'CountIndex',
      'LowTempF',
      'HighTempF',
      'AMClouds',
      'PMClouds',
      'AMRain',
      'PMRain',
      'AMSnow',
      'PMSnow',
    ]);
    return;
  }

  if (active === 'effort') {
    panelEl.innerHTML = renderTable(state.effort, ['Year', 'CountDate', 'CountIndex', 'NumHours']);
    return;
  }

  if (active === 'participation') {
    panelEl.innerHTML = renderTable(state.participation, ['Year', 'CountDate', 'CountIndex', 'NumParticipants']);
    return;
  }

  panelEl.innerHTML = '<div class="empty">Unknown tab.</div>';
}

function getTableConfig(active) {
  const r = state.ranges || {};
  if (active === 'species') {
    const years = state.yearsFull || state.years || [];
    return {
      title: 'Species',
      range: r.speciesYears || '—',
      rows: state.species || [],
      columns: ['Species', ...years.map(String)],
    };
  }
  if (active === 'weather') {
    return {
      title: 'Weather',
      range: r.weatherYears || '—',
      rows: state.weather || [],
      columns: ['Year', 'CountDate', 'CountIndex', 'LowTempF', 'HighTempF', 'AMClouds', 'PMClouds', 'AMRain', 'PMRain', 'AMSnow', 'PMSnow'],
    };
  }
  if (active === 'effort') {
    return {
      title: 'Effort',
      range: r.effortYears || '—',
      rows: state.effort || [],
      columns: ['Year', 'CountDate', 'CountIndex', 'NumHours'],
    };
  }
  if (active === 'participation') {
    return {
      title: 'Participation',
      range: r.participationYears || '—',
      rows: state.participation || [],
      columns: ['Year', 'CountDate', 'CountIndex', 'NumParticipants'],
    };
  }
  return { title: 'Table', range: '—', rows: [], columns: [] };
}

function csvEscapeCell(v) {
  if (v === null || v === undefined) return '';
  const s = String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replaceAll('"', '""')}"`;
  return s;
}

function rowsToCsv(rows, columns) {
  const cols = columns && columns.length ? columns : rows.length ? Object.keys(rows[0]) : [];
  const header = cols.map(csvEscapeCell).join(',');
  const lines = rows.map((r) => cols.map((c) => csvEscapeCell(r[c])).join(','));
  return [header, ...lines].join('\n');
}

function sanitizeFilenamePart(s) {
  return String(s || '')
    .trim()
    .replaceAll(/\s+/g, '_')
    .replaceAll(/[^A-Za-z0-9._-]/g, '')
    .slice(0, 80);
}

function downloadCsv(filename, csvText) {
  const blob = new Blob([csvText], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function clearPlot(msg) {
  if (!plotEl) return;
  const { mount, empty } = ensurePlotScaffold();
  const P = plotlyRef;
  if (P && mount) {
    try {
      P.purge(mount);
    } catch {
    }
  }
  setPlotHeader({ title: 'Plot', enableExport: false });
  if (mount) {
    try {
      mount.replaceChildren();
    } catch {
      mount.innerHTML = '';
    }
  }
  if (empty) {
    empty.textContent = msg;
    empty.style.display = '';
  }
}

async function plotLollipop({ x, y, title, markerColors = null }) {
  if (!plotEl) return;
  const { mount, empty } = ensurePlotScaffold();
  if (!mount) return;
  if (empty) empty.style.display = 'none';
  try {
    mount.replaceChildren();
  } catch {
    mount.innerHTML = '';
  }

  const Plotly = await getPlotly();
  const n = Math.min(x.length, y.length);
  const xs = [];
  const ys = [];
  for (let i = 0; i < n; i++) {
    const xv = x[i];
    const yv = y[i];
    if (typeof xv !== 'number' || !Number.isFinite(xv)) continue;
    if (typeof yv !== 'number' || !Number.isFinite(yv)) continue;
    xs.push(xv);
    ys.push(yv);
  }

  if (xs.length === 0) {
    clearPlot('No numeric data to plot.');
    return;
  }

  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const maxY = Math.max(...ys);

  const xAll = xs;
  const yAll = ys;

  const stems = (xv, yv) => {
    const outX = [];
    const outY = [];
    for (let i = 0; i < xv.length; i++) {
      outX.push(xv[i], xv[i], null);
      outY.push(0, yv[i], null);
    }
    return { outX, outY };
  };

  const allStem = stems(xAll, yAll);

  const traces = [
    {
      type: 'scatter',
      mode: 'lines',
      x: allStem.outX,
      y: allStem.outY,
      showlegend: false,
      hoverinfo: 'skip',
      line: { color: '#636EFA', width: 2 },
    },
    {
      type: 'scatter',
      mode: 'markers',
      x: xAll,
      y: yAll,
      showlegend: false,
      marker: { color: markerColors || '#636EFA', size: 6 },
      hovertemplate: '%{x}: %{y}<extra></extra>',
    },
  ];

  const ndAnnotations = [];
  if (Array.isArray(markerColors)) {
    for (let i = 0; i < Math.min(xAll.length, yAll.length, markerColors.length); i++) {
      if (markerColors[i] !== 'red') continue;
      ndAnnotations.push({
        xref: 'x',
        yref: 'y',
        x: xAll[i],
        y: yAll[i],
        text: '<b>ND</b>',
        showarrow: false,
        yshift: 14,
        align: 'center',
        font: { size: 10, color: 'red' },
      });
    }
  }

  setPlotHeader({ title, enableExport: true });

  const { width, height } = currentPlotSize();

  const layout = {
    ...PLOT_STYLE_BASE,
    ...(width ? { width } : {}),
    ...(height ? { height } : {}),
    annotations: [
      {
        align: 'left',
        font: { size: 18 },
        showarrow: false,
        text: `<b>${escapeHtmlForPlotText(title)}</b>`,
        x: 0.01,
        xanchor: 'left',
        xref: 'paper',
        y: 0.99,
        yanchor: 'top',
        yref: 'paper',
      },
      ...ndAnnotations,
    ],
    xaxis: {
      ...AXIS_STYLE_BASE,
      type: 'linear',
      tickmode: 'linear',
      tick0: Math.floor(minX / 5) * 5,
      dtick: 5,
      range: [minX - 0.7, maxX + 0.7],
      autorange: false,
      showgrid: true,
    },
    yaxis: {
      ...AXIS_STYLE_BASE,
      range: [0, Math.max(1, maxY * 1.08)],
      showgrid: true,
    },
  };

  Plotly.newPlot(mount, traces, layout, plotlyConfig());
  schedulePlotResize();
}

async function renderPlot(active) {
  if (!state.species) {
    clearPlot('Click on a species above to plot');
    return;
  }

  if (active === 'species') {
    if (!state.selectedSpecies) {
      clearPlot('Click on a species above to plot');
      return;
    }
    if (isSpRecord(state.selectedSpecies)) {
      clearPlot('This record is hidden (sp.).');
      return;
    }

    const selectedNorm = stripBracketedText(state.selectedSpecies).toLowerCase();
    const row =
      (state.species || []).find((r) => r.Species === state.selectedSpecies) ||
      (state.species || []).find((r) => stripBracketedText(r?.Species || '').toLowerCase() === selectedNorm);
    if (!row) {
      clearPlot('Species not found.');
      return;
    }
    if (isSpRecord(row.Species)) {
      clearPlot('This record is hidden (sp.).');
      return;
    }
    const years = (state.yearsFull || state.years || []).slice().sort((a, b) => a - b);
    const missingYearsSet = new Set((state.missingYears || []).filter((y) => typeof y === 'number' && Number.isFinite(y)));
    const x = years;
    const markerColors = years.map((yy) => {
      return missingYearsSet.has(yy) ? 'red' : '#636EFA';
    });
    const y = years.map((yy) => {
      return missingYearsSet.has(yy) ? 0 : parseCount(row[String(yy)]);
    });
    const fullTitle = `${withCbcSuffix(countDisplayName())} - ${normalizeRangeForTitle(
      state.ranges?.speciesYears || yearRangeStr(years)
    )}${IS_POPOUT_PLOT ? '' : ` - ${stripBracketedText(state.selectedSpecies)}`}`;
    await plotLollipop({ x, y, title: fullTitle, markerColors });
    return;
  }

  if (active === 'weather') {
    const rows = (state.weather || [])
      .filter((r) => typeof r.Year === 'number' && Number.isFinite(r.Year))
      .slice()
      .sort((a, b) => a.Year - b.Year);
    if (rows.length === 0) {
      clearPlot('No weather rows to plot.');
      return;
    }
    const x = rows.map((r) => r.Year);
    const high = rows.map((r) => (typeof r.HighTempF === 'number' ? r.HighTempF : null));
    const low = rows.map((r) => (typeof r.LowTempF === 'number' ? r.LowTempF : null));

    const weatherTitle = `${countDisplayName()} - Weather - ${normalizeRangeForTitle(state.ranges?.weatherYears || yearRangeStr(x))}`;

    setPlotHeader({ title: weatherTitle, enableExport: true });
    const { mount, empty } = ensurePlotScaffold();
    if (!mount) return;
    if (empty) empty.style.display = 'none';
    try {
      mount.replaceChildren();
    } catch {
      mount.innerHTML = '';
    }
    const Plotly = await getPlotly();
    const traces = [
      {
        type: 'scatter',
        mode: 'lines',
        name: 'High Temp (F)',
        x,
        y: high,
        line: { color: 'red' },
      },
      {
        type: 'scatter',
        mode: 'lines',
        name: 'Low Temp (F)',
        x,
        y: low,
        line: { color: '#7ec8ff' },
      },
    ];

    const layout = {
      ...PLOT_STYLE_BASE,
      margin: { l: 50, r: 50, t: 10, b: 30 },
      annotations: [
        {
          align: 'left',
          font: { size: 18 },
          showarrow: false,
          text: `<b>${escapeHtmlForPlotText(weatherTitle)}</b>`,
          x: 0.01,
          xanchor: 'left',
          xref: 'paper',
          y: 0.99,
          yanchor: 'top',
          yref: 'paper',
        },
      ],
      xaxis: { ...AXIS_STYLE_BASE, type: 'linear', tickmode: 'linear', tick0: 1970, dtick: 5, showgrid: true },
      yaxis: { ...AXIS_STYLE_BASE, title: { text: 'Temp (F)', standoff: 15 }, showgrid: true },
      legend: { orientation: 'h', x: 0, y: 1.12 },
    };
    const { width, height } = currentPlotSize();
    const sizedLayout = {
      ...layout,
      ...(width ? { width } : {}),
      ...(height ? { height } : {}),
    };
    Plotly.newPlot(mount, traces, sizedLayout, plotlyConfig());
    schedulePlotResize();
    return;
  }

  if (active === 'effort') {
    const rows = (state.effort || [])
      .filter((r) => typeof r.Year === 'number' && Number.isFinite(r.Year))
      .slice()
      .sort((a, b) => a.Year - b.Year);
    if (rows.length === 0) {
      clearPlot('No effort rows to plot.');
      return;
    }
    const x = rows.map((r) => r.Year);
    const y = rows.map((r) => (typeof r.NumHours === 'number' ? r.NumHours : null));
    const fullTitle = `${countDisplayName()} - Number of Hours - ${normalizeRangeForTitle(state.ranges?.effortYears || yearRangeStr(x))}`;
    await plotLollipop({ x, y, title: fullTitle });
    return;
  }

  if (active === 'participation') {
    const rows = (state.participation || [])
      .filter((r) => typeof r.Year === 'number' && Number.isFinite(r.Year))
      .slice()
      .sort((a, b) => a.Year - b.Year);
    if (rows.length === 0) {
      clearPlot('No participation rows to plot.');
      return;
    }
    const x = rows.map((r) => r.Year);
    const y = rows.map((r) => (typeof r.NumParticipants === 'number' ? r.NumParticipants : null));
    const fullTitle = `${countDisplayName()} - Number of Participants - ${normalizeRangeForTitle(state.ranges?.participationYears || yearRangeStr(x))}`;
    await plotLollipop({ x, y, title: fullTitle });
    return;
  }

  clearPlot('Select a tab to plot.');
}

async function downloadPngFromPlot() {
  if (!plotEl) return;
  if (!state.species) return;

  const { mount } = ensurePlotScaffold();
  if (!mount) return;

  const title = currentPlotTitle || `${countDisplayName()} - ${activeTab}`;
  const filename = `${sanitizeFilenamePart(title)}.png`;

  const Plotly = await getPlotly();
  Plotly.toImage(mount, { format: 'png', scale: 2 })
    .then((dataUrl) => {
      const a = document.createElement('a');
      a.href = dataUrl;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
    })
    .catch(() => {
    });
}



async function parseUploadedFile(file) {
  const name = String(file?.name || '').toLowerCase();

  let countInfo;
  let years;
  let meta;
  let speciesTable;
  let pe;
  let weatherRaw;

  if (!name.endsWith('.csv')) {
    throw new Error('Only .csv files are supported in this version.');
  }

  if (typeof file.size === 'number' && file.size > MAX_CSV_BYTES) {
    throw new Error('Error: .csv files must be 10MB or smaller.');
  }
  const text = await file.text();
  if (text.length > MAX_CSV_BYTES) {
    throw new Error('Error: .csv files must be 10MB or smaller.');
  }
  const { parseHistoricalResultsByCountCsv } = await import('./csv_historical_results.js');
  const parsed = parseHistoricalResultsByCountCsv(text);
  countInfo = parsed.countInfo;
  years = parsed.years;
  meta = parsed.meta;
  speciesTable = parsed.speciesTable;
  pe = parsed.participantsEffort;
  weatherRaw = parsed.weatherRaw;

  const yearsFull = continuousYears(years);
  const missingYears = missingYearsFromRanges(years, yearsFull);

  const metaYearByIdx = new Map((meta || []).map((r) => [r.CountIndex, r.Year]));
  pe = (pe || []).map((r) => {
    const metaYear = metaYearByIdx.get(r?.CountIndex);
    const yearFromDate = r?.CountDate ? yearFromCountDateString(r.CountDate) : null;
    const Year =
      (typeof metaYear === 'number' && Number.isFinite(metaYear))
        ? metaYear
        : (typeof yearFromDate === 'number' && Number.isFinite(yearFromDate))
          ? yearFromDate
          : (r?.Year ?? null);
    return { ...r, Year };
  });

  const weather = joinWeatherWithYearInfo(weatherRaw || [], pe || [], meta || []);

  const participation = (pe || []).map((r) => ({
    Year: r.Year,
    CountDate: r.CountDate,
    CountIndex: r.CountIndex,
    NumParticipants: r.NumParticipants,
  }));

  const effort = (pe || []).map((r) => ({
    Year: r.Year,
    CountDate: r.CountDate,
    CountIndex: r.CountIndex,
    NumHours: r.NumHours,
  }));

  const ranges = {
    speciesYears: yearRangeStr(continuousYears(years)),
    weatherYears: yearRangeStr(yearsFromRows(weather)),
    effortYears: yearRangeStr(yearsFromRows(effort)),
    participationYears: yearRangeStr(yearsFromRows(participation)),
  };

  const maxCountIndex = inferMaxCountIndex({ meta, weather, effort, participation });
  const sourceUrlBase = buildHistoricalSourceUrl({ countInfo, sy: 1, ey: CURRENT_MAX_COUNT_INDEX });
  const sourceUrl = sourceUrlBase ? normalizeUpdateUrl(sourceUrlBase) : null;

  return {
    years,
    yearsFull,
    missingYears,
    meta,
    sourceUrl,
    maxCountIndex,
    species: speciesTable,
    weather,
    effort,
    participation,
    countInfo,
    ranges,
  };
}

async function handleFile(file) {
  try {
    panelEl.innerHTML = '<div class="empty">Parsing file…</div>';
    summaryEl.innerHTML = '<div class="empty">Processing…</div>';

    const parsed = await parseUploadedFile(file);
    state = {
      ...parsed,
      filename: file.name,
      selectedSpecies: null,
      speciesFilterRare: false,
      speciesFilterOwls: false,
    };

    renderCountHeader();
    renderSummary();
    setActiveTab('species');

    await setSelectedCircleByCode(state?.countInfo?.CountCode || '');

    setTimeout(() => {
      if (leafletMap) leafletMap.invalidateSize();
    }, 0);

    saveCurrentStateToSqlite().catch(() => {
    });
  } catch (e) {
    const msg = e?.message || String(e);
    summaryEl.innerHTML = `<div class="empty">Error: ${escapeHtml(msg)}</div>`;
    panelEl.innerHTML = `<div class="empty">Error: ${escapeHtml(msg)}</div>`;
  }
}

panelEl?.addEventListener('click', (e) => {
  const cell = e.target?.closest?.('[data-action="plot-species"]');
  if (!cell) return;
  const sp = cell.getAttribute('data-species');
  if (!sp) return;
  state.selectedSpecies = sp;
  renderPanel('species');
  renderPlot('species');
});

ingestedEl?.addEventListener('click', async (e) => {
  const deleteBtn = e.target?.closest?.('[data-action="delete-count"]');
  if (deleteBtn) {
    e.preventDefault();
    const code = deleteBtn.getAttribute('data-code');
    if (!code) return;

    const ok = window.confirm(`Delete stored data for ${code}?`);
    if (!ok) return;

    (async () => {
      await idbDel(`${IDB_KEY_DB_PREFIX}${code}`);
      const idx = await loadCountsIndex();
      const next = (idx || []).filter((r) => r.code !== code);
      await saveCountsIndex(next);
      renderIngestedCountsList(next);

      const currentCode = cleanText(state?.countInfo?.CountCode || '');
      if (currentCode && currentCode === code) {
        state = {
          species: null,
          weather: null,
          effort: null,
          participation: null,
          meta: null,
          sourceUrl: null,
          maxCountIndex: null,
          years: null,
          yearsFull: null,
          missingYears: null,
          filename: null,
          countInfo: null,
          ranges: null,
          selectedSpecies: null,
          speciesFilterRare: false,
          speciesFilterOwls: false,
        };
        renderCountHeader();
        renderSummary();
        panelHeaderEl.innerHTML = '';
        panelEl.innerHTML = '<div class="empty">Table appears here when a CSV is loaded.</div>';
        clearPlot('Click on a species above to plot');
      }
    })().catch((err) => {
      const msg = err?.message || String(err);
      panelEl.innerHTML = `<div class="empty">Error: ${escapeHtml(msg)}</div>`;
    });

    return;
  }

  const updateBtn = e.target?.closest?.('[data-action="update-count"]');
  if (updateBtn) {
    e.preventDefault();
    const code = updateBtn.getAttribute('data-code');
    if (!code) return;

    const updateCell = updateBtn.closest?.('.update-cell');
    const metaEl = updateCell?.querySelector?.('.update-meta') || null;
    const metaPrev = metaEl ? metaEl.textContent : null;

    const rawCid = updateBtn.getAttribute('data-cid');
    const rawName = updateBtn.getAttribute('data-name');

    const prevDisabled = updateBtn.disabled;
    updateBtn.disabled = true;

    if (metaEl) metaEl.textContent = 'Updating…';

    (async () => {
      let picked = null;

      if (rawCid) {
        const cidNum = parseInt(String(rawCid).trim(), 10);
        if (Number.isFinite(cidNum)) {
          picked = { Abbrev: code, Circle_id: cidNum, Name: rawName || '' };
        }
      }

      if (!picked && KNOWN_COUNT_IDS[code]) {
        picked = { Abbrev: code, Circle_id: KNOWN_COUNT_IDS[code] };
      } else if (!picked) {
        try {
          const rows = await ensureCirclesIndexLoaded();
          const hit = (rows || []).find((r) => r?.Abbrev === code);
          if (hit && Number.isFinite(Number(hit.Circle_id))) {
            picked = { Abbrev: hit.Abbrev, Circle_id: hit.Circle_id, Name: hit.Name };
          }
        } catch {
        }
      }

      if (picked) {
        await updateStoredCircleFromWorker({ abbrev: picked.Abbrev, cid: picked.Circle_id, name: picked.Name || '' });

        const currentCode = cleanText(state?.countInfo?.CountCode || '');
        const isCurrent = currentCode && currentCode === code;
        const isSelected = cleanText(selectedCircle?.Abbrev || '') === cleanText(code);
        if (isCurrent || isSelected || !state?.species) {
          panelEl.innerHTML = '<div class="empty">Loading…</div>';
          await loadStateFromSqlite(code);
        }
        return;
      }

      throw new Error('Could not resolve circle ID for update.');
    })()
      .catch((err) => {
        const msg = err?.message || String(err);
        panelEl.innerHTML = `<div class="empty">${escapeHtml(msg)}</div>`;

        if (metaEl) metaEl.textContent = 'Update failed';
      })
      .finally(() => {
        updateBtn.disabled = prevDisabled;
        if (metaEl && metaEl.textContent === 'Updating…') metaEl.textContent = metaPrev || '';
      });

    return;
  }

  const link = e.target?.closest?.('[data-action="load-count"]');
  if (!link) return;
  e.preventDefault();
  const code = link.getAttribute('data-code');
  if (!code) return;
  panelEl.innerHTML = '<div class="empty">Loading stored count…</div>';
  loadStateFromSqlite(code).catch((err) => {
    const msg = err?.message || String(err);
    panelEl.innerHTML = `<div class="empty">Error: ${escapeHtml(msg)}</div>`;
  });
});

countSearchEl?.addEventListener('input', () => {
  scheduleCircleSearch();
});

countSearchEl?.addEventListener('focus', () => {
  ensureCirclesIndexLoaded().catch(() => {});
});

countSearchResultsEl?.addEventListener('click', async (e) => {
  const btn = e.target?.closest?.('[data-action="pick-circle"]');
  if (!btn) return;
  const code = cleanText(btn.getAttribute('data-code') || '');
  if (!code) return;
  const rows = await ensureCirclesIndexLoaded();
  const picked = rows.find((r) => r.Abbrev === code);
  if (!picked) return;
  setSelectedCircle(picked, { fetchIfMissing: true });
});

function onPickFile() {
  fileInputEl?.click();
}

dropzoneEl?.addEventListener('click', onPickFile);
dropzoneEl?.addEventListener('keydown', (e) => {
  if (e.key === 'Enter' || e.key === ' ') {
    e.preventDefault();
    onPickFile();
  }
});

fileInputEl?.addEventListener('change', () => {
  const f = fileInputEl.files?.[0];
  if (!f) return;
  handleFile(f);
});

const setDragState = (isActive) => {
  if (!dropzoneEl) return;
  dropzoneEl.style.background = isActive ? '#f2f6ff' : '#fafafa';
};

dropzoneEl?.addEventListener('dragover', (e) => {
  e.preventDefault();
  e.stopPropagation();
  try {
    if (e.dataTransfer) e.dataTransfer.dropEffect = 'copy';
  } catch {
  }
  setDragState(true);
});

dropzoneEl?.addEventListener('dragleave', () => setDragState(false));
dropzoneEl?.addEventListener('drop', (e) => {
  e.preventDefault();
  e.stopPropagation();
  setDragState(false);
  const f = e.dataTransfer?.files?.[0];
  if (!f) return;
  handleFile(f);
});

const preventBrowserFileDrop = (e) => {
  e.preventDefault();
  try {
    if (e.dataTransfer) e.dataTransfer.dropEffect = 'copy';
  } catch {
  }
};

document.addEventListener('dragenter', preventBrowserFileDrop, { capture: true });
document.addEventListener('dragover', preventBrowserFileDrop, { capture: true });
document.addEventListener('drop', preventBrowserFileDrop, { capture: true });

async function applyDefaultSelections() {
  if (state?.species) return;

  try {
    await loadStateFromSqlite('CAPC');
  } catch {
    return;
  }

  const target = 'Wrentit';
  const row = (state.species || []).find(
    (r) => stripBracketedText(r?.Species || '').toLowerCase() === target.toLowerCase()
  );
  if (!row || isSpRecord(row.Species)) return;

  state.selectedSpecies = row.Species;
  await renderPlot('species');
}

ensureSeedCountsImported()
  .catch(() => {})
  .finally(() => {
    (async () => {
      await refreshIngestedCounts();
      requestSidebarSync();
      await applyDefaultSelections();
      requestSidebarSync();
    })().catch(() => {});
  });

window.addEventListener('resize', () => {
  schedulePlotResize();
  if (leafletMap) leafletMap.invalidateSize();
  requestSidebarSync();
});
