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

const CBC_RESULTS_URL = 'https://netapp.audubon.org/CBCObservation/Historical/ResultsByCount.aspx';
const CBC_HISTORICAL_RESULTS_URL = 'https://netapp.audubon.org/CBCObservation/Reports/HistoricalResultsByCount.aspx';
const CBC_MAP_URL = 'https://gis.audubon.org/christmasbirdcount/';
const CBC_126_CIRCLES_QUERY_URL =
  'https://services1.arcgis.com/lDFzr3JyGEn5Eymu/arcgis/rest/services/CBC_126/FeatureServer/0/query';
const MAX_CSV_BYTES = 10 * 1024 * 1024;
const CURRENT_MAX_COUNT_INDEX = 125;

const KNOWN_COUNT_IDS = {
  CAPC: 57023,
  CASM: 57049,
  CACC: 57441,
  CARC: 58818,
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

app.innerHTML = `
  <div class="app">
    <header class="topbar">
      <div class="topbarTitle">Christmas Bird Count Historical Data</div>
      <button id="infoBtn" class="infoButton" type="button" aria-label="Info">i</button>
    </header>

    <div class="content-row">

    <aside class="sidebar">
    <div class="sidebar-card nav-card card">
      <div class="cardBody">
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
          <div id="countSearchSelected" class="count-search-selected hidden" aria-label="Selected count"></div>
        </div>

        <div id="dropzone" class="dropzone" tabindex="0" role="button" aria-label="Drop CSV here">
          <div class="dropzone-title">Drop CSV</div>
          <div class="dropzone-sub">…or click to choose a file</div>
          <input id="fileInput" class="file-input" type="file" accept=".csv,text/csv" />
        </div>

        <div class="section-title">Imported data</div>
        <div id="ingested" class="summary">
          <div class="empty">None yet.</div>
        </div>

        <div class="section-title">Summary</div>
        <div id="summary" class="summary">
          <div class="empty">No CSV loaded.</div>
        </div>
      </div>
    </div>

    <div class="sidebar-card map-card card">
      <div class="cardBody map-body">
        <div id="map" class="map">
          <div class="empty">No location loaded.</div>
        </div>
      </div>
    </div>

    <div class="sidebar-card resources-card card">
      <div class="cardHeader">Resources</div>
      <div class="cardBody resources-body">
        <div class="resources-links">
          <a class="link" href="${CBC_RESULTS_URL}" target="_blank" rel="noopener noreferrer">Download Audubon CBC Count Results</a>
          <a class="link" href="${CBC_MAP_URL}" target="_blank" rel="noopener noreferrer">Audubon CBC map</a>
        </div>
      </div>
    </div>
    </aside>

    <main class="main">
      <div id="countHeader" class="count-header card">
        <div class="count-header-bar">
          <div id="countHeaderText" class="count-header-text">Load a CSV or select existing count data</div>
          <div class="tabs" role="tablist" aria-label="Tables">
            <button id="tabSpecies" class="tab-button active" type="button">Species</button>
            <button id="tabWeather" class="tab-button" type="button">Weather</button>
            <button id="tabEffort" class="tab-button" type="button">Effort</button>
            <button id="tabParticipation" class="tab-button" type="button">Participation</button>
          </div>
          <button id="exportCsvBtn" class="export-button export-button-header" type="button" disabled>Export CSV</button>
        </div>
      </div>

      <div class="split">
        <div class="table-pane card">
          <div id="panelHeader" class="panel-header"></div>
          <div id="panel" class="panel">
            <div class="empty">Table appears here when a CSV is loaded.</div>
          </div>
        </div>

        <div class="plot-pane card">
          <div id="plotHeader" class="panel-header"></div>
          <div id="plot" class="plot">
            <div class="empty">Plot appears here when a CSV is loaded.</div>
          </div>
        </div>
      </div>
    </main>
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
              Facilitating the extraction of historical data from Audubon CBC historical files available from
              <a href="${CBC_RESULTS_URL}" target="_blank" rel="noopener noreferrer">CBC count results</a>
            </li>
            <li>Allow quick visualization of historical count data</li>
          </ul>

          <p style="margin-top: 12px; font-weight: 800;">How to use</p>
          <p>
            Drag and drop or select upload button to add a CSV file from the Audubon CBC portal. Once ingested the information will be visible and downloadable in a generic text format.
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

const dropzoneEl = document.getElementById('dropzone');
const fileInputEl = document.getElementById('fileInput');
const ingestedEl = document.getElementById('ingested');
const summaryEl = document.getElementById('summary');
const mapEl = document.getElementById('map');
const countHeaderEl = document.getElementById('countHeader');
const countHeaderTextEl = document.getElementById('countHeaderText');
const exportCsvBtnEl = document.getElementById('exportCsvBtn');
const panelHeaderEl = document.getElementById('panelHeader');
const panelEl = document.getElementById('panel');
const plotHeaderEl = document.getElementById('plotHeader');
const plotEl = document.getElementById('plot');

const countSearchEl = document.getElementById('countSearch');
const countSearchResultsEl = document.getElementById('countSearchResults');
const countSearchSelectedEl = document.getElementById('countSearchSelected');

const navCardEl = document.querySelector('.nav-card');
const mapCardEl = document.querySelector('.map-card');
const resourcesCardEl = document.querySelector('.resources-card');

function syncSidebarHeights() {
  if (!navCardEl || !mapCardEl) return;
  if (!countHeaderEl) return;
  const tablePaneEl = document.querySelector('.table-pane');
  const sidebarEl = navCardEl.closest('.sidebar');
  if (!tablePaneEl || !sidebarEl) return;

  const headerRect = countHeaderEl.getBoundingClientRect();
  const tableRect = tablePaneEl.getBoundingClientRect();
  const sidebarRect = sidebarEl.getBoundingClientRect();

  let navHeight = Math.round(tableRect.bottom - headerRect.top);
  if (!Number.isFinite(navHeight) || navHeight <= 0) return;
  navHeight += 2;
  const GAP_PX = 14;
  const MIN_MAP_PX = 160;
  let resourcesHeight = 0;
  if (resourcesCardEl) {
    resourcesCardEl.style.flex = '0 0 auto';
    resourcesCardEl.style.height = 'auto';
    resourcesHeight = Math.round(resourcesCardEl.getBoundingClientRect().height);
  }

  const gaps = resourcesCardEl ? GAP_PX * 2 : GAP_PX;
  const maxNav = Math.max(
    200,
    Math.floor(sidebarRect.height - gaps - resourcesHeight - MIN_MAP_PX)
  );
  navHeight = Math.min(navHeight, maxNav);
  navHeight = Math.max(220, navHeight);

  const mapHeight = Math.max(
    MIN_MAP_PX,
    Math.floor(sidebarRect.height - gaps - resourcesHeight - navHeight)
  );

  navCardEl.style.flex = '0 0 auto';
  mapCardEl.style.flex = '0 0 auto';
  navCardEl.style.height = `${navHeight}px`;
  mapCardEl.style.height = `${mapHeight}px`;

  if (leafletMap) leafletMap.invalidateSize();
}

L.Icon.Default.mergeOptions({
  iconRetinaUrl: leafletMarker2x,
  iconUrl: leafletMarker,
  shadowUrl: leafletMarkerShadow,
});

let leafletMap = null;
let leafletMapMarker = null;
let leafletCircle = null;
let _sidebarSyncQueued = false;
function requestSidebarSync() {
  if (_sidebarSyncQueued) return;
  _sidebarSyncQueued = true;
  requestAnimationFrame(() => {
    _sidebarSyncQueued = false;
    syncSidebarHeights();
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

  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; OpenStreetMap contributors',
  }).addTo(leafletMap);

  requestSidebarSync();
  return leafletMap;
}

try {
  const tablePaneEl = document.querySelector('.table-pane');
  if (tablePaneEl && window.ResizeObserver) {
    const ro = new ResizeObserver(() => requestSidebarSync());
    ro.observe(tablePaneEl);
  }
} catch {
}

function updateMapFromState() {
  if (!mapEl) return;
  const ci = state?.countInfo || {};
  const lat = typeof ci.Lat === 'number' ? ci.Lat : parseFloat(String(ci.Lat ?? '').trim());
  const lon = typeof ci.Lon === 'number' ? ci.Lon : parseFloat(String(ci.Lon ?? '').trim());
  const has = Number.isFinite(lat) && Number.isFinite(lon);

  if (!has) {
    if (!leafletMap) {
      mapEl.innerHTML = '<div class="empty">No location loaded.</div>';
      return;
    }
    if (leafletMapMarker) {
      leafletMapMarker.remove();
      leafletMapMarker = null;
    }
    if (leafletCircle) {
      leafletCircle.remove();
      leafletCircle = null;
    }
    return;
  }

  const map = ensureLeafletMap();
  if (!map) return;

  const center = [lat, lon];
  if (!leafletMapMarker) leafletMapMarker = L.marker(center).addTo(map);
  else leafletMapMarker.setLatLng(center);

  const radiusMeters = 7.5 * 1609.344;
  if (!leafletCircle) {
    leafletCircle = L.circle(center, {
      radius: radiusMeters,
      color: 'red',
      weight: 2,
      fill: false,
      fillOpacity: 0,
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
}

let circlesIndexPromise = null;
let circlesIndex = null;
let selectedCircle = null;

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

    const r = await fetch(u.toString(), { credentials: 'omit' });
    if (!r.ok) throw new Error('Failed to load circle index.');
    const data = await r.json();
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

function renderSelectedCircle() {
  if (!countSearchSelectedEl) return;
  if (!selectedCircle) {
    countSearchSelectedEl.replaceChildren();
    countSearchSelectedEl.classList.add('hidden');
    return;
  }
  countSearchSelectedEl.classList.remove('hidden');
  const url = buildDefaultCsvDownloadUrl({ abbrev: selectedCircle.Abbrev, cid: selectedCircle.Circle_id });
  countSearchSelectedEl.replaceChildren();

  const addRow = (k, v) => {
    const row = document.createElement('div');
    row.className = 'count-selected-row';
    const kk = document.createElement('span');
    kk.className = 'k';
    kk.textContent = k;
    const vv = document.createElement('span');
    vv.textContent = String(v ?? '');
    row.appendChild(kk);
    row.appendChild(vv);
    countSearchSelectedEl.appendChild(row);
  };

  addRow('Name', selectedCircle.Name);
  addRow('Code', selectedCircle.Abbrev);
  addRow('ID', String(selectedCircle.Circle_id));

  const actions = document.createElement('div');
  actions.className = 'count-selected-actions';
  const a = document.createElement('a');
  a.className = 'button-link';
  a.textContent = 'Download CSV';
  a.target = '_blank';
  a.rel = 'noopener noreferrer';
  if (url) a.href = url;
  else a.href = '#';
  actions.appendChild(a);
  countSearchSelectedEl.appendChild(actions);
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
  const seeds = ['CAPC', 'CARC'];
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
  if (rows.length === 0) {
    ingestedEl.replaceChildren(Object.assign(document.createElement('div'), { className: 'empty', textContent: 'None yet.' }));
    return;
  }

  const wrap = document.createElement('div');
  wrap.className = 'table-wrap';
  const table = document.createElement('table');
  table.className = 'table';

  const thead = document.createElement('thead');
  const headRow = document.createElement('tr');
  for (const h of ['Count', 'Code', 'Years', 'Update']) {
    const th = document.createElement('th');
    th.textContent = h;
    if (h === 'Update') th.className = 'col-update';
    headRow.appendChild(th);
  }
  thead.appendChild(headRow);

  const tbody = document.createElement('tbody');
  for (const r of rows) {
    const name = r?.name || 'Count';
    const code = r?.code || '';
    const range = r?.range || '—';
    const updatedText = formatShortDate(r?.updatedAt);

    const tr = document.createElement('tr');

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
    const btnUpdate = document.createElement('button');
    btnUpdate.className = 'update-button';
    btnUpdate.type = 'button';
    btnUpdate.dataset.action = 'update-count';
    btnUpdate.dataset.code = code;
    btnUpdate.setAttribute('aria-label', 'Update');
    btnUpdate.title = 'Update';
    btnUpdate.textContent = '↻';

    const btnDel = document.createElement('button');
    btnDel.className = 'update-button delete-button';
    btnDel.type = 'button';
    btnDel.dataset.action = 'delete-count';
    btnDel.dataset.code = code;
    btnDel.setAttribute('aria-label', 'Delete');
    btnDel.title = 'Delete';
    btnDel.textContent = 'X';

    cell.appendChild(btnUpdate);
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
  const buf = await idbGet(`${IDB_KEY_DB_PREFIX}${code}`);
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
  };
  renderCountHeader();
  renderSummary();
  setActiveTab('species');
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
  if (!plotHeaderEl) return;
  currentPlotTitle = title || null;
  applyHeaderTheme();
  plotHeaderEl.innerHTML = title ? `<div class="panel-title">${escapeHtml(String(title))}</div>` : '';
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
      P.Plots.resize(plotEl);
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

async function setActiveTab(name) {
  activeTab = name;
  for (const [k, el] of Object.entries(tabs)) {
    el.classList.toggle('active', k === name);
  }
  applyHeaderTheme();
  renderPanel(name);
  await renderPlot(name);
  schedulePlotResize();
}

for (const [k, el] of Object.entries(tabs)) {
  el.addEventListener('click', () => {
    setActiveTab(k);
  });
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
    ['File', state.filename || '—'],
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
  if (!state.species) {
    countHeaderTextEl.textContent = 'Load a CSV or select existing count data';
    if (exportCsvBtnEl) exportCsvBtnEl.setAttribute('disabled', '');
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
  if (exportCsvBtnEl) exportCsvBtnEl.removeAttribute('disabled');
  requestSidebarSync();
}

function stripBracketedText(s) {
  const raw = String(s ?? '');
  return raw.replaceAll(/\s*\[[^\]]*\]\s*/g, ' ').replaceAll(/\s+/g, ' ').trim();
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
      return `<tr>${tds}</tr>`;
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
  if (active === 'species') {
    panelHeaderEl.innerHTML = '';
  } else {
    panelHeaderEl.innerHTML = `
      <div class="panel-title">
        ${escapeHtml(cfg.title)}
        <span class="panel-sub">${escapeHtml(cfg.range)}</span>
      </div>
    `;
  }

  if (active === 'species') {
    const years = state.yearsFull || state.years || [];
    const missingYearsSet = new Set((state.missingYears || []).filter((y) => typeof y === 'number' && Number.isFinite(y)));
    panelEl.innerHTML = renderTable(state.species, ['Species', ...years.map(String)], {
      clickSpecies: true,
      ndForMissingYears: true,
      missingYearsSet,
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
  const P = plotlyRef;
  if (P) {
    try {
      P.purge(plotEl);
    } catch {
    }
  }
  setPlotHeader({ title: 'Plot', enableExport: false });
  plotEl.innerHTML = `<div class="empty">${escapeHtml(msg)}</div>`;
}

async function plotLollipop({ x, y, title, markerColors = null }) {
  if (!plotEl) return;
  plotEl.innerHTML = '';

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

  Plotly.newPlot(plotEl, traces, layout, plotlyConfig());
  schedulePlotResize();
}

async function renderPlot(active) {
  if (!state.species) {
    clearPlot('Plot appears here when a CSV is loaded.');
    return;
  }

  if (active === 'species') {
    if (!state.selectedSpecies) {
      clearPlot('Click a species name to plot.');
      return;
    }
    const row = (state.species || []).find((r) => r.Species === state.selectedSpecies);
    if (!row) {
      clearPlot('Species not found.');
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
    )} - ${stripBracketedText(state.selectedSpecies)}`;
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
    plotEl.innerHTML = '';
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
    Plotly.newPlot(plotEl, traces, sizedLayout, plotlyConfig());
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

  const title = currentPlotTitle || `${countDisplayName()} - ${activeTab}`;
  const filename = `${sanitizeFilenamePart(title)}.png`;

  const Plotly = await getPlotly();
  Plotly.toImage(plotEl, { format: 'png', scale: 2 })
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

plotHeaderEl?.addEventListener('click', (e) => {
  const btn = e.target?.closest?.('[data-action="export-plot"]');
  if (!btn) return;
  if (btn.hasAttribute('disabled')) return;
  void downloadPngFromPlot();
});

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
    };

    renderCountHeader();
    renderSummary();
    setActiveTab('species');

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

exportCsvBtnEl?.addEventListener('click', () => {
  if (!state.species) return;
  const cfg = getTableConfig(activeTab);
  const csv = rowsToCsv(cfg.rows || [], cfg.columns || []);
  const ci = state.countInfo || {};
  const base = sanitizeFilenamePart(ci.CountCode || ci.CountName || 'cbc');
  const tab = sanitizeFilenamePart(cfg.title || activeTab);
  downloadCsv(`${base}_${tab}.csv`, csv);
});

panelEl?.addEventListener('click', (e) => {
  const cell = e.target?.closest?.('[data-action="plot-species"]');
  if (!cell) return;
  const sp = cell.getAttribute('data-species');
  if (!sp) return;
  state.selectedSpecies = sp;
  renderPlot('species');
});

ingestedEl?.addEventListener('click', (e) => {
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
        };
        renderCountHeader();
        renderSummary();
        panelHeaderEl.innerHTML = '';
        panelEl.innerHTML = '<div class="empty">Table appears here when a CSV is loaded.</div>';
        clearPlot('Plot appears here when a CSV is loaded.');
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

    if (KNOWN_COUNT_IDS[code]) {
      const url = buildDefaultCsvDownloadUrl({ abbrev: code, cid: KNOWN_COUNT_IDS[code] });
      if (url) {
        const a = document.createElement('a');
        a.href = url;
        a.target = '_blank';
        a.rel = 'noopener noreferrer';
        document.body.appendChild(a);
        a.click();
        a.remove();
        return;
      }
    }

    idbGet(`${IDB_KEY_DB_PREFIX}${code}`)
      .then(async (buf) => {
        if (!buf) throw new Error('No stored database found for that count.');
        const SQL = await getSql();
        const db = new SQL.Database(new Uint8Array(buf));
        const resUrl = db.exec('SELECT value FROM kv WHERE key = ' + JSON.stringify('sourceUrl'));
        const resCi = db.exec('SELECT value FROM kv WHERE key = ' + JSON.stringify('countInfo'));
        db.close();

        const rawUrl = resUrl?.[0]?.values?.[0]?.[0] ? JSON.parse(resUrl[0].values[0][0]) : null;
        const rawCountInfo = resCi?.[0]?.values?.[0]?.[0] ? JSON.parse(resCi[0].values[0][0]) : null;
        const url =
          normalizeUpdateUrl(rawUrl) ||
          normalizeUpdateUrl(
            buildHistoricalSourceUrl({
              countInfo: rawCountInfo || { CountCode: code },
              sy: 1,
              ey: CURRENT_MAX_COUNT_INDEX,
            })
          );
        if (!url) throw new Error('No source URL available for this count. Re-ingest the file to compute it.');

        const a = document.createElement('a');
        a.href = url;
        a.target = '_blank';
        a.rel = 'noopener noreferrer';
        document.body.appendChild(a);
        a.click();
        a.remove();
      })
      .catch((err) => {
        const msg = err?.message || String(err);
        panelEl.innerHTML = `<div class="empty">Error: ${escapeHtml(msg)}</div>`;
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
  selectedCircle = picked;
  renderSelectedCircle();
  if (countSearchResultsEl) countSearchResultsEl.innerHTML = '';
  if (countSearchEl) countSearchEl.value = `${picked.Name} (${picked.Abbrev})`;
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

ensureSeedCountsImported()
  .catch(() => {})
  .finally(() => {
    refreshIngestedCounts();
    requestSidebarSync();
  });

window.addEventListener('resize', () => {
  schedulePlotResize();
  if (leafletMap) leafletMap.invalidateSize();
  requestSidebarSync();
});
