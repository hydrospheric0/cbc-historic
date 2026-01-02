import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';

import initSqlJs from 'sql.js';
import { parseHistoricalResultsByCountCsv } from '../src/csv_historical_results.js';

const require = createRequire(import.meta.url);

const CBC_HISTORICAL_RESULTS_URL = 'https://netapp.audubon.org/CBCObservation/Reports/HistoricalResultsByCount.aspx';
const CURRENT_MAX_COUNT_INDEX = 125;

const SEEDS = [
  { code: 'CAPC', cid: 57023 },
  { code: 'CARC', cid: 58818 },
];

function yearRangeStr(years) {
  if (!years || years.length === 0) return '—';
  const minY = Math.min(...years);
  const maxY = Math.max(...years);
  return minY === maxY ? String(minY) : `${minY}–${maxY}`;
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

function parseYearFromCountDateString(countDate) {
  const s = String(countDate ?? '').trim();
  if (!s) return null;
  const m = s.match(/\b(19\d{2}|20\d{2})\b/);
  if (!m) return null;
  const y = parseInt(m[1], 10);
  return Number.isFinite(y) ? y : null;
}

function joinWeatherWithYearInfo(weatherRows, participantsEffortRows, metaRows) {
  const peByIdx = new Map((participantsEffortRows || []).map((r) => [r.CountIndex, r]));
  const metaByIdx = new Map((metaRows || []).map((r) => [r.CountIndex, r]));

  return (weatherRows || []).map((w) => {
    const pe = peByIdx.get(w.CountIndex);
    const meta = metaByIdx.get(w.CountIndex);
    const Year = (pe && pe.Year) || (meta && meta.Year) || null;
    const CountDate = (pe && pe.CountDate) || (meta && meta.CountDate) || null;
    return { Year, CountDate, ...w };
  });
}

function inferMaxCountIndex({ meta, weather, effort, participation }) {
  const idxs = [];
  for (const r of meta || []) {
    if (typeof r?.CountIndex === 'number' && Number.isFinite(r.CountIndex)) idxs.push(r.CountIndex);
  }
  for (const r of weather || []) {
    if (typeof r?.CountIndex === 'number' && Number.isFinite(r.CountIndex)) idxs.push(r.CountIndex);
  }
  for (const r of effort || []) {
    if (typeof r?.CountIndex === 'number' && Number.isFinite(r.CountIndex)) idxs.push(r.CountIndex);
  }
  for (const r of participation || []) {
    if (typeof r?.CountIndex === 'number' && Number.isFinite(r.CountIndex)) idxs.push(r.CountIndex);
  }
  return idxs.length ? Math.max(...idxs) : null;
}

function buildCsvUrl({ code, cid }) {
  const params = new URLSearchParams();
  params.set('rf', 'CSV');
  params.set('cid', String(cid));
  params.set('sy', '1');
  params.set('ey', String(CURRENT_MAX_COUNT_INDEX));
  params.set('so', '0');
  params.set('abbrev', String(code));
  return `${CBC_HISTORICAL_RESULTS_URL}?${params.toString()}`;
}

async function getSql() {
  const wasmPath = require.resolve('sql.js/dist/sql-wasm.wasm');
  const wasmBinary = await fs.readFile(wasmPath);
  return initSqlJs({ wasmBinary });
}

async function fetchCsvText(url) {
  const r = await fetch(url, { redirect: 'follow' });
  if (!r.ok) throw new Error(`Failed to fetch CSV (${r.status})`);
  return await r.text();
}

async function buildStateFromCsvText(csvText) {
  const parsed = parseHistoricalResultsByCountCsv(csvText);
  const countInfo = parsed.countInfo;
  const years = parsed.years;
  const meta = parsed.meta;
  const speciesTable = parsed.speciesTable;
  let pe = parsed.participantsEffort;
  const weatherRaw = parsed.weatherRaw;

  const yearsFull = continuousYears(years);
  const missingYears = missingYearsFromRanges(years, yearsFull);

  const metaYearByIdx = new Map((meta || []).map((r) => [r.CountIndex, r.Year]));
  pe = (pe || []).map((r) => {
    const metaYear = metaYearByIdx.get(r?.CountIndex);
    const yearFromDate = r?.CountDate ? parseYearFromCountDateString(r.CountDate) : null;
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

  return {
    countInfo,
    ranges,
    years,
    yearsFull,
    missingYears,
    meta,
    sourceUrl: null,
    maxCountIndex,
    species: speciesTable,
    weather,
    effort,
    participation,
  };
}

async function writeSqlite({ outPath, state }) {
  const SQL = await getSql();
  const db = new SQL.Database();

  db.run('CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)');
  const put = db.prepare('INSERT OR REPLACE INTO kv(key, value) VALUES(?, ?)');
  const saveJson = (key, value) => put.run([key, JSON.stringify(value)]);

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

  await fs.mkdir(path.dirname(outPath), { recursive: true });
  await fs.writeFile(outPath, Buffer.from(bytes));
}

async function main() {
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const repoRoot = path.resolve(__dirname, '..');

  for (const seed of SEEDS) {
    const url = buildCsvUrl(seed);
    process.stdout.write(`Fetching ${seed.code}... `);
    const csvText = await fetchCsvText(url);
    process.stdout.write('parsing... ');
    const state = await buildStateFromCsvText(csvText);
    process.stdout.write('writing sqlite... ');
    const outPath = path.join(repoRoot, 'public', 'seed', `${seed.code}.sqlite`);
    await writeSqlite({ outPath, state });
    process.stdout.write('done\n');
  }
}

await main();
