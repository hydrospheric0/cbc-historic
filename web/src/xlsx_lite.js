import { unzipSync, strFromU8 } from 'fflate';

function parseXml(xmlText) {
  const doc = new DOMParser().parseFromString(xmlText, 'application/xml');
  const err = doc.getElementsByTagName('parsererror')[0];
  if (err) {
    throw new Error('Failed to parse XLSX XML.');
  }
  return doc;
}

function getAttrAny(el, names) {
  for (const n of names) {
    const v = el.getAttribute(n);
    if (v !== null && v !== undefined) return v;
  }
  return null;
}

function colLettersToIndex(letters) {
  let n = 0;
  for (let i = 0; i < letters.length; i++) {
    const code = letters.charCodeAt(i);
    if (code < 65 || code > 90) return null;
    n = n * 26 + (code - 64);
  }
  return n - 1;
}

function parseCellRef(a1) {
  const m = /^([A-Z]+)(\d+)$/.exec(String(a1 || '').toUpperCase());
  if (!m) return null;
  const col = colLettersToIndex(m[1]);
  const row = parseInt(m[2], 10) - 1;
  if (!Number.isFinite(col) || !Number.isFinite(row) || col < 0 || row < 0) return null;
  return { row, col };
}

function parseSharedStrings(files) {
  const ss = files['xl/sharedStrings.xml'];
  if (!ss) return [];
  const doc = parseXml(strFromU8(ss));
  const sis = Array.from(doc.getElementsByTagName('si'));
  return sis.map((si) => {
    const ts = Array.from(si.getElementsByTagName('t'));
    return ts.map((t) => t.textContent || '').join('');
  });
}

function resolveFirstSheetPath(files) {
  const wb = files['xl/workbook.xml'];
  const rels = files['xl/_rels/workbook.xml.rels'];

  if (wb && rels) {
    const wbDoc = parseXml(strFromU8(wb));
    const sheets = Array.from(wbDoc.getElementsByTagName('sheet'));
    const first = sheets[0];
    if (first) {
      const rid =
        getAttrAny(first, ['r:id', 'id']) ||
        first.getAttributeNS('http://schemas.openxmlformats.org/officeDocument/2006/relationships', 'id');
      if (rid) {
        const relDoc = parseXml(strFromU8(rels));
        const relEls = Array.from(relDoc.getElementsByTagName('Relationship'));
        const rel = relEls.find((r) => (r.getAttribute('Id') || '') === rid);
        const target = rel ? rel.getAttribute('Target') : null;
        if (target) {
          const t = target.replace(/^\//, '');
          return `xl/${t}`;
        }
      }
    }
  }

  // Fallback: pick sheet1.xml or first worksheet file.
  if (files['xl/worksheets/sheet1.xml']) return 'xl/worksheets/sheet1.xml';
  const sheetKeys = Object.keys(files)
    .filter((k) => /^xl\/worksheets\/sheet\d+\.xml$/.test(k))
    .sort();
  if (sheetKeys.length) return sheetKeys[0];
  throw new Error('Could not locate a worksheet in the XLSX file.');
}

function parseCellValue(cellEl, sharedStrings) {
  const t = cellEl.getAttribute('t') || '';

  if (t === 'inlineStr') {
    const is = cellEl.getElementsByTagName('is')[0];
    if (!is) return '';
    const ts = Array.from(is.getElementsByTagName('t'));
    return ts.map((x) => x.textContent || '').join('');
  }

  const vEl = cellEl.getElementsByTagName('v')[0];
  const vText = vEl ? vEl.textContent || '' : '';

  if (t === 's') {
    const idx = parseInt(vText, 10);
    return Number.isFinite(idx) && sharedStrings[idx] !== undefined ? sharedStrings[idx] : '';
  }
  if (t === 'b') {
    return vText === '1';
  }
  if (t === 'str') {
    return vText;
  }
  if (t === 'e') {
    return '';
  }

  // Default: number if it looks like one, else string.
  if (/^\s*-?\d+(?:\.\d+)?\s*$/.test(vText)) {
    const num = Number(vText);
    return Number.isFinite(num) ? num : vText;
  }

  return vText;
}

export function parseXlsxFirstSheetToGrid(arrayBuffer) {
  const u8 = new Uint8Array(arrayBuffer);
  const files = unzipSync(u8);

  const sharedStrings = parseSharedStrings(files);
  const sheetPath = resolveFirstSheetPath(files);
  const sheet = files[sheetPath];
  if (!sheet) throw new Error('Worksheet XML not found in XLSX.');

  const doc = parseXml(strFromU8(sheet));
  const rows = Array.from(doc.getElementsByTagName('row'));

  let maxRow = -1;
  const rowArrays = new Map();

  for (const rowEl of rows) {
    const cells = Array.from(rowEl.getElementsByTagName('c'));
    for (const c of cells) {
      const r = c.getAttribute('r');
      const ref = parseCellRef(r);
      if (!ref) continue;
      maxRow = Math.max(maxRow, ref.row);
      let rowArr = rowArrays.get(ref.row);
      if (!rowArr) {
        rowArr = [];
        rowArrays.set(ref.row, rowArr);
      }
      if (rowArr.length <= ref.col) rowArr.length = ref.col + 1;
      rowArr[ref.col] = parseCellValue(c, sharedStrings);
    }
  }

  const grid = [];
  const totalRows = Math.max(maxRow + 1, rowArrays.size ? Math.max(...rowArrays.keys()) + 1 : 0);
  for (let r = 0; r < totalRows; r++) {
    grid[r] = rowArrays.get(r) || [];
  }
  return grid;
}
