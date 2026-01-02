import * as XLSX from 'xlsx';
const MAX_XLS_BYTES = 2 * 1024 * 1024;

/**
 * Parses a legacy .xls workbook inside a Worker.
 *
 * Notes:
 * - This is isolation, not a security guarantee.
 * - Enforces a strict file size cap before doing work.
 */
self.onmessage = async (e) => {
  try {
    const { arrayBuffer, preferredSheetName } = e.data || {};

    if (!(arrayBuffer instanceof ArrayBuffer)) {
      throw new Error('Invalid worker input (expected ArrayBuffer).');
    }
    if (arrayBuffer.byteLength > MAX_XLS_BYTES) {
      throw new Error('File too large. Max .xls size is 2MB.');
    }

    const wb = XLSX.read(arrayBuffer, {
      type: 'array',
      cellDates: true,
      cellNF: false,
      cellText: true,
    });

    const sheetName =
      (preferredSheetName && wb.SheetNames?.includes(preferredSheetName) && preferredSheetName) ||
      (wb.SheetNames?.includes('HistoricalResultsByCount') && 'HistoricalResultsByCount') ||
      wb.SheetNames?.[0];

    if (!sheetName) throw new Error('No sheets found in .xls file.');

    const ws = wb.Sheets?.[sheetName];
    if (!ws) throw new Error('Could not load worksheet in .xls file.');

    const grid = XLSX.utils.sheet_to_json(ws, { header: 1, raw: true, defval: '' });

    // Reasonable post-parse limits to prevent UI blowups.
    const MAX_ROWS = 20000;
    const MAX_COLS = 800;
    if (grid.length > MAX_ROWS) {
      throw new Error(`Worksheet too large (${grid.length} rows).`);
    }
    for (let r = 0; r < grid.length; r++) {
      const row = grid[r];
      if (Array.isArray(row) && row.length > MAX_COLS) {
        throw new Error(`Worksheet too wide (row ${r + 1} has ${row.length} columns).`);
      }
    }

    self.postMessage({ ok: true, sheetName, grid });
  } catch (err) {
    const msg = err && typeof err === 'object' && 'message' in err ? String(err.message) : String(err);
    const stack = err && typeof err === 'object' && 'stack' in err ? String(err.stack) : '';
    self.postMessage({ ok: false, error: stack ? `${msg}\n${stack}` : msg || 'Failed to parse .xls file.' });
  }
};
