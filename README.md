# cbc_extract_data

This repo contains two tools for working with Audubon Christmas Bird Count (CBC) **Historical Results By Count** spreadsheet exports.

## Web app (recommended)

The web UI lives in [web/](web/).

```bash
cd web
npm install
npm run dev
```

Then open the local URL Vite prints.

Notes:
- CSV-only ingestion.

## Python script

[extract_historical_results_by_count.py](extract_historical_results_by_count.py) extracts cleaned CSV tables from a CBC export `.xls`.

Dependencies:
- `pandas`
- `xlrd`

Example:

```bash
python extract_historical_results_by_count.py --input "HistoricalResultsByCount [CAPC-1972-2025].xls"
```

Outputs default under `source/cbc_website/...` (see script args); you can override with `--counts-output`, `--participants-output`, etc.
