# CBC Extract Data (Web)

Small Vite web UI for parsing the Audubon CBC **Historical Results By Count** spreadsheet export and showing the extracted tables.

## Run

```bash
cd web
npm install
npm run dev
```

Then open the local URL Vite prints.

## Usage

1. Go to **Audubon CBC Count Results**:
   https://netapp.audubon.org/CBCObservation/Historical/ResultsByCount.aspx
2. Download the spreadsheet export.
3. Drag/drop the `.xls`/`.xlsx` into the left sidebar.
4. View the extracted tables under tabs:
   - Species
   - Weather
   - Effort
   - Participation

The plot panel shows a simple Plotly chart (for species-by-year series).

## Notes

- `.xlsx` parsing uses a lightweight ZIP/XML reader (no SheetJS).
- `.xls` parsing runs in a Web Worker and is limited to **2MB** per file.
- `npm audit` may report a high-severity issue in the `xlsx` dependency used for legacy `.xls` support; there is currently no upstream fix. The app mitigates this with worker isolation + strict size limits.
