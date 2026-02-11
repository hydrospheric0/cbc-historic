# CBC Historic

## About the Christmas Bird Count
The Christmas Bird Count is the nation’s longest-running community science bird project. <br>
It occurs annually between December 14 and January 5 in over 3000 count circles. <br>
More information can be found on the [Audubon website](https://www.audubon.org/community-science/christmas-bird-count).

## About the tool
This tool was developed to help count circle compilers and other interested parties by:
- Fetching Audubon CBC “Historical Results By Count” data on demand (per circle)
- Parsing it into clean tables and plots directly in the browser
- Persisting a local, offline-ready cache in IndexedDB/sql.js
- Allowing export of normalized CSV tables

The app is hosted at: https://hydrospheric0.github.io/cbc-historic/

## Features
- Map with all circles; click a circle to load data
- On-demand CSV fetch via Cloudflare Worker proxy + local caching
- Search by circle name/code
- Tabbed tables (Species / Weather / Effort / Participation)
- Plot pane for quick trend visualization
- Export normalized CSV tables

## How to use
1. Click a circle on the map (or search by name/code).
2. The app downloads that circle’s data via the proxy and stores it locally.
3. Tables and plots populate automatically; you can export normalized CSV tables.
4. Use the ↻ Update button in Available data to refresh a circle.

### Support this project
If you find this tool useful, please consider supporting its development:

<a href="https://buymeacoffee.com/bartg">
	<img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me a Coffee" width="180" />
</a>
