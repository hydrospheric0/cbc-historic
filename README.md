# CBC Historic

## About the Christmas Bird Count
The Christmas Bird Count is the nation’s longest-running community science bird project. It occurs December 14 to January 5 every season in over 3000 count circles. More information can be found on the [Audubon website](https://www.audubon.org/community-science/christmas-bird-count).

## About the tool
This tool was developed to help count circle compilers and other interested parties by:
- Parsing the Audubon CBC “Historical Results By Count” CSV export into clean tables
- Providing quick visualization of historical count data
- Allowing export of normalized CSV tables

The app is hosted at: https://hydrospheric0.github.io/cbc-historic/

## Features
- Upload / drag-and-drop Audubon CBC “Historical Results By Count” CSV
- Count search (name/code) with links to download the correct export
- Map view (Esri Topo default with layer selection)
- Tabbed tables (Species / Weather / Effort / Participation)
- Plot pane for quick trend visualization

## How to use
You can either:
- Download the “Historical Results By Count” CSV for your count circle from the Audubon portal, then drop it into the app, or
- (Optional) Configure the Cloudflare Worker proxy and use the in-app update/download buttons to fetch and ingest a circle directly.

Once ingested, tables and plots populate automatically, and you can export normalized CSV tables.

## Optional: Cloudflare Worker CSV proxy
This repo includes a minimal Worker in [cloudflare-worker/README.md](cloudflare-worker/README.md) that:
- Proxies a single circle’s Audubon CSV export
- Adds CORS so the GitHub Pages frontend can fetch it
- Caches responses at the edge

Frontend configuration:
- Set `VITE_CBC_WORKER_BASE` to your Worker base URL (no trailing slash)

Example:
- `VITE_CBC_WORKER_BASE=https://cbc-historic-cbc-proxy.example.workers.dev`

## GitHub Pages
This repo is configured to deploy to GitHub Pages via Actions.

- Workflow: `.github/workflows/deploy-pages.yml`
- Vite `base` is set for project pages at `https://hydrospheric0.github.io/cbc-historic/`

In GitHub repo settings, ensure **Pages** is set to **GitHub Actions** as the source.

### Enable on-demand circle fetch on GitHub Pages
Because GitHub Pages serves a static build, the Worker base URL must be provided at build time.

1. Deploy the Worker (see [cloudflare-worker/README.md](cloudflare-worker/README.md)) and copy its base URL.
2. In your GitHub repo settings, add one of:
	- **Settings → Secrets and variables → Actions → Variables**: `VITE_CBC_WORKER_BASE`, or
	- **Settings → Secrets and variables → Actions → Secrets**: `VITE_CBC_WORKER_BASE`

Example value (no trailing slash):
- `https://cbc-historic-cbc-proxy.example.workers.dev`

The workflow [ .github/workflows/deploy-pages.yml ](.github/workflows/deploy-pages.yml) injects this into `npm run build`.

## Run locally

```bash
npm install
npm run dev
```

If you want to use the Worker locally:

```bash
npm --prefix cloudflare-worker install
npm --prefix cloudflare-worker run dev
```

Then run the frontend with:
- `VITE_CBC_WORKER_BASE=http://127.0.0.1:8787`

### Support this project
If you find this tool useful, please consider supporting its development:

<a href="https://buymeacoffee.com/bartg">
	<img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me a Coffee" width="180" />
</a>
