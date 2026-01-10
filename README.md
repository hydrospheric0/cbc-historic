# CBC Historic

[![language](https://img.shields.io/github/languages/top/hydrospheric0/cbc-historic?label=language)](https://github.com/hydrospheric0/cbc-historic)
[![languages](https://img.shields.io/github/languages/count/hydrospheric0/cbc-historic?label=languages)](https://github.com/hydrospheric0/cbc-historic)

![Top languages](https://github-readme-stats.vercel.app/api/top-langs/?username=hydrospheric0&repo=cbc-historic&layout=compact)

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
- Map view (OpenStreetMap basemap)
- Tabbed tables (Species / Weather / Effort / Participation)
- Plot pane for quick trend visualization

## How to use
Download the “Historical Results By Count” CSV for your count circle from the Audubon portal, then drop it into the app. Once ingested, tables and plots populate automatically, and you can export the parsed tables back out as CSV.

## GitHub Pages
This repo is configured to deploy to GitHub Pages via Actions.

- Workflow: `.github/workflows/deploy-pages.yml`
- Vite `base` is set for project pages at `https://hydrospheric0.github.io/cbc-historic/`

In GitHub repo settings, ensure **Pages** is set to **GitHub Actions** as the source.

## Run locally

```bash
npm install
npm run dev
```

### Support this project

<a href="https://buymeacoffee.com/bartg">
	<img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me a Coffee" width="180" />
</a>
