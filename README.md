# BALTI2

BALTI2 is the new home for the Baltimore neighborhood health dashboard.

Previous GitHub Pages site: `https://romulloataides.github.io/Baltimore-Dashboard/`

GitHub repo: `https://github.com/romulloataides/BALTI2`

## Current architecture

- `index.html` contains the UI and fetches `data.json` and `csa_boundaries.geojson` at runtime.
- `data.json` is the live dashboard data source used by the deployed site.
- `csa_boundaries.geojson` contains the 55 Baltimore CSA polygons used by the map.
- `update_data.R` updates `data.json`, so pipeline runs now affect the deployed dashboard without rewriting `index.html`.

## Deployment

GitHub Pages was previously enabled on the predecessor repository and can be re-enabled here once BALTI2 is ready to publish.

Published files:

- `index.html`
- `data.json`
- `csa_boundaries.geojson`

Because `index.html` now fetches `data.json` and `csa_boundaries.geojson` at runtime, future data updates can ship without rebuilding the HTML shell.

## GitHub Pages Setup Reference

1. Create a public GitHub repository, for example `BALTI2`.
2. Upload `index.html` from this folder to the root of the repo.
3. In GitHub, open `Settings` -> `Pages`.
4. Set `Source` to `Deploy from a branch`.
5. Select the `main` branch and the `/ (root)` folder.
6. Save and wait for the site to build.

That kind of setup produces a site URL like:

`https://YOUR_GITHUB_USERNAME.github.io/BALTI2/`

## Next Build Steps

- Replace the modeled values in `data.json` with real BNIA time-series data.
- Add `CENSUS_API_KEY` to GitHub Actions secrets so `update_data.R` can run end to end.
- Fix or replace the current 311 ingest in `update_data.R`.
- Add a real backend for community reports if submissions need persistence.

## Notes

- Share links use the current page URL instead of a hardcoded placeholder domain.
- The dashboard still depends on CDN assets for Leaflet and Chart.js.
- Most yearly trends are still modeled from snapshot values unless a metric is provided as a real 2016–2023 array in `data.json`.
