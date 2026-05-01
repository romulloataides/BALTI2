# Baltimore Dashboard

This folder is prepared for static hosting on GitHub Pages, Netlify, or Vercel.

## Current architecture

- `index.html` contains the UI and fetches `data.json` and `csa_boundaries.geojson` at runtime.
- `data.json` is the live dashboard data source used by the deployed site.
- `csa_boundaries.geojson` contains the 55 Baltimore CSA polygons used by the map.
- `update_data.R` updates `data.json`, so pipeline runs now affect the deployed dashboard without rewriting `index.html`.

## Publish on GitHub Pages

1. Create a new public GitHub repository, for example `baltimore-dashboard`.
2. Upload `index.html` from this folder to the root of the repo.
3. In GitHub, open `Settings` -> `Pages`.
4. Set `Source` to `Deploy from a branch`.
5. Select the `main` branch and the `/ (root)` folder.
6. Save and wait for the site to build.

Your site URL will look like:

`https://YOUR_GITHUB_USERNAME.github.io/baltimore-dashboard/`

## Notes

- Share links use the current page URL instead of a hardcoded placeholder domain.
- The dashboard still depends on CDN assets for Leaflet and Chart.js.
- Most yearly trends are still modeled from snapshot values unless a metric is provided as a real 2016–2023 array in `data.json`.
