# BALTI2

BALTI2 is the new home for the Baltimore neighborhood health dashboard.

Previous GitHub Pages site: `https://romulloataides.github.io/Baltimore-Dashboard/`

GitHub repo: `https://github.com/romulloataides/BALTI2`

## Current architecture

- `index.html` contains the UI and fetches `data.json` and `csa_boundaries.geojson` at runtime.
- `data.json` is the live dashboard data source used by the deployed site.
- `csa_boundaries.geojson` contains the 55 Baltimore CSA polygons used by the map.
- `update_data.R` updates `data.json`, so pipeline runs now affect the deployed dashboard without rewriting `index.html`.
- `scripts/migrate_data_schema.mjs` upgrades the legacy flat JSON shape into the normalized longitudinal schema used by Phase 2.

## Data schema

`data.json` now uses a normalized structure:

```json
{
  "meta": {
    "schema_version": 2,
    "years": [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]
  },
  "neighborhoods": {
    "Canton": {
      "2023": { "hi": 79, "le": 74, "va": 4.1 }
    }
  },
  "benchmarks": {
    "city": {
      "2023": { "hi": 58, "le": 69, "va": 10.1 }
    },
    "state": {},
    "federal": {}
  }
}
```

Notes:

- `neighborhoods` stores year-specific records for each CSA.
- `benchmarks.city` is shared across all neighborhoods and is derived from the Baltimore CSA dataset.
- `benchmarks.state` and `benchmarks.federal` are currently provisional scaffolds until real ACS/FRED benchmark imports are wired into the pipeline.
- The current longitudinal values are still modeled where real year arrays are not yet available. That modeling now lives in the data layer instead of the frontend render path.

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

## Migration

If you still have a legacy flat `data.json`, run:

```bash
node scripts/migrate_data_schema.mjs data.json
```

## Next Build Steps

- Replace the modeled longitudinal values in `data.json` with real BNIA time-series data.
- Add `CENSUS_API_KEY` to GitHub Actions secrets so `update_data.R` can run end to end.
- Add real state and federal benchmarks through ACS/FRED inputs instead of the current provisional scaffolds.
- Fix or replace the current 311 ingest in `update_data.R`.
- Add a real backend for community reports if submissions need persistence.

## Notes

- Share links use the current page URL instead of a hardcoded placeholder domain.
- The dashboard still depends on CDN assets for Leaflet and Chart.js.
- Most yearly trends are still modeled from snapshot values unless a metric is provided as a real 2016–2023 array in `data.json`.
