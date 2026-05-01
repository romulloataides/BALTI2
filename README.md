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
- `benchmarks.state` and `benchmarks.federal` now use real official series where available, and fall back to the old scaffold only for the remaining gaps.
- Real year arrays now come from a mix of BNIA files, BNIA ArcGIS services, CDC tract datasets, and the existing baseline `data.json`.
- The dashboard `hi` series is derived from official component metrics when BNIA does not provide a direct health-index series.
- Remaining gaps are still modeled in the data layer instead of in the frontend render path.

## BNIA longitudinal input

`update_data.R` now looks for a real BNIA Vital Signs file before it falls back to the existing modeled series. If no local BNIA file is present, it also tries to enrich the dashboard from BNIA's live ArcGIS indicator services.

Supported file locations:

- `BNIA_VITAL_SIGNS_PATH` environment variable
- `bnia_vital_signs.csv`
- `BNIA_Vital_Signs.csv`
- `vital_signs.csv`
- `data_sources/bnia_vital_signs.csv`

Spreadsheet inputs (`.xlsx` / `.xls`) are also supported when `readxl` is installed.

Supported table shapes:

1. `CSA` + `Year` + one or more metric columns such as `health index`, `life expectancy`, `poverty rate`
2. `CSA` + `Year` + `Indicator` + `Value`
3. `CSA` + `Indicator` + year columns `2016` through `2023`

The importer maps common indicator names onto dashboard metric keys such as `hi`, `le`, `as`, `la`, `va`, `pv`, `un`, `hs`, `fd`, `gs`, `cr`, `rt`, `dp`, `ws`, and `hz`.
If no BNIA file is found, the pipeline keeps using the current `data.json` values and only models the missing yearly series.

Live BNIA service fallback currently supplements:

- `le` from the `Lifexp` ArcGIS service
- `la` from the `Ebll` ArcGIS service
- `va` from the `Vacant` ArcGIS service
- `un` from the `Unempr` ArcGIS service
- `hs` as `100 - Lesshs` from the `Lesshs` ArcGIS service

Official non-BNIA supplements currently fill the remaining health gaps:

- `as` from CDC `500 Cities` / `PLACES` tract releases for `2016` through `2023`, aggregated to CSA using tract centroid assignment and population weighting
- `hi` derived from the official component metrics `le`, `as`, `la`, `va`, `pv`, `un`, and `hs` using yearly min-max normalization and equal weighting when no direct BNIA `hi` series is available

These sources are helpful, but they are not perfect substitutes for a current full BNIA export. The CDC asthma series is an adult current-asthma prevalence proxy, not the original asthma emergency-department rate. The pipeline still keeps existing `data.json` values and modeled series for any remaining gaps.

## State and federal benchmarks

The benchmark pipeline now has three tiers:

- `city` is derived directly from the Baltimore CSA dashboard values
- `state` uses Maryland-wide official series where available
- `federal` uses national official series where available

Current real benchmark coverage:

- `un` from FRED / BLS (`MDUR` for Maryland and `UNRATE` for the United States)
- `as` from CDC `PLACES` tract releases for `2018` through `2023`, aggregated with population weighting

Current benchmark gaps that still depend on the old scaffold until more sources are connected:

- `pv`, `hs`, and `va` are implemented in the pipeline through ACS 5-year series, but they only activate when `CENSUS_API_KEY` is available
- `le` and `la` do not yet have a clean statewide/national benchmark source in this repo
- benchmark `hi` is derived only when enough real benchmark components are present; otherwise it falls back

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

- Drop the real BNIA Vital Signs longitudinal file into one of the supported locations so `update_data.R` can replace more of the modeled neighborhood series than the live service fallback can cover.
- Add `CENSUS_API_KEY` to GitHub Actions secrets so `update_data.R` can run end to end.
- Once `CENSUS_API_KEY` is set, rerun the pipeline so ACS-backed `pv`, `hs`, and `va` state/federal benchmarks replace more of the scaffold.
- Replace the CDC asthma proxy with a direct neighborhood-level official asthma ED source if BNIA or Maryland publishes a current one again.
- Add direct official state/federal sources for `le` and `la` if you want the benchmark switcher to be fully non-scaffolded.
- Fix or replace the current 311 ingest in `update_data.R` if the ArcGIS service becomes unstable again.
- Add a real backend for community reports if submissions need persistence.

## Notes

- Share links use the current page URL instead of a hardcoded placeholder domain.
- The dashboard still depends on CDN assets for Leaflet and Chart.js.
- Most yearly trends are still modeled only when a real `2016`–`2023` array is unavailable after the BNIA, CDC, ACS, and 311 refresh steps run.
