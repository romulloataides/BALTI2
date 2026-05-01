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
- `supabase/migrations/001_initial_schema.sql` defines the live prototype backend for community reports, votes, and inline annotations.

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
- `le` from the official CDC `U.S. Life Expectancy by State and Sex, 2018-2022` CSV for Maryland, plus official United States life-table totals for the federal layer in `2016`, `2017`, and `2023`
- `la` from the official CDC Childhood Blood Lead Surveillance workbook for Maryland in `2017` through `2022`, plus official Maryland annual-report backfills for `2016` and `2023` derived from statewide counts of children age `0-72 months` with `BLL >=5 µg/dL`

Current clearly labeled benchmark proxies:

- federal `la` uses a cycle-level NHANES proxy of `2.5%`, representing the share of U.S. children ages `1-5` above the CDC blood lead reference value. This is inferred from CDC's definition of the BLRV as the `97.5th` percentile of the `2015-2018` NHANES distribution, so it is not an annual surveillance rate and is not directly method-matched to Maryland's tested-children series.

Current benchmark gaps that still depend on the old scaffold until more sources are connected:

- `pv`, `hs`, and `va` are implemented in the pipeline through ACS 5-year series, but they only activate when `CENSUS_API_KEY` is available
- `le` still falls back for Maryland `2016`, `2017`, and `2023`, because the official CDC state-life table feed currently covers only `2018` through `2022`
- benchmark `hi` is derived only when enough real benchmark components are present; otherwise it falls back

## Deployment

GitHub Pages was previously enabled on the predecessor repository and can be re-enabled here once BALTI2 is ready to publish.

Published files:

- `index.html`
- `data.json`
- `csa_boundaries.geojson`

Because `index.html` now fetches `data.json` and `csa_boundaries.geojson` at runtime, future data updates can ship without rebuilding the HTML shell.

## Live prototype backend

The Supabase-backed prototype now supports five live community-data layers:

- `reports` for community-submitted items
- `votes` for confirm / dispute validation
- `annotations` for inline human context attached to metrics, gap flags, and reports
- `pilot_accuracy_votes` for persistent `Yes / No / Not sure` responses on the Carrollton pilot cards
- `analysis_sessions` / `analysis_messages` for the admin analysis desk

The annotation UI remains intentionally separate from official data. It stores the context target in the `annotations.metric` field using namespaced keys such as `metric:hi`, `gap:la`, and `report:BLT-1234ABCD`.

Opening the dashboard with `?admin=1` still reveals the moderation helper, but it now also expects a real Supabase-authenticated admin session. The browser does not grant admin power from the URL alone anymore:

- the user must sign in through Supabase Auth
- the email must be allowlisted in `public.admin_users`
- the live analysis desk then talks to the `analysis-desk` Edge Function instead of the old deterministic local template engine

The new Phase 6 / 7 migration is [`supabase/migrations/002_ai_admin_pilot.sql`](./supabase/migrations/002_ai_admin_pilot.sql). It extends the original schema with:

- admin allowlisting
- analysis prompt profiles
- persisted analysis sessions and messages
- `spending_events` as the first real backend hook for efficacy work
- `pilot_accuracy_votes` plus the `pilot_accuracy_vote_counts` view
- richer `reports` columns (`source`, `pilot_slug`, `block_label`, `observed_on`, `metadata`)

If the Phase 7 objects are not live yet, the dashboard now falls back cleanly: pilot accuracy cards keep the current browser choice locally and explain that shared vote sync is still pending the latest Supabase migration.

## Phase 6 setup

To activate the live admin assistant after pulling this repo:

1. Run [`supabase/migrations/001_initial_schema.sql`](./supabase/migrations/001_initial_schema.sql).
2. Run [`supabase/migrations/002_ai_admin_pilot.sql`](./supabase/migrations/002_ai_admin_pilot.sql).
3. Add an allowlisted admin row, for example:

```sql
insert into public.admin_users (email, role)
values ('you@example.org', 'admin')
on conflict do nothing;
```

4. Set Supabase Edge Function secrets:
   - `OPENAI_API_KEY`
   - optional `OPENAI_MODEL`
   - optional `DASHBOARD_DATA_URL`
5. Deploy [`supabase/functions/analysis-desk/index.ts`](./supabase/functions/analysis-desk/index.ts).
6. In Supabase Auth settings, allow your dashboard URL as a redirect target for magic-link sign-in.
7. Flip `window.BALTI2_ENABLE_ANALYSIS_DESK = true` in [`supabase/config.js`](./supabase/config.js) after the function is live.

## Phase 3, 6, and 7 prototype coverage

The current repo now covers the next three product phases with a mixed live/prototype split:

- **Phase 3 — Gap View**
  - Gap mode now includes an **Efficacy watch** section.
  - The efficacy layer is clearly labeled as a proxy built from repeat resident signal plus current burden, not linked city spending data yet.
  - Blind-spot filtering and gap-severity map mode remain URL-shareable.

- **Phase 6 — Admin analysis desk**
  - Opening the dashboard with `?admin=1` now reveals the moderation helper **and** a live **Analysis desk** drawer.
  - The desk is no longer deterministic in the repo code. It is wired for a real model-backed Supabase Edge Function with tool access to:
    - neighborhood data from `data.json`
    - live community reports
    - 311 proxy history
    - spending records
    - pilot accuracy votes
  - Proper admin access now depends on Supabase Auth plus the `admin_users` allowlist.
  - The remaining manual step is deployment: until the `analysis-desk` function and secrets are live, the UI will show a clear setup error instead of silently faking answers.

- **Phase 7 — Carrollton Ridge pilot**
  - Opening the dashboard with `?pilot=carrollton` locks the prototype to `Southwest Baltimore`, which is the current CSA stand-in for `Carrollton Ridge / Franklin Square`.
  - Pilot mode foregrounds two focal issues:
    - illegal dumping, using the current `311 hazards` and sanitation-signal proxy
    - broadband access, using reports and annotations that mention connectivity-related terms
  - Each pilot card now keeps the current user's selection in the browser and will persist shared vote counts through `pilot_accuracy_votes` once the Phase 7 migration is live.
  - If that migration has not been applied yet, the UI now says so explicitly instead of throwing backend errors.
  - The public demo can keep `window.BALTI2_ENABLE_PILOT_VOTES = false` in [`supabase/config.js`](./supabase/config.js) until the migration is live, which prevents noisy 404s while preserving the local demo selection.
  - [`submit/index.html`](./submit/index.html) is now a richer field-intake flow with:
    - an explicit queued outbox
    - optional GPS capture
    - richer pilot metadata in `reports.metadata`
    - legacy-schema fallback while the new migration is being rolled out
    - clearer pilot-vs-CSA copy, so the form shows `Carrollton Ridge / Franklin Square` while staying explicit that official dashboard metrics are still anchored to the `Southwest Baltimore` CSA
  - [`submit/sw.js`](./submit/sw.js) still caches the flow after first load, but the page itself now exposes queue state instead of treating offline drafts as a hidden implementation detail.
  - Physical field-testing is still a human step. The repo now supports it better; it does not replace it.

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
- Add a defensible federal `la` source and newer official `le` years if you want the benchmark switcher to be fully non-scaffolded.
- Fix or replace the current 311 ingest in `update_data.R` if the ArcGIS service becomes unstable again.
- Run the new Supabase migration and deploy the `analysis-desk` Edge Function so Phase 6 becomes live end to end.
- Add real spending / work-order records to `spending_events` so efficacy queries stop saying "no spending data loaded yet."
- Replace the Carrollton-to-`Southwest Baltimore` proxy if BNIA or another defensible CSA mapping source becomes available.
- Finish human mobile field-testing with Digital Navigators and adjust the pilot intake copy based on what they actually need in the street.

## Notes

- Share links use the current page URL instead of a hardcoded placeholder domain.
- The dashboard still depends on CDN assets for Leaflet and Chart.js.
- Most yearly trends are still modeled only when a real `2016`–`2023` array is unavailable after the BNIA, CDC, ACS, and 311 refresh steps run.
