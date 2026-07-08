# City Health Dashboard Source Notes

Checked 2026-07-07.

## What CHDB Provides

- CHDB data can be accessed through downloads or API. The documented API endpoints include `metrics`, `metric-data`, geographic identifiers, data sources, and demographic identifiers. API calls use a `token` query parameter, and large result sets paginate with `page`.
  Source: https://www.cityhealthdashboard.com/data-access

- CHDB explicitly points users to its Data Dictionary and Technical Document before analysis. The Technical Document page says the PDF explains data sources, sub-tables, variables, formulas, and analytic decisions for the metrics.
  Sources: https://www.cityhealthdashboard.com/data-access and https://www.cityhealthdashboard.com/technical-documentation

- CHDB describes its metric set as 45+ measures across Health Outcomes, Social and Economic Factors, Health Behavior, Physical Environment, and Clinical Care. It says data come from federal, state, and other datasets.
  Source: https://www.cityhealthdashboard.com/metrics

## API Keys To Get

1. `CHDB_API_KEY`: needed to reproduce the linked CHDB city overview and fetch `metric-data/{id}`.
2. `CENSUS_API_KEY`: already used by BALTI2 for ACS tract/place/county/state/national pulls.
3. Optional Socrata app token: useful for CDC PLACES and Open Baltimore reliability at higher volume, but not required for the current public pulls.
4. Optional BLS key: useful if monthly/current unemployment is pulled directly from BLS instead of CHDB metric 42.

## Metric 42

CHDB metric 42 is `Unemployment - Current, City-Level`: percentage of population age 16+ unemployed but seeking work, by month. It reproduces a city overview metric, not a CSA/neighborhood choropleth. BALTI2 should map CHDB-style neighborhood unemployment with the annual neighborhood concept, metric 41, using the existing `un` CSA series.

## BALTI2 Wiring Rule

Use CHDB for city-level overview/benchmark reproduction. Use BALTI2's existing neighborhood pipeline for CSA maps. Only add a CHDB metric to the map when the crosswalk status is `mapped_now` or a real tract-to-CSA/source implementation has been added.

## Current App Wiring

- `index.html` now renders a `Baltimore overview` side-panel state when no CSA is pinned. It summarizes city/benchmark context from `data.json`, shows key city cards, and ranks the current map metric by CSA.
- The city profile mirrors CHDB's Baltimore overview fields: population, region, city type, and local CSA geography.
- The map toolbar exposes 23 top-level map metrics. Rat, dumping, and water/sewer call fields stay inside the 311 hazards logic instead of appearing as separate metric tabs.
- Staged fallback fields are labeled as staged in the UI provenance chips. They should not be described as fully source-refreshed until a matched import is added.
- The overview now includes "Next-step modules" rather than a loose roadmap: CIP/spending seed ready (1,254 CSA allocations across 55 CSAs; about $2.38B if source amount fields are read as $000s), redlining layer ready, live 311 layer wired, CHDB peer-city comparisons gated on `CHDB_API_KEY`, and tract demographics queued for a separate ACS cache.
