# Baltimore Neighborhood Health Dashboard — Data Audit
## Project Trace / Project Compass
**Audit date:** 2026-04-30  
**Scope:** All candidate data sources for the BALTI2 dashboard MVP and future phases

---

## 1. Executive Summary

This audit covers nine public data sources that together can power the full indicator set for the Baltimore Neighborhood Health Dashboard. Sources are grouped into three priority tiers based on data availability, geographic fit, and implementation effort.

### Priority Table

| Priority | Source | Rationale |
|----------|--------|-----------|
| **1 — MVP Core** | BNIA Vital Signs | Primary neighborhood-level health, social, and environmental data for all 55 CSAs; direct CSA match |
| **1 — MVP Core** | Open Baltimore 311 | Operational request data with lat/lng for point-in-polygon CSA enrichment; hazard/311 tab |
| **1 — MVP Core** | ACS 5-Year Estimates | Authoritative poverty, income, employment, education, housing burden at tract and city level |
| **1 — MVP Core** | City Health Dashboard | City/state/national benchmark values for chronic disease and social determinants |
| **2 — Phase 2** | CDC PLACES | Census-tract-level modeled estimates for chronic conditions; useful benchmark once CSA aggregation is validated |
| **2 — Phase 2** | EPA EJScreen | Environmental context layer (PM2.5, diesel PM, lead paint, traffic proximity) at block group |
| **2 — Phase 2** | Baltimore Area Survey | Resident perception data on safety and neighborhood quality; requires multi-wave pooling for CSA estimates |
| **3 — Context/Optional** | BLS | City/state/national unemployment time series for benchmark comparisons |
| **3 — Context/Optional** | FRED | Alternative unemployment and economic series; redundant if ACS and BLS are in use |

---

## 2. Source Profiles

### 2.1 BNIA Vital Signs

**Purpose:** Primary source for neighborhood-level health, housing, economic, and educational indicators for all 55 Community Statistical Areas (CSAs) in Baltimore City.

**URL:** https://bniajfi.org/vital_signs/ and https://vital-signs-bniajfi.hub.arcgis.com/

**API method:** ArcGIS Hub open data portal. Each annual release is a downloadable CSV and GeoJSON. No programmatic REST API for time-series queries; annual downloads must be assembled manually or via direct URL patterns.

**API key required:** No

**Geographic levels:** Community Statistical Area (CSA) — 55 neighborhoods covering all of Baltimore City. No sub-CSA or census-tract breakdown.

**Years available:** 2010–2022 (Vital Signs 22 is the current release; Vital Signs 23 is in progress as of 2026)

**Update frequency:** Annual

**Candidate indicators:**
- `leXX` — Life expectancy at birth
- `astXX` — Asthma emergency department visit rate per 1,000
- `nleaXX` — Children under 6 with elevated blood lead levels (%)
- `vacXX` — Vacant housing units (%)
- `hhpovXX` — Households below poverty line (%)
- `unemXX` — Unemployment rate (%)
- `compXX` — High school completion rate (%)
- `mhiXX` — Median household income ($)
- Plus ~142 additional indicators across health, housing, economy, education, and crime

**Overlap with dashboard metrics:** Direct match for life expectancy, asthma ED rate, lead exposure, vacant housing, poverty, unemployment, HS graduation, and median household income — all core dashboard tabs.

**CSA join method:** Direct name match on the CSA/Community field. The `geoName()` helper in `index.html` already handles this. Cross-reference with `csa_boundaries.geojson` for map rendering.

**Data type:** Administrative data (vital statistics, hospital discharge records, ACS extracts, Baltimore City vacancy surveys)

**Limitations:**
- Some metrics are composite or model-based and should be labeled accordingly
- Longitudinal coverage requires manual download of one CSV per year; no single time-series API endpoint
- Vital Signs 22 (2022 data) may have a 12–18 month lag on some sub-components
- Not all 150+ indicators are available for every year; gaps exist for early years

**Recommended MVP use:** Load the most recent multi-year CSV (Vital Signs 18–22) as the primary `data.json` spine. Populate life expectancy, asthma ED, lead, vacant, poverty, unemployment, HS graduation, and median income tabs with BNIA as the authoritative source. Label source as "BNIA Vital Signs" in the legend.

**Recommended later-phase use:** Expand to the full ~150-indicator set for the Determinants tab. Use historical CSVs (2010–2017) to extend the trend slider back to 2010.

---

### 2.2 City Health Dashboard

**Purpose:** Pre-built city-level and census-tract-level health indicators with city/state/national benchmarks. Provides benchmark values for the comparison overlay without requiring custom aggregation.

**URL:** https://www.cityhealthdashboard.com

**API method:** REST API endpoint: `https://www.cityhealthdashboard.com/api/data` — returns JSON with place-level records. Baltimore City is available as a named "place." No authentication required for public data.

**API key required:** No

**Geographic levels:** City (Baltimore as a whole), some metrics available at census tract

**Years available:** Varies by metric; generally 2014–2022. Not all metrics have full annual coverage.

**Update frequency:** Periodic (approximately annual, tied to data releases from underlying sources)

**Candidate indicators:** Chronic disease prevalence, mental health status, obesity, smoking, physical inactivity, preventable hospitalizations, poverty rate, housing cost burden, unemployment rate, median household income, educational attainment

**Overlap with dashboard metrics:** Provides ready-made city/state/national benchmark values for the benchmark comparison bar in the Overview and Compare tabs. Reduces need to build benchmark aggregation from ACS from scratch.

**CSA join method:** Not applicable at CSA level. Used only for city-wide and national benchmark values passed into `BENCHMARKS.city`, `BENCHMARKS.state`, and `BENCHMARKS.federal` objects.

**Data type:** Mix of survey (BRFSS) and administrative data

**Limitations:**
- Census-tract data available only for some metrics
- Year coverage is inconsistent across indicators; some fields may be 2–3 years behind
- Does not use CSA geography; CSA estimates require separate BNIA source

**Recommended MVP use:** Query the API at build time to populate the city/state/national benchmark rows in the data pipeline. Store results in the `benchmarks` section of `data.json`.

**Recommended later-phase use:** Use tract-level data (where available) to cross-validate BNIA CSA estimates and as a secondary source for metrics not covered by BNIA.

---

### 2.3 CDC PLACES

**Purpose:** Small-area modeled estimates for chronic disease and health behaviors at census-tract and place level. Provides asthma prevalence, COPD, diabetes, heart disease, high blood pressure, obesity, smoking, and other metrics.

**URL:** https://www.cdc.gov/places/

**API method:** Socrata SODA API on data.cdc.gov. Dataset ID for census-tract level: `cwsq-ngmh`. Example query: `https://data.cdc.gov/resource/cwsq-ngmh.json?StateAbbr=MD&PlaceName=Baltimore`

**API key required:** No (anonymous access allowed; rate-limited; Socrata app token recommended for production)

**Geographic levels:** Census tract, place (city), county

**Years available:** 2020–2024 (annual releases)

**Update frequency:** Annual

**Candidate indicators:** Current asthma among adults (%), COPD (%), diabetes (%), coronary heart disease (%), high blood pressure (%), obesity (%), current smoking (%), physical inactivity (%), mental health not good ≥14 days/month (%), general health fair or poor (%), preventive service utilization

**Overlap with dashboard metrics:** Asthma tab (as a proxy or secondary source), general health context for the Overview panel, and as a cross-validation layer for BNIA asthma ED data.

**CSA join method:** Census-tract centroids → point-in-polygon join with `csa_boundaries.geojson`. Aggregate tract estimates to CSA using population-weighted averaging. Requires a tract-to-CSA crosswalk table (available from BNIA or constructible from census TIGER files).

**Data type:** MODEL-BASED small-area estimates (not direct clinical counts or survey microdata). Derived from BRFSS survey data using multilevel regression and poststratification (MRP). Must be clearly labeled as modeled estimates.

**Limitations:**
- Confidence intervals can be wide at small geographies (individual tracts)
- Values represent estimated prevalence in the adult population, not counts
- Not directly comparable to BNIA asthma ED visit rate (different numerator/denominator)
- Must be labeled "CDC PLACES modeled estimate" on the dashboard — do not present as clinical measurement data

**Recommended MVP use:** Use as a secondary/context source for the Determinants tab. Display CDC PLACES asthma prevalence alongside BNIA asthma ED rate with explicit labeling. Activate via the `proxy_metrics` metadata flag in `data.json`.

**Recommended later-phase use:** Full set of chronic disease indicators for the Determinants deep-dive. Use confidence interval bands when displaying on charts. Build the tract-to-CSA crosswalk for population-weighted aggregation.

---

### 2.4 Open Baltimore 311

**Purpose:** City operational service request data with category, status, location (lat/lng), and timestamps. Powers the 311 Hazards map tab and request-rate metrics.

**URL:** https://data.baltimorecity.gov/datasets/baltimore::311-customer-service-requests-2025 (per-year datasets, 2020–2026)  
Legacy Socrata ID: `9agw-sxsr` (historical pre-2020 data)

**API method:** Socrata SODA API. Example: `https://data.baltimorecity.gov/resource/9agw-sxsr.json?$where=created_date>'2024-01-01'&$limit=50000`. ArcGIS Hub per-year datasets also support GeoJSON download.

**API key required:** No (Socrata app token recommended for production rate limits)

**Geographic levels:** Point data (lat/lng per request). No neighborhood/CSA field — requires point-in-polygon join with `csa_boundaries.geojson` to attribute to CSAs.

**Years available:** 2015–present (2015–2019 via legacy Socrata ID; 2020–2026 via per-year ArcGIS Hub datasets)

**Update frequency:** Near real-time (daily updates in the operational system)

**Candidate indicators:**
- 311 request rate per 1,000 residents (all categories)
- Hazard-specific sub-rates: rat eradication, illegal dumping, water/sewer, street lighting
- Median days to resolution (open → closed)
- Share of requests still open after 30 days

**Overlap with dashboard metrics:** Direct source for the "311 Hazards" map tab and the `hz`, `rt`, `dp`, `ws` metrics visible in the Determinants panel.

**CSA join method:** Point-in-polygon spatial join using Turf.js or a pre-built crosswalk. Each request's lat/lng is tested against CSA polygon boundaries from `csa_boundaries.geojson`. This join must be done at data build time (R script or Python ETL); the result is stored in `data.json` as CSA-level aggregates.

**Data type:** Administrative/operational — direct system records from Baltimore City's 311 customer service system.

**Limitations:**
- No neighborhood/CSA field in raw data — all CSA attribution is derived via spatial join
- Request categories are user-selected and inconsistently applied; some cleaning required
- Resolution times are affected by staffing and workflow changes, not just neighborhood conditions
- Duplicate requests for the same incident are common

**Recommended MVP use:** Run the point-in-polygon join in `update_data.R` to generate CSA-level 311 counts for hazard categories. Populate `hz`, `rt`, `dp`, `ws` fields in `data.json`. Display on the 311 layer toggle and Determinants tab.

**Recommended later-phase use:** Build a resolution-time metric per CSA. Create a real-time or weekly refresh pipeline. Add 311 trend lines to the Trends tab.

---

### 2.5 ACS 5-Year Estimates

**Purpose:** Authoritative survey-based estimates for poverty, employment, income, education, housing cost burden, disability, vehicle access, and internet access at tract, place, county, state, and national levels. Primary source for city/state/national benchmark values.

**URL:** https://api.census.gov/data/2024/acs/acs5

**API method:** Census Bureau Data API. Example: `https://api.census.gov/data/2024/acs/acs5?get=B17001_002E,B17001_001E&for=place:04000&in=state:24`

**API key required:** Yes — free registration at https://api.census.gov/data/key_signup.html. Required for production use (higher rate limits).

**Geographic levels:**
- Census tract (for CSA-level aggregation via crosswalk)
- Place: Baltimore City FIPS `2404000`
- County: Baltimore City FIPS `24510`
- State: Maryland FIPS `24`
- National

**Years available:** 2010–2024 (as 5-year rolling estimates; most recent release is 2020–2024 ACS 5-year)

**Update frequency:** Annual (5-year rolling window, released each December)

**Candidate indicators:**
- `B17001` — Population below poverty level
- `B23025` — Employment status (unemployment rate denominator)
- `B19013` — Median household income
- `B15003` — Educational attainment (HS diploma or higher)
- `B25070` — Gross rent as percent of household income (housing cost burden)
- `B18101` — Disability status
- `B08201` — Household vehicle availability
- `B28002` — Internet subscription type

**Overlap with dashboard metrics:** Poverty (`pv`), unemployment (`un`), median income (`mhi`), HS graduation (`hs`) — provides city/state/national comparison values and can serve as a secondary neighborhood source when BNIA data has gaps.

**CSA join method:** Census-tract level estimates aggregated to CSA using a tract-to-CSA population crosswalk. Note: CSA boundaries do not always align perfectly with tract boundaries; use areal interpolation or population-weighted aggregation. BNIA Vital Signs uses ACS as a primary input for many of its indicators, so double-counting must be avoided.

**Data type:** Survey estimates with margins of error (MOE). All ACS estimates carry sampling uncertainty; MOEs should be stored and displayed for small-geography estimates.

**Limitations:**
- 5-year estimates lag approximately 18 months behind the reference period end date
- MOEs can be large for small geographic units (individual tracts with population <1,000)
- Not designed for year-over-year change analysis at fine geographies due to overlapping survey windows

**Recommended MVP use:** Call the Census API at build time to populate `BENCHMARKS.city`, `BENCHMARKS.state`, and `BENCHMARKS.federal` objects in `data.json` for poverty, income, unemployment, and education. Store the API key as an environment variable in the ETL script.

**Recommended later-phase use:** Pull tract-level ACS data, aggregate to CSA, and use as a gap-filler or cross-validation for BNIA indicators. Add MOE bands to trend charts.

---

### 2.6 EPA EJScreen

**Purpose:** Environmental justice screening tool providing block-group-level environmental burden indicators. Used as context layer for air quality, traffic proximity, lead paint risk, and environmental justice index.

**URL:** https://www.epa.gov/ejscreen

**API method:** No programmatic API. Download the nationwide or state-level CSV from the EJScreen download page. Version 2.3 (July 2024) is current.

**API key required:** No

**Geographic levels:** Census block group (aggregable to census tract and CSA via spatial join)

**Years available:** Annual versions since ~2015. Current: EJScreen 2.3 (July 2024)

**Update frequency:** Annual

**Candidate indicators:**
- `PM25` — PM2.5 in air (µg/m³, annual average)
- `OZONE` — Ozone (ppb, summer days average)
- `DSLPM` — Diesel PM emissions (µg/m³)
- `PTRAF` — Traffic proximity and volume (daily count per meter)
- `LDPNT` — Lead paint indicator (% pre-1960 housing)
- `PNPL` — Proximity to Superfund/NPL sites
- `PWDIS` — Proximity to wastewater discharge
- `UST` — Underground storage tanks proximity
- `DEMOGIDX` — Demographic Index (minority + low income percentages)
- `EJ_INDEXD2_B` and `EJ_INDEXD5_B` — Environmental justice supplemental indexes

**Overlap with dashboard metrics:** PM2.5 air quality, traffic proximity, lead paint risk, and environmental justice index for the Determinants tab and future Environment layer toggle.

**CSA join method:** Download Maryland block-group CSV. Join block groups to CSAs using `csa_boundaries.geojson` via spatial join (block group centroid or area-weighted overlap). Aggregate to CSA using population-weighted mean.

**Data type:** Modeled/derived screening estimates (not direct regulatory measurements). Derived from a mix of EPA ambient monitoring data, emissions inventories, and demographic census data.

**Limitations:**
- EJScreen is a screening tool; values should not be used as causal health exposure measurements or regulatory determinations
- Block-group estimates have wide uncertainty ranges; treat as relative ranking, not precise exposure
- Data currency varies by underlying source; some components lag 2–3 years
- Must be labeled "EPA EJScreen screening estimate" — not "measured pollution level"

**Recommended MVP use:** Load as a static reference layer. Extract PM2.5, traffic proximity, and lead paint indicator for Baltimore City block groups. Aggregate to CSA and store in `data.json` for the Determinants tab environmental section.

**Recommended later-phase use:** Build a dedicated Environment tab with map choropleth for EJ index. Add NO2 (new in EJScreen 2.3). Display block-group granularity for users who zoom in.

---

### 2.7 Baltimore Area Survey

**Purpose:** Annual resident perception survey on neighborhood safety, quality of life, housing, climate adaptation, and crime conducted by Johns Hopkins 21st Century Cities Initiative. Provides the community voice / sentiment layer.

**URL:** https://21cc.jhu.edu/baltimore-area-survey/  
**Data (GitHub):** https://github.com/JHUCities/baltimore-area-survey-data

**API method:** Public CSV files on GitHub. No API — download directly from the repository. Waves released each December.

**API key required:** No

**Geographic levels:** Baltimore City (surveyed); CSA-level estimates require pooling multiple waves (~748 respondents per wave; single-wave n is too small for reliable CSA-level estimates)

**Years available:** 2023, 2024, 2025 (annual December releases)

**Update frequency:** Annual (December)

**Candidate indicators:**
- Safety perception (% feeling safe in neighborhood, day/night)
- Neighborhood quality satisfaction (% rating neighborhood as good or excellent)
- Crime concern index
- Housing stability indicators
- Climate adaptation willingness

**Overlap with dashboard metrics:** Community sentiment tab (future phase). Provides the only source of direct resident perception data at sub-city geography.

**CSA join method:** Survey respondents are assigned to CSAs using a geographic identifier in the raw microdata. For reliable CSA-level estimates, pool 2 or more survey waves before aggregation (combined n ≈ 1,496 for 2023+2024 waves). Document pooled wave years in data source footnotes.

**Data type:** Probability sample survey (n ≈ 748 Baltimore City residents per wave). Not administrative data.

**Limitations:**
- Single-wave sample (n ≈ 748) is insufficient for CSA-level estimates in most CSAs; requires pooling
- Non-response and coverage biases typical of telephone/online surveys
- Must be labeled "Resident perception survey — Baltimore Area Survey, Johns Hopkins 21CC" and distinguished from administrative or clinical data
- Not all CSAs are represented equally in each wave

**Recommended MVP use:** Not included in MVP. Store notes for Phase 3 implementation.

**Recommended later-phase use:** Phase 3: Load pooled 2023–2025 wave microdata from GitHub. Aggregate safety perception and neighborhood quality scores to CSA. Display in a dedicated Community Sentiment tab with survey methodology footnote. Update annually with each December release.

---

### 2.8 Bureau of Labor Statistics (BLS)

**Purpose:** Official labor force statistics including unemployment rates for Baltimore City, Maryland, and the nation. Used as benchmark context for the unemployment metric.

**URL:** https://api.bls.gov/publicAPI/v2/timeseries/data/

**API method:** BLS Public Data API v2. POST request with series IDs and year range. Example series: `LAUCN245100000000003A` (Baltimore City annual unemployment rate).

**API key required:** Recommended (free registration at https://data.bls.gov/registrationEngine/). Required for higher rate limits and longer date ranges. Anonymous access limited to 25 daily queries.

**Geographic levels:** Baltimore City, Maryland state, U.S. national

**Years available:** 1990–present (annual and monthly series available)

**Update frequency:** Monthly (with annual revisions)

**Candidate indicators:**
- `LAUCN245100000000003A` — Baltimore City unemployment rate (annual, %)
- `LASST240000000000003` — Maryland unemployment rate
- `LNS14000000` — U.S. national unemployment rate

**Overlap with dashboard metrics:** Provides city/state/national benchmark values for the unemployment (`un`) metric. Complements ACS employment status data.

**CSA join method:** Not applicable. BLS data is city/state/national only — no sub-city geography.

**Data type:** Administrative labor force statistics (Current Population Survey + Local Area Unemployment Statistics program)

**Limitations:**
- No sub-city or neighborhood-level data
- Annual LAUS estimates are modeled at small-area level and carry uncertainty
- Superseded for neighborhood-level estimates by BNIA (which draws from ACS)

**Recommended MVP use:** Query at build time for the most recent 8 years of annual Baltimore City, Maryland, and U.S. unemployment rates. Store in `BENCHMARKS.city`, `BENCHMARKS.state`, `BENCHMARKS.federal` for the unemployment benchmark bar.

**Recommended later-phase use:** Add monthly series for a real-time unemployment widget. Use as a cross-check against BNIA unemployment figures.

---

### 2.9 FRED (Federal Reserve Economic Data)

**Purpose:** Alternative economic time series from the St. Louis Federal Reserve. Largely redundant with BLS for unemployment; useful for income and economic context series not available elsewhere.

**URL:** https://api.stlouisfed.org/fred/series/observations

**API method:** REST API with `series_id` and `api_key` parameters. Example: `https://api.stlouisfed.org/fred/series/observations?series_id=MDBALT5URN&api_key=YOUR_KEY&file_type=json`

**API key required:** Yes — free registration at https://fred.stlouisfed.org/docs/api/api_key.html

**Geographic levels:** Baltimore City (monthly series), Maryland state, U.S. national

**Years available:** Varies by series; most unemployment series start in the 1990s

**Update frequency:** Monthly (mirrors BLS release schedule)

**Candidate indicators:**
- `MDBALT5URN` — Baltimore City monthly unemployment rate
- `MDUR` — Maryland unemployment rate
- `UNRATE` — U.S. national unemployment rate

**Overlap with dashboard metrics:** Same unemployment benchmark values available via BLS; FRED is a convenience wrapper. For median household income, ACS is a superior source at city and neighborhood levels.

**CSA join method:** Not applicable — national/state/city level only.

**Data type:** Administrative/derived (FRED re-packages BLS, Census, and BEA data)

**Limitations:**
- API key required, unlike BLS public API
- Largely duplicates BLS data
- No sub-city geography
- Income series for small geographies are better sourced from ACS

**Recommended MVP use:** Not required if BLS is implemented. Optional convenience source for teams already familiar with the FRED API.

**Recommended later-phase use:** Use for economic context widgets if time-series visualization beyond unemployment is added (e.g., housing price index). Consider only if BLS API proves difficult to integrate.

---

## 3. Indicator Map

### 3.1 Core Neighborhood Indicators (CSA Level)

| Dashboard Metric | Key | Primary Source | Source ID | Variable(s) | Notes |
|-----------------|-----|---------------|-----------|-------------|-------|
| Health Index | `hi` | BNIA Vital Signs | `bnia_vital_signs` | Composite (derived) | Weighted composite of core metrics |
| Life Expectancy | `le` | BNIA Vital Signs | `bnia_vital_signs` | `leXX` | Years at birth |
| Asthma ED Rate | `as` | BNIA Vital Signs | `bnia_vital_signs` | `astXX` | ED visits per 1,000 |
| Lead Exposure | `la` | BNIA Vital Signs | `bnia_vital_signs` | `nleaXX` | % children under 6 with elevated BLL |
| Vacant Housing | `va` | BNIA Vital Signs | `bnia_vital_signs` | `vacXX` | % vacant units |
| Poverty Rate | `pv` | BNIA Vital Signs | `bnia_vital_signs` | `hhpovXX` | % households below poverty |
| Unemployment Rate | `un` | BNIA Vital Signs | `bnia_vital_signs` | `unemXX` | % labor force unemployed |
| HS Graduation | `hs` | BNIA Vital Signs | `bnia_vital_signs` | `compXX` | % adults HS diploma or higher |
| 311 Hazard Rate | `hz` | Open Baltimore 311 | `open_baltimore_311` | Count per CSA (via spatial join) | Requires point-in-polygon join |
| 311 Resolution Time | `rt_days` | Open Baltimore 311 | `open_baltimore_311` | Median days open→closed per CSA | Phase 2 |

### 3.2 City / State / National Comparison Indicators

| Metric | City Benchmark Source | State/National Source | Notes |
|--------|----------------------|----------------------|-------|
| Unemployment | BLS (`LAUCN245100000000003A`) / City Health Dashboard | BLS (`LASST240000000000003`, `LNS14000000`) | BLS preferred for annual series |
| Poverty Rate | ACS 5-yr (`B17001`) / City Health Dashboard | ACS 5-yr (state and national) | |
| Median HH Income | ACS 5-yr (`B19013`) | ACS 5-yr (state and national) | |
| HS Graduation | ACS 5-yr (`B15003`) | ACS 5-yr (state and national) | |
| Asthma (adult %) | CDC PLACES (`cwsq-ngmh`) | CDC PLACES (national) | Label as modeled estimate |
| Life Expectancy | City Health Dashboard | City Health Dashboard (national) | |

### 3.3 Environmental Context Indicators

| Metric | Source | Variable | Notes |
|--------|--------|----------|-------|
| PM2.5 Air Quality | EPA EJScreen | `PM25` | Modeled annual mean µg/m³; screening only |
| Traffic Proximity | EPA EJScreen | `PTRAF` | Daily count per meter; screening only |
| Lead Paint Risk | EPA EJScreen | `LDPNT` | % pre-1960 housing; proxy for lead paint risk |
| Environmental Justice Index | EPA EJScreen | `EJ_INDEXD2_B` | Combined EJ index; screening only |
| Diesel Particulate Matter | EPA EJScreen | `DSLPM` | µg/m³; screening only |

### 3.4 Community Sentiment Indicators

| Metric | Source | Notes |
|--------|--------|-------|
| Safety Perception | Baltimore Area Survey (JHU 21CC) | % feeling safe; must pool ≥2 waves for CSA estimates |
| Neighborhood Quality | Baltimore Area Survey (JHU 21CC) | % rating neighborhood good/excellent; pool ≥2 waves |

---

## 4. Implementation Order

1. **BNIA Vital Signs (MVP)** — Load annual CSV for Vital Signs 18–22 into `data.json`. This is the spine of all neighborhood-level metrics. Without this, the map has no data. Run the R ETL script to normalize into the multi-year JSON schema.

2. **ACS 5-Year API (MVP)** — Register for a Census API key. Call the ACS API at build time to generate city/state/national benchmark rows for poverty, income, unemployment, and education. Eliminates the derived/synthetic benchmarks currently in use.

3. **Open Baltimore 311 (MVP)** — Download the 2016–2023 per-year 311 CSVs from Open Baltimore. Run point-in-polygon join in R to aggregate hazard categories to CSA. Populate `hz`, `rt`, `dp`, `ws` fields in `data.json`.

4. **City Health Dashboard API (MVP)** — Query the public API for Baltimore City benchmark values for chronic disease and social determinants. Merge into the benchmark layer of `data.json`. Validates/extends the ACS-derived benchmarks.

5. **CDC PLACES — tract level (Phase 2)** — Download Maryland tract-level data from Socrata. Build tract-to-CSA crosswalk. Aggregate asthma prevalence and other indicators to CSA using population weights. Add as secondary/proxy layer with clear labeling.

6. **EPA EJScreen (Phase 2)** — Download Maryland block-group CSV. Spatial join to CSA. Populate PM2.5, traffic, lead paint, and EJ index for the Determinants tab environmental section.

7. **Baltimore Area Survey (Phase 3)** — Download pooled 2023–2025 wave microdata from the JHU GitHub repository. Aggregate safety and neighborhood quality scores to CSA. Build Community Sentiment tab with appropriate survey methodology footnotes.

8. **BLS API (Phase 3 / ongoing)** — Integrate into the build pipeline to keep unemployment benchmarks current with each BLS monthly release. Register for a BLS API key.

9. **FRED API (Optional)** — Implement only if BLS API proves insufficient or if additional macroeconomic series are needed for future dashboard features.

---

## 5. Acceptance Criteria Checklist

### Data Ingestion
- [ ] BNIA Vital Signs 18–22 CSV loaded and normalized into `data.json` multi-year schema
- [ ] All 55 CSA names in `data.json` match CSA names in `csa_boundaries.geojson`
- [ ] ACS API key stored as environment variable (not hardcoded)
- [ ] City/state/national benchmarks populated for all BENCHMARK_METRICS set metrics
- [ ] 311 point-in-polygon join produces non-null `hz` values for ≥50 of 55 CSAs

### Data Quality
- [ ] CDC PLACES values labeled "Modeled estimate — CDC PLACES" in UI wherever displayed
- [ ] EPA EJScreen values labeled "Screening estimate — EPA EJScreen" in UI wherever displayed
- [ ] Baltimore Area Survey values labeled "Resident perception survey — JHU 21CC"
- [ ] No raw ACS margin-of-error values silently dropped; MOEs stored in `data.json`
- [ ] Health index composite documented: formula, component weights, and source attribution visible in legend or tooltip

### Geographic Integrity
- [ ] All CSA-level metrics validated against the 55-CSA boundary file
- [ ] Census tract → CSA crosswalk documented and versioned in `/data/metadata/`
- [ ] 311 lat/lng records outside Baltimore City bounds excluded from aggregation
- [ ] EJScreen block-group-to-CSA aggregation uses population-weighted mean (not simple mean)

### Benchmark Comparisons
- [ ] City average benchmark reflects Baltimore City, not Maryland average
- [ ] State benchmark reflects Maryland
- [ ] Federal/national benchmark reflects U.S. average
- [ ] Benchmark year matches neighborhood data year in all side-by-side comparisons

### Documentation
- [ ] `source_catalog.json` updated whenever a new source is added
- [ ] `data_dictionary.json` updated whenever a new indicator is added
- [ ] This audit document updated at each major data release (annual minimum)
- [ ] Provisional data clearly flagged in `data.json` `meta.provisional` field
