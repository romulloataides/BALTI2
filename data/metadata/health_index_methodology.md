# Health Index (hi) — Methodology

## Status

**Derived composite.** The `hi` metric is not a direct import from BNIA.
It is computed by `update_data.R` from six official component indicators
when a direct BNIA health index series is not available.

To replace this with the official BNIA composite: contact BNIA at
[bniajfi.org/contact](https://bniajfi.org/contact) and request the Vital
Signs composite health index series for all 55 CSAs across available years.
When a direct series is available, set `derive_hi_from_components <- FALSE`
in `update_data.R` (line ~1735) — the pipeline will use imported values instead.

---

## Formula

```
hi = weighted_mean(le_n, pv_n, as_n, un_n, hs_n, la_n)
```

where each `_n` suffix denotes a normalized score (0–100 scale).

**Minimum component requirement:** 4 of 6 components must be non-missing
to produce a score. Otherwise `hi = NA`.

**Clamping:** final score is clamped to [0, 100].

---

## Component weights

| Metric | Label | Weight | Direction | Rationale |
|---|---|---|---|---|
| `le` | Life expectancy (years) | **0.25** | Higher = better | Strongest single predictor of overall neighborhood health |
| `pv` | Poverty rate (%) | **0.20** | Lower = better (inverted) | Primary social determinant of health |
| `as` | Asthma prevalence (%) | **0.15** | Lower = better (inverted) | Key chronic disease health outcome proxy |
| `un` | Unemployment rate (%) | **0.15** | Lower = better (inverted) | Economic determinant; strongly correlated with health |
| `hs` | HS graduation rate (%) | **0.15** | Higher = better | Educational attainment; life-chance proxy |
| `la` | Lead exposure (%) | **0.10** | Lower = better (inverted) | Environmental determinant; irreversible developmental impact |

Weights sum to **1.00**. If a component is missing for a given CSA-year,
remaining weights are rescaled proportionally so they still sum to 1.

**Excluded:** `va` (vacant housing) is tracked as its own indicator and
excluded from the composite to avoid double-counting housing-related poverty effects.

---

## Normalization

Each component is normalized using **citywide min-max scaling** across all
55 CSAs for each year independently:

```
normalized = (raw - city_min) / (city_max - city_min) × 100
```

Inverse metrics (`as`, `la`, `pv`, `un`) are flipped after normalization:

```
normalized_inv = 100 - normalized
```

This ensures higher `hi` always means better health, regardless of metric direction.

---

## Data sources for components

| Metric | Source | Type |
|---|---|---|
| `le` | BNIA Vital Signs / ACS life tables | Administrative/modeled |
| `pv` | ACS 5-year B17001 | Survey estimate |
| `as` | CDC PLACES tract estimates (crosswalked to CSA) | **Modeled estimate** |
| `un` | ACS 5-year B23025 / BNIA Vital Signs | Survey estimate |
| `hs` | BNIA Vital Signs / ACS B15003 | Administrative/survey |
| `la` | BNIA Vital Signs / Maryland childhood blood lead surveillance | Administrative |

> **Note on `as`:** The current asthma metric is CDC PLACES adult asthma
> prevalence — a modeled estimate, not the original asthma ED rate from BCHD.
> Replacing this with the BCHD asthma emergency department rate would improve
> the composite. See `update_data.R` for the TODO comment in `load_cdc_asthma_longitudinal`.

---

## Methodological reference

Adapted from the [RWJF County Health Rankings](https://www.countyhealthrankings.org/explore-health-rankings/methods-and-ranking-approach/methods)
composite approach, which uses z-score normalization and explicit component
weights derived from evidence on social determinants of health.

The main departure from County Health Rankings: we use min-max normalization
(instead of z-scores) to produce an intuitive 0–100 scale for display in the dashboard.

---

## Changelog

| Date | Change |
|---|---|
| 2026-05-01 | Replaced equal-weight `mean()` with explicit `weighted.mean()` using documented weights. Added `va` exclusion rationale. Added clamping to [0, 100]. |
| Initial | Equal-weight mean of all available components. |
