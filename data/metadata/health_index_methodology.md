# Health Index (`hi`) Methodology

## Status

`hi` is a **derived composite** metric, not an official BNIA-published index.

- `data.json` now labels this explicitly: `meta.data_type.hi = "derived_composite"`.
- If BNIA later publishes an official CSA-level health index series for 2016-2023, that official series should replace this derived method.

## Composite Formula

For each year and CSA:

```text
hi = 0.25*le_n + 0.15*as_n + 0.10*la_n + 0.20*pv_n + 0.15*un_n + 0.15*hs_n
```

Where:

- `le` = life expectancy
- `as` = asthma rate metric currently populated by proxy source (to be replaced by BCHD asthma ED rate)
- `la` = lead exposure
- `pv` = poverty rate
- `un` = unemployment
- `hs` = high school graduation / attainment

Inverse metrics (`as`, `la`, `pv`, `un`) are flipped so **higher is always better**.

## Normalization

Each component is normalized to 0-100 by year, across Baltimore CSAs:

```text
component_n = (x - city_min_year) / (city_max_year - city_min_year) * 100
```

For inverse metrics:

```text
component_n = 100 - component_n
```

The final `hi` value is clamped to `[0, 100]`.

## Missing-Data Rule

- At least **4 of 6** components are required to compute `hi` in a given CSA-year.
- If fewer than 4 components are present, `hi` is `NA` for that CSA-year.
- When some components are missing, available component weights are re-normalized to sum to 1.

## Citywide Min/Max Bounds Used (Current Snapshot)

These are the observed citywide bounds from the current `data.json` used by the normalization step when this snapshot was generated.

| Year | Metric | Min | Max |
|---|---|---:|---:|
| 2016 | le | 66.400 | 85.200 |
| 2016 | as | 8.700 | 15.200 |
| 2016 | la | 0.000 | 6.200 |
| 2016 | pv | 6.800 | 51.500 |
| 2016 | un | 3.300 | 25.400 |
| 2016 | hs | 60.800 | 99.200 |
| 2017 | le | 65.700 | 85.000 |
| 2017 | as | 8.000 | 14.000 |
| 2017 | la | 0.000 | 4.100 |
| 2017 | pv | 6.700 | 51.300 |
| 2017 | un | 2.000 | 25.700 |
| 2017 | hs | 61.000 | 99.000 |
| 2018 | le | 63.200 | 84.700 |
| 2018 | as | 8.000 | 14.800 |
| 2018 | la | 0.000 | 7.500 |
| 2018 | pv | 6.600 | 51.300 |
| 2018 | un | 1.800 | 26.600 |
| 2018 | hs | 61.500 | 98.800 |
| 2019 | as | 8.600 | 16.000 |
| 2019 | la | 0.000 | 6.000 |
| 2019 | pv | 5.900 | 50.800 |
| 2019 | un | 1.300 | 23.600 |
| 2019 | hs | 61.400 | 99.200 |
| 2020 | as | 8.400 | 15.600 |
| 2020 | pv | 5.700 | 50.300 |
| 2020 | un | 1.200 | 24.400 |
| 2020 | hs | 61.300 | 98.800 |
| 2021 | as | 9.000 | 16.000 |
| 2021 | pv | 5.700 | 50.300 |
| 2021 | un | 1.100 | 23.500 |
| 2021 | hs | 61.900 | 98.700 |
| 2022 | as | 9.400 | 15.900 |
| 2022 | pv | 5.200 | 50.100 |
| 2022 | un | 1.500 | 22.900 |
| 2022 | hs | 62.000 | 98.000 |
| 2023 | as | 9.900 | 15.800 |
| 2023 | pv | 4.700 | 49.400 |
| 2023 | un | 1.400 | 22.700 |
| 2023 | hs | 59.400 | 98.700 |

Note: Bounds can change when source data refreshes.

## Asthma Metric Upgrade TODO

Current `as` neighborhood values come from CDC PLACES proxy data.

Planned replacement:

- Replace `as` with BCHD asthma ED rate data from Open Baltimore once a public dataset/API endpoint is available.
- If no endpoint is public, request BCHD data-team publication and then update `load_cdc_asthma_longitudinal()` in `update_data.R` to pull the official ED-rate series.
