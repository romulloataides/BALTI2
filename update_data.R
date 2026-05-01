library(tidyverse)
library(sf)
library(jsonlite)
library(tidycensus)
library(httr)

print("Starting Baltimore Health Data Pipeline (CSA Architecture)...")

years <- 2016:2023

normalize_name <- function(x) {
  x %>%
    replace_na("") %>%
    str_replace_all("&", " and ") %>%
    str_replace_all("[^[:alnum:]]+", " ") %>%
    str_squish() %>%
    str_to_lower()
}

parse_year_value <- function(x) {
  suppressWarnings(as.integer(str_extract(as.character(x), "(19|20)\\d{2}")))
}

coerce_numeric_value <- function(x) {
  txt <- as.character(x)
  txt[is.na(txt)] <- NA_character_
  txt <- str_squish(txt)
  txt[txt %in% c("", "NA", "N/A", "null", "--", "suppressed", "Suppressed")] <- NA_character_
  txt[str_detect(txt, "^<")] <- NA_character_
  readr::parse_number(txt, locale = readr::locale(grouping_mark = ","))
}

dashboard_metric_aliases <- list(
  hi = c("hi", "health index", "composite health index"),
  le = c("le", "life expectancy", "life exp", "life expectancy at birth", "lifexp"),
  as = c("as", "asthma ed", "asthma ed rate", "asthma ed visits", "asthma emergency department"),
  la = c("la", "lead", "lead exp", "lead exposure", "children with elevated blood lead levels"),
  va = c("va", "vacant", "vacant housing", "vacant properties", "vacant building notices"),
  pv = c("pv", "poverty", "poverty rate"),
  un = c("un", "unemployment", "unemployment rate"),
  hs = c("hs", "hs grad", "high school graduation", "high school grad", "high school attainment"),
  fd = c("fd", "food desert", "food access"),
  gs = c("gs", "green space", "greenspace"),
  cr = c("cr", "crime", "crime rate", "violent crime"),
  rt = c("rt", "rat eradication", "rodent", "rodent complaints", "rats"),
  dp = c("dp", "illegal dumping", "dumping"),
  ws = c("ws", "water sewer", "water and sewer", "water/sewer"),
  hz = c("hz", "hazards", "311 hazards", "total hazards")
)

metric_alias_lookup <- purrr::imap(
  dashboard_metric_aliases,
  ~ {
    vals <- rep(.y, length(.x))
    names(vals) <- normalize_name(.x)
    vals
  }
) %>%
  unname() %>%
  do.call(c, .)

resolve_dashboard_metric <- function(x) {
  key <- normalize_name(x)
  matched <- unname(metric_alias_lookup[key])
  matched <- matched[!is.na(matched)]
  if (!length(matched)) {
    return(NA_character_)
  }
  matched[[1]]
}

detect_first_col <- function(df, patterns, exclude = character()) {
  candidates <- setdiff(names(df), exclude)
  match <- candidates[str_detect(str_to_lower(candidates), patterns)][1]
  if (is.na(match)) NA_character_ else match
}

col_or_default <- function(df, col, default = NA) {
  if (col %in% names(df)) {
    df[[col]]
  } else {
    rep(default, nrow(df))
  }
}

find_bnia_source <- function() {
  env_path <- trimws(Sys.getenv("BNIA_VITAL_SIGNS_PATH"))
  candidates <- c(
    env_path,
    "bnia_vital_signs.csv",
    "bnia_vital_signs.xlsx",
    "BNIA_Vital_Signs.csv",
    "BNIA_Vital_Signs.xlsx",
    "vital_signs.csv",
    "vital_signs.xlsx",
    "data_sources/bnia_vital_signs.csv",
    "data_sources/bnia_vital_signs.xlsx",
    "data_sources/BNIA_Vital_Signs.csv",
    "data_sources/BNIA_Vital_Signs.xlsx"
  )

  existing <- candidates[nzchar(candidates) & file.exists(candidates)]
  if (!length(existing)) return(NA_character_)
  existing[[1]]
}

read_bnia_table <- function(path) {
  ext <- str_to_lower(tools::file_ext(path))

  if (ext %in% c("csv", "txt")) {
    return(readr::read_csv(path, show_col_types = FALSE, progress = FALSE))
  }

  if (ext %in% c("tsv", "tab")) {
    return(readr::read_tsv(path, show_col_types = FALSE, progress = FALSE))
  }

  if (ext %in% c("xlsx", "xls")) {
    if (!requireNamespace("readxl", quietly = TRUE)) {
      stop("BNIA spreadsheet input requires the readxl package. Install readxl or export the file to CSV.")
    }
    return(readxl::read_excel(path))
  }

  stop(paste("Unsupported BNIA file type:", ext))
}

series_from_long <- function(long_df, csa_lookup, years = 2016:2023) {
  aligned <- long_df %>%
    transmute(
      CSA_input = as.character(CSA),
      CSA_key = normalize_name(CSA_input),
      metric = map_chr(metric, resolve_dashboard_metric),
      Year = parse_year_value(Year),
      value = coerce_numeric_value(value)
    )

  unmatched <- aligned %>%
    distinct(CSA_input, CSA_key) %>%
    anti_join(csa_lookup, by = "CSA_key") %>%
    pull(CSA_input)

  matched <- aligned %>%
    filter(!is.na(metric), Year %in% years) %>%
    inner_join(csa_lookup %>% rename(CSA_canonical = CSA), by = "CSA_key") %>%
    transmute(CSA = CSA_canonical, CSA_key, metric, Year, value) %>%
    group_by(CSA, CSA_key, metric, Year) %>%
    summarize(
      value = if (all(is.na(value))) NA_real_ else round(mean(value, na.rm = TRUE), 1),
      .groups = "drop"
    ) %>%
    group_by(CSA, CSA_key, metric) %>%
    tidyr::complete(Year = years) %>%
    arrange(Year, .by_group = TRUE) %>%
    summarize(series = list(as.numeric(value)), .groups = "drop") %>%
    pivot_wider(names_from = metric, values_from = series)

  list(
    data = matched,
    metrics = setdiff(names(matched), c("CSA", "CSA_key")),
    unmatched = unmatched
  )
}

extract_bnia_longitudinal <- function(df, csa_lookup, years = 2016:2023) {
  name_col <- detect_first_col(df, "^community$|csa2010|csa2020|csa_name|^csa$|neigh|^name$")
  if (is.na(name_col)) {
    stop("Could not detect a CSA/neighborhood column in the BNIA file.")
  }

  year_cols <- intersect(as.character(years), names(df))
  metric_col <- detect_first_col(df, "metric|indicator|measure|variable|series|metric_key", exclude = name_col)
  year_col <- detect_first_col(df, "^year$|calendar.?year|fiscal.?year|period|time", exclude = c(name_col, metric_col))
  value_col <- detect_first_col(
    df,
    "^value$|value$|estimate$|score$|pct$|percent$|percentage$|share$|count$|number$|amount$",
    exclude = c(name_col, metric_col, year_col)
  )

  mapped_metric_cols <- tibble(source_col = names(df)) %>%
    filter(!source_col %in% c(name_col, metric_col, year_col, value_col)) %>%
    mutate(metric = map_chr(source_col, resolve_dashboard_metric)) %>%
    filter(!is.na(metric))

  if (length(year_cols) > 0 && !is.na(metric_col) && is.na(year_col)) {
    long_df <- df %>%
      transmute(
        CSA = .data[[name_col]],
        metric = .data[[metric_col]],
        across(all_of(year_cols))
      ) %>%
      pivot_longer(cols = all_of(year_cols), names_to = "Year", values_to = "value")

    parsed <- series_from_long(long_df, csa_lookup, years = years)
    parsed$format <- "indicator-by-year"
    return(parsed)
  }

  if (!is.na(year_col) && !is.na(metric_col) && !is.na(value_col)) {
    long_df <- df %>%
      transmute(
        CSA = .data[[name_col]],
        Year = .data[[year_col]],
        metric = .data[[metric_col]],
        value = .data[[value_col]]
      )

    parsed <- series_from_long(long_df, csa_lookup, years = years)
    parsed$format <- "long"
    return(parsed)
  }

  if (!is.na(year_col) && nrow(mapped_metric_cols) > 0) {
    long_df <- df %>%
      select(all_of(c(name_col, year_col, mapped_metric_cols$source_col))) %>%
      pivot_longer(
        cols = all_of(mapped_metric_cols$source_col),
        names_to = "source_metric",
        values_to = "value"
      ) %>%
      left_join(mapped_metric_cols, by = c("source_metric" = "source_col")) %>%
      transmute(
        CSA = .data[[name_col]],
        Year = .data[[year_col]],
        metric = metric,
        value = value
      )

    parsed <- series_from_long(long_df, csa_lookup, years = years)
    parsed$format <- "wide"
    return(parsed)
  }

  stop(
    paste(
      "Could not parse the BNIA file.",
      "Supported formats are:",
      "1) CSA + Year + metric columns;",
      "2) CSA + Year + Indicator + Value;",
      "3) CSA + Indicator + 2016...2023 year columns."
    )
  )
}

load_bnia_longitudinal <- function(csa_lookup, years = 2016:2023) {
  source_path <- find_bnia_source()

  if (is.na(source_path)) {
    print("No BNIA longitudinal file found. Keeping existing neighborhood data as the baseline.")
    return(list(
      data = tibble(CSA = character(), CSA_key = character()),
      metrics = character(),
      unmatched = character(),
      format = NA_character_,
      source_path = NA_character_
    ))
  }

  print(paste("Loading BNIA longitudinal data from", source_path, "..."))

  parsed <- tryCatch(
    {
      raw <- read_bnia_table(source_path)
      extract_bnia_longitudinal(raw, csa_lookup, years = years)
    },
    error = function(e) {
      warning(paste("BNIA import failed:", conditionMessage(e)))
      NULL
    }
  )

  if (is.null(parsed)) {
    return(list(
      data = tibble(CSA = character(), CSA_key = character()),
      metrics = character(),
      unmatched = character(),
      format = NA_character_,
      source_path = source_path
    ))
  }

  if (length(parsed$unmatched)) {
    warning(
      paste(
        "BNIA rows skipped because their CSA names did not match the dashboard boundary file:",
        paste(unique(parsed$unmatched), collapse = "; ")
      )
    )
  }

  print(
    paste(
      "  Imported",
      length(parsed$metrics),
      "BNIA metrics across",
      nrow(parsed$data),
      "CSAs using",
      parsed$format,
      "format."
    )
  )

  parsed$source_path <- source_path
  parsed
}

fetch_bnia_service_metric <- function(metric, service_url, field_prefix, csa_lookup, years = 2016:2023, transform = identity) {
  query_url <- paste0(
    service_url,
    "/0/query?where=1%3D1&returnGeometry=false&outFields=*&f=json"
  )

  payload <- tryCatch(
    {
      response <- GET(query_url, timeout(120))
      stop_for_status(response)
      fromJSON(content(response, "text", encoding = "UTF-8"), simplifyDataFrame = TRUE)
    },
    error = function(e) {
      warning(paste("BNIA service import failed for", metric, ":", conditionMessage(e)))
      NULL
    }
  )

  attrs <- payload$features$attributes
  if (is.null(attrs) || !nrow(as_tibble(attrs))) {
    return(tibble(CSA = character(), CSA_key = character()))
  }

  attrs <- as_tibble(attrs)
  csa_2020_col <- names(attrs)[str_detect(str_to_lower(names(attrs)), "^csa2020$")][1]
  csa_2010_col <- names(attrs)[str_detect(str_to_lower(names(attrs)), "^csa2010$|^community$|^name$")][1]

  if (is.na(csa_2010_col) && is.na(csa_2020_col)) {
    warning(paste("BNIA service import failed for", metric, ": CSA name column not found."))
    return(tibble(CSA = character(), CSA_key = character()))
  }

  year_fields <- set_names(
    paste0(field_prefix, substr(years, 3, 4)),
    as.character(years)
  )

  available_fields <- intersect(unname(year_fields), names(attrs))

  if (!length(available_fields)) {
    return(tibble(CSA = character(), CSA_key = character()))
  }

  service_tbl <- attrs %>%
    transmute(
      CSA = coalesce(
        if (!is.na(csa_2020_col)) as.character(.data[[csa_2020_col]]) else NA_character_,
        if (!is.na(csa_2010_col)) as.character(.data[[csa_2010_col]]) else NA_character_
      )
    ) %>%
    mutate(CSA = str_squish(CSA), CSA_key = normalize_name(CSA))

  service_tbl[[metric]] <- purrr::pmap(
    attrs[, available_fields, drop = FALSE],
    function(...) {
      row_values <- list(...)
      series <- map_dbl(as.character(years), function(year_key) {
        field_name <- year_fields[[year_key]]
        if (!field_name %in% available_fields) return(NA_real_)
        value <- row_values[[match(field_name, available_fields)]]
        transformed <- tryCatch(transform(as.numeric(value)), error = function(e) NA_real_)
        if (is.na(transformed)) NA_real_ else round(as.numeric(transformed), 1)
      })
      series
    }
  )

  service_tbl %>%
    inner_join(csa_lookup, by = "CSA_key", suffix = c("_service", "")) %>%
    transmute(CSA = coalesce(CSA, CSA_service), CSA_key, !!metric := .data[[metric]])
}

load_bnia_service_longitudinal <- function(csa_lookup, years = 2016:2023) {
  service_specs <- list(
    le = list(url = "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Lifexp/FeatureServer", prefix = "lifexp", transform = identity),
    la = list(url = "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Ebll/FeatureServer", prefix = "ebll", transform = identity),
    va = list(url = "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Vacant/FeatureServer", prefix = "vacant", transform = identity),
    un = list(url = "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Unempr/FeatureServer", prefix = "unempr", transform = identity),
    hs = list(url = "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Lesshs/FeatureServer", prefix = "lesshs", transform = function(x) ifelse(is.na(x), NA_real_, 100 - x))
  )

  print("Loading BNIA ArcGIS service metrics...")

  metric_tables <- imap(service_specs, function(spec, metric) {
    fetch_bnia_service_metric(
      metric = metric,
      service_url = spec$url,
      field_prefix = spec$prefix,
      csa_lookup = csa_lookup,
      years = years,
      transform = spec$transform
    )
  })

  available_metrics <- names(metric_tables)[map_lgl(metric_tables, ~ nrow(.x) > 0)]
  if (!length(available_metrics)) {
    print("No BNIA ArcGIS service metrics were imported.")
    return(list(
      data = tibble(CSA = character(), CSA_key = character()),
      metrics = character(),
      sources = character()
    ))
  }

  merged <- reduce(
    metric_tables[available_metrics],
    function(x, y) full_join(x, y, by = c("CSA", "CSA_key"))
  ) %>%
    mutate(CSA = coalesce(CSA, csa_lookup$CSA[match(CSA_key, csa_lookup$CSA_key)]))

  print(
    paste(
      "  Imported",
      length(available_metrics),
      "metrics from BNIA ArcGIS services:",
      paste(available_metrics, collapse = ", ")
    )
  )

  list(
    data = merged,
    metrics = available_metrics,
    sources = map_chr(service_specs[available_metrics], "url")
  )
}

parse_geolocation_point <- function(x) {
  txt <- str_squish(as.character(x))
  txt[txt %in% c("", "NA", "N/A", "null")] <- NA_character_

  out <- tibble(
    Longitude = rep(NA_real_, length(txt)),
    Latitude = rep(NA_real_, length(txt))
  )

  point_idx <- !is.na(txt) & str_detect(txt, "^POINT\\s*\\(")
  if (any(point_idx)) {
    coords <- str_match(txt[point_idx], "^POINT\\s*\\(([-0-9\\.]+)\\s+([-0-9\\.]+)\\)$")
    out$Longitude[point_idx] <- suppressWarnings(as.numeric(coords[, 2]))
    out$Latitude[point_idx] <- suppressWarnings(as.numeric(coords[, 3]))
  }

  tuple_idx <- !is.na(txt) & str_detect(txt, "^\\(")
  if (any(tuple_idx)) {
    coords <- str_match(txt[tuple_idx], "^\\(([-0-9\\.]+),\\s*([-0-9\\.]+)\\)$")
    out$Latitude[tuple_idx] <- suppressWarnings(as.numeric(coords[, 2]))
    out$Longitude[tuple_idx] <- suppressWarnings(as.numeric(coords[, 3]))
  }

  out
}

fetch_cdc_csv <- function(dataset_id, query = list()) {
  endpoint <- paste0("https://data.cdc.gov/resource/", dataset_id, ".csv")
  response <- GET(endpoint, query = query, timeout(120))
  stop_for_status(response)

  txt <- content(response, "text", encoding = "UTF-8")
  readr::read_csv(I(txt), show_col_types = FALSE, progress = FALSE)
}

normalize_component_scores <- function(values, inverse = FALSE) {
  scores <- rep(NA_real_, length(values))
  usable <- !is.na(values)

  if (!any(usable)) {
    return(scores)
  }

  observed <- values[usable]
  if (dplyr::n_distinct(observed) <= 1) {
    scores[usable] <- 50
    return(scores)
  }

  scaled <- (observed - min(observed, na.rm = TRUE)) /
    (max(observed, na.rm = TRUE) - min(observed, na.rm = TRUE)) * 100

  if (inverse) {
    scaled <- 100 - scaled
  }

  scores[usable] <- round(scaled, 1)
  scores
}

derive_health_index_series <- function(df, years = 2016:2023) {
  component_metrics <- c("le", "as", "la", "va", "pv", "un", "hs")
  inverse_metrics <- c("as", "la", "va", "pv", "un")

  available_components <- intersect(component_metrics, names(df))
  if (!length(available_components)) {
    return(rep(list(rep(NA_real_, length(years))), nrow(df)))
  }

  out <- replicate(nrow(df), rep(NA_real_, length(years)), simplify = FALSE)

  for (idx in seq_along(years)) {
    component_values <- map_dfc(available_components, function(metric) {
      tibble(!!metric := map_dbl(df[[metric]], function(series) {
        value <- series[[idx]]
        if (is.null(value) || is.na(value)) return(NA_real_)
        as.numeric(value)
      }))
    })

    scaled_values <- map_dfc(available_components, function(metric) {
      tibble(!!metric := normalize_component_scores(
        component_values[[metric]],
        inverse = metric %in% inverse_metrics
      ))
    })

    year_scores <- apply(as.matrix(scaled_values), 1, function(row_values) {
      usable <- row_values[!is.na(row_values)]
      if (length(usable) < 4) return(NA_real_)
      round(mean(usable), 1)
    })

    for (row_idx in seq_len(nrow(df))) {
      out[[row_idx]][idx] <- year_scores[[row_idx]]
    }
  }

  out
}

load_cdc_asthma_longitudinal <- function(csa_boundaries, years = 2016:2023) {
  release_specs <- list(
    `2016` = list(dataset_id = "k25u-mg9b", schema = "legacy_500cities", year = 2016),
    `2017` = list(dataset_id = "k86t-wghb", schema = "legacy_500cities", year = 2017),
    `2018` = list(dataset_id = "4ai3-zynv", schema = "places", year = 2018),
    `2019` = list(dataset_id = "373s-ayzu", schema = "places", year = 2019),
    `2020` = list(dataset_id = "nw2y-v4gm", schema = "places", year = 2020),
    `2021` = list(dataset_id = "em5e-5hvn", schema = "places", year = 2021),
    `2022` = list(dataset_id = "ai6z-tcin", schema = "places", year = 2022),
    `2023` = list(dataset_id = "cwsq-ngmh", schema = "places", year = 2023)
  )

  csa_lookup <- csa_boundaries %>%
    st_drop_geometry() %>%
    distinct(CSA, CSA_key)

  print("Loading CDC tract-based asthma prevalence proxy...")

  requested_specs <- release_specs[intersect(as.character(years), names(release_specs))]

  asthma_points <- imap_dfr(requested_specs, function(spec, year_key) {
    raw <- tryCatch(
      {
        if (identical(spec$schema, "legacy_500cities")) {
          fetch_cdc_csv(
            spec$dataset_id,
            query = c(
              list(
                stateabbr = "MD",
                placefips = "2404000",
                `$limit` = 5000
              ),
              setNames(
                list("tractfips,population2010,casthma_crudeprev,geolocation"),
                "$select"
              )
            )
          )
        } else {
          fetch_cdc_csv(
            spec$dataset_id,
            query = list(
              stateabbr = "MD",
              countyfips = "24510",
              measureid = "CASTHMA",
              datavaluetypeid = "CrdPrv",
              `$limit` = 5000
            )
          )
        }
      },
      error = function(e) {
        warning(
          paste(
            "CDC asthma import failed for",
            spec$year,
            "from dataset",
            spec$dataset_id,
            ":",
            conditionMessage(e)
          )
        )
        tibble()
      }
    )

    if (!nrow(raw)) {
      return(tibble(
        Year = integer(),
        tract_id = character(),
        weight = numeric(),
        value = numeric(),
        Longitude = numeric(),
        Latitude = numeric()
      ))
    }

    enriched_raw <- bind_cols(raw, parse_geolocation_point(col_or_default(raw, "geolocation", NA_character_)))

    if (identical(spec$schema, "legacy_500cities")) {
      tibble(
        Year = rep(spec$year, nrow(enriched_raw)),
        tract_id = as.character(col_or_default(enriched_raw, "tractfips", NA_character_)),
        weight = coerce_numeric_value(col_or_default(enriched_raw, "population2010", NA_real_)),
        value = coerce_numeric_value(col_or_default(enriched_raw, "casthma_crudeprev", NA_real_)),
        Longitude = suppressWarnings(as.numeric(col_or_default(enriched_raw, "Longitude", NA_real_))),
        Latitude = suppressWarnings(as.numeric(col_or_default(enriched_raw, "Latitude", NA_real_)))
      )
    } else {
      tibble(
        Year = parse_year_value(col_or_default(enriched_raw, "year", spec$year)),
        tract_id = as.character(col_or_default(enriched_raw, "locationid", NA_character_)),
        weight = coalesce(
          coerce_numeric_value(col_or_default(enriched_raw, "totalpop18plus", NA_real_)),
          coerce_numeric_value(col_or_default(enriched_raw, "totalpopulation", NA_real_))
        ),
        value = coerce_numeric_value(col_or_default(enriched_raw, "data_value", NA_real_)),
        Longitude = suppressWarnings(as.numeric(col_or_default(enriched_raw, "Longitude", NA_real_))),
        Latitude = suppressWarnings(as.numeric(col_or_default(enriched_raw, "Latitude", NA_real_)))
      ) %>%
        filter(Year == spec$year)
    }
  })

  asthma_points <- asthma_points %>%
    filter(
      Year %in% years,
      !is.na(value),
      !is.na(Longitude),
      !is.na(Latitude)
    ) %>%
    mutate(weight = if_else(is.na(weight) | weight <= 0, 1, weight))

  if (!nrow(asthma_points)) {
    print("No CDC asthma proxy records were imported.")
    return(list(
      data = tibble(CSA = character(), CSA_key = character()),
      metrics = character(),
      sources = character()
    ))
  }

  asthma_csa <- asthma_points %>%
    st_as_sf(coords = c("Longitude", "Latitude"), crs = 4326) %>%
    st_join(csa_boundaries %>% select(CSA, CSA_key), join = st_intersects, left = FALSE) %>%
    st_drop_geometry() %>%
    group_by(CSA, CSA_key, Year) %>%
    summarize(
      as_value = round(weighted.mean(value, weight, na.rm = TRUE), 1),
      .groups = "drop"
    )

  asthma_series <- tidyr::crossing(
    csa_lookup,
    Year = years
  ) %>%
    left_join(asthma_csa, by = c("CSA", "CSA_key", "Year")) %>%
    arrange(CSA, Year) %>%
    group_by(CSA, CSA_key) %>%
    summarize(as = list(as.numeric(as_value)), .groups = "drop")

  print(
    paste(
      "  Imported CDC asthma proxy for",
      nrow(asthma_series),
      "CSAs across",
      length(unique(asthma_points$Year)),
      "years."
    )
  )

  list(
    data = asthma_series,
    metrics = "as",
    sources = map_chr(requested_specs, ~ paste0("https://data.cdc.gov/resource/", .x$dataset_id, ".csv"))
  )
}

merge_benchmark_tables <- function(tables) {
  usable <- tables[map_lgl(tables, ~ !is.null(.x) && nrow(.x) > 0)]
  if (!length(usable)) {
    return(tibble(Year = integer()))
  }
  reduce(usable, full_join, by = "Year")
}

year_benchmark_record <- function(tbl, year) {
  row <- tbl %>% filter(Year == year)
  if (!nrow(row)) {
    return(list())
  }

  record <- as.list(row[1, setdiff(names(row), "Year"), drop = FALSE])
  record[map_lgl(record, ~ length(.x) == 1 && !is.na(.x))]
}

empty_benchmark_import <- function() {
  list(
    state = tibble(Year = integer()),
    federal = tibble(Year = integer()),
    metrics = character(),
    sources = character()
  )
}

load_acs_benchmark_series <- function(years = 2016:2023, has_census_key = FALSE) {
  if (!has_census_key) {
    return(empty_benchmark_import())
  }

  lesshs_ids <- sprintf("B15003_%03d", 2:16)
  acs_vars <- c(
    poverty_num = "B17001_002",
    poverty_den = "B17001_001",
    ed_total = "B15003_001",
    setNames(lesshs_ids, paste0("lesshs_", seq_along(lesshs_ids))),
    housing_total = "B25002_001",
    housing_vacant = "B25002_003"
  )

  summarize_acs_benchmark <- function(df, year) {
    lesshs_cols <- grep("^lesshs_\\d+E$", names(df), value = TRUE)

    tibble(
      Year = year,
      pv = round((df$poverty_numE / df$poverty_denE) * 100, 1),
      hs = round(100 - ((rowSums(df[, lesshs_cols, drop = FALSE], na.rm = TRUE) / df$ed_totalE) * 100), 1),
      va = round((df$housing_vacantE / df$housing_totalE) * 100, 1)
    )
  }

  fetch_acs_level <- function(level = c("state", "federal")) {
    level <- match.arg(level)

    map_dfr(years, function(year) {
      tryCatch(
        {
          raw <- if (identical(level, "state")) {
            get_acs(
              geography = "state",
              variables = acs_vars,
              state = "MD",
              year = year,
              survey = "acs5",
              geometry = FALSE,
              output = "wide"
            )
          } else {
            get_acs(
              geography = "us",
              variables = acs_vars,
              year = year,
              survey = "acs5",
              geometry = FALSE,
              output = "wide"
            )
          }

          summarize_acs_benchmark(raw, year)
        },
        error = function(e) {
          warning(paste("ACS", level, "benchmark refresh failed for", year, ":", conditionMessage(e)))
          tibble(Year = year, pv = NA_real_, hs = NA_real_, va = NA_real_)
        }
      )
    })
  }

  state_tbl <- fetch_acs_level("state")
  federal_tbl <- fetch_acs_level("federal")

  list(
    state = state_tbl %>% filter(if_any(-Year, ~ !is.na(.x))),
    federal = federal_tbl %>% filter(if_any(-Year, ~ !is.na(.x))),
    metrics = c("pv", "hs", "va"),
    sources = c(
      state = "ACS 5-year Maryland state estimates",
      federal = "ACS 5-year United States estimates"
    )
  )
}

load_fred_unemployment_benchmarks <- function(years = 2016:2023) {
  fetch_fred_yearly <- function(series_id) {
    response <- GET(
      paste0("https://fred.stlouisfed.org/graph/fredgraph.csv?id=", series_id),
      timeout(120)
    )
    stop_for_status(response)

    txt <- content(response, "text", encoding = "UTF-8")
    raw <- readr::read_csv(I(txt), show_col_types = FALSE, progress = FALSE)
    value_col <- setdiff(names(raw), "observation_date")[1]

    raw %>%
      transmute(
        Year = lubridate::year(as.Date(observation_date)),
        value = coerce_numeric_value(.data[[value_col]])
      ) %>%
      filter(Year %in% years, !is.na(value)) %>%
      group_by(Year) %>%
      summarize(un = round(mean(value, na.rm = TRUE), 1), .groups = "drop")
  }

  state_tbl <- tryCatch(
    fetch_fred_yearly("MDUR"),
    error = function(e) {
      warning(paste("FRED Maryland unemployment refresh failed:", conditionMessage(e)))
      tibble(Year = integer(), un = numeric())
    }
  )

  federal_tbl <- tryCatch(
    fetch_fred_yearly("UNRATE"),
    error = function(e) {
      warning(paste("FRED national unemployment refresh failed:", conditionMessage(e)))
      tibble(Year = integer(), un = numeric())
    }
  )

  list(
    state = state_tbl,
    federal = federal_tbl,
    metrics = "un",
    sources = c(
      state = "https://fred.stlouisfed.org/series/MDUR",
      federal = "https://fred.stlouisfed.org/series/UNRATE"
    )
  )
}

load_cdc_asthma_benchmark_series <- function(years = 2016:2023) {
  release_specs <- list(
    `2018` = list(dataset_id = "4ai3-zynv", year = 2018),
    `2019` = list(dataset_id = "373s-ayzu", year = 2019),
    `2020` = list(dataset_id = "nw2y-v4gm", year = 2020),
    `2021` = list(dataset_id = "em5e-5hvn", year = 2021),
    `2022` = list(dataset_id = "ai6z-tcin", year = 2022),
    `2023` = list(dataset_id = "cwsq-ngmh", year = 2023)
  )

  fetch_weighted_proxy <- function(dataset_id, stateabbr = NULL) {
    query <- c(
      list(
        measureid = "CASTHMA",
        datavaluetypeid = "CrdPrv"
      ),
      if (!is.null(stateabbr)) list(stateabbr = stateabbr) else list(),
      setNames(
        list("sum(totalpopulation) as pop,sum(totalpopulation*data_value) as weighted"),
        "$select"
      )
    )

    raw <- fetch_cdc_csv(dataset_id, query = query)
    if (!nrow(raw)) {
      return(NA_real_)
    }

    pop <- coerce_numeric_value(raw$pop[[1]])
    weighted <- coerce_numeric_value(raw$weighted[[1]])
    if (is.na(pop) || pop <= 0 || is.na(weighted)) {
      return(NA_real_)
    }

    round(weighted / pop, 1)
  }

  requested_specs <- release_specs[intersect(as.character(years), names(release_specs))]

  rows <- imap_dfr(requested_specs, function(spec, year_key) {
    state_val <- tryCatch(
      fetch_weighted_proxy(spec$dataset_id, stateabbr = "MD"),
      error = function(e) {
        warning(paste("CDC asthma state benchmark failed for", spec$year, ":", conditionMessage(e)))
        NA_real_
      }
    )

    federal_val <- tryCatch(
      fetch_weighted_proxy(spec$dataset_id, stateabbr = NULL),
      error = function(e) {
        warning(paste("CDC asthma federal benchmark failed for", spec$year, ":", conditionMessage(e)))
        NA_real_
      }
    )

    tibble(
      Year = spec$year,
      as_state = state_val,
      as_federal = federal_val
    )
  })

  list(
    state = rows %>% transmute(Year, as = as_state) %>% filter(!is.na(as)),
    federal = rows %>% transmute(Year, as = as_federal) %>% filter(!is.na(as)),
    metrics = "as",
    sources = map_chr(requested_specs, ~ paste0("https://data.cdc.gov/resource/", .x$dataset_id, ".csv"))
  )
}

load_cdc_life_expectancy_benchmark_series <- function(years = 2016:2023) {
  csv_url <- "https://www.cdc.gov/nchs/data-visualization/state-life-expectancy/data/USLT-2018-2022.csv"
  federal_fill_values <- tribble(
    ~Year, ~le,
    2016L, 78.7,
    2017L, 78.6,
    2023L, 78.4
  )

  raw <- tryCatch(
    {
      response <- GET(csv_url, timeout(120))
      stop_for_status(response)
      txt <- content(response, "text", encoding = "UTF-8")
      readr::read_csv(I(txt), show_col_types = FALSE, progress = FALSE)
    },
    error = function(e) {
      warning(paste("CDC life expectancy benchmark refresh failed:", conditionMessage(e)))
      tibble()
    }
  )

  if (!nrow(raw)) {
    return(empty_benchmark_import())
  }

  cleaned <- raw %>%
    transmute(
      Year = parse_year_value(col_or_default(raw, "Year", NA_integer_)),
      State = str_squish(as.character(col_or_default(raw, "State", NA_character_))),
      Sex = str_squish(as.character(col_or_default(raw, "Sex", NA_character_))),
      le = coerce_numeric_value(col_or_default(raw, "LEB", NA_real_))
    ) %>%
    filter(
      Year %in% years,
      str_to_lower(Sex) == "total",
      !is.na(le)
    )

  federal_tbl <- bind_rows(
    cleaned %>%
      filter(State == "United States") %>%
      transmute(Year, le),
    federal_fill_values %>% filter(Year %in% years)
  ) %>%
    arrange(Year) %>%
    distinct(Year, .keep_all = TRUE)

  list(
    state = cleaned %>%
      filter(State == "Maryland") %>%
      transmute(Year, le),
    federal = federal_tbl,
    metrics = "le",
    sources = c(
      state = csv_url,
      federal = paste(
        "CDC U.S. State Life Expectancy by Sex, 2018-2022 CSV plus",
        "United States Life Tables 2016, 2017, and 2023 official totals"
      )
    )
  )
}

load_cdc_lead_benchmark_series <- function(years = 2016:2023) {
  workbook_url <- "https://www.cdc.gov/lead-prevention/media/files/2025/08/2017-2022-cbls-national-data-508-1.xlsx"

  if (!requireNamespace("readxl", quietly = TRUE)) {
    warning("readxl is required for CDC lead benchmark imports. Install readxl to enable Maryland `la` benchmarks.")
    return(empty_benchmark_import())
  }

  tmp_path <- tempfile(fileext = ".xlsx")
  on.exit(unlink(tmp_path), add = TRUE)

  raw <- tryCatch(
    {
      response <- GET(workbook_url, write_disk(tmp_path, overwrite = TRUE), timeout(120))
      stop_for_status(response)
      readxl::read_excel(tmp_path, skip = 2)
    },
    error = function(e) {
      warning(paste("CDC lead benchmark refresh failed:", conditionMessage(e)))
      tibble()
    }
  )

  if (!nrow(raw)) {
    return(empty_benchmark_import())
  }

  normalized_cols <- tibble(
    source_col = names(raw),
    normalized = normalize_name(names(raw))
  )

  year_col <- normalized_cols$source_col[normalized_cols$normalized == "year"][1]
  state_col <- normalized_cols$source_col[normalized_cols$normalized == "state"][1]
  pct5_col <- normalized_cols$source_col[
    normalized_cols$normalized == normalize_name("Percent of Children with Confirmed BLLs ≥5 µg/dL")
  ][1]

  if (any(is.na(c(year_col, state_col, pct5_col)))) {
    warning("CDC lead benchmark workbook schema changed unexpectedly; skipping Maryland `la` benchmark refresh.")
    return(empty_benchmark_import())
  }

  maryland_tbl <- raw %>%
    transmute(
      Year = parse_year_value(.data[[year_col]]),
      State = str_squish(as.character(.data[[state_col]])),
      pct5 = as.character(.data[[pct5_col]])
    ) %>%
    filter(
      State == "Maryland",
      Year %in% years
    ) %>%
    transmute(
      Year,
      la = coerce_numeric_value(pct5)
    ) %>%
    filter(!is.na(la))

  list(
    state = maryland_tbl,
    federal = tibble(Year = integer()),
    metrics = "la",
    sources = c(
      state = workbook_url
    )
  )
}

has_series_data <- function(x) {
  if (is.null(x)) return(FALSE)
  values <- suppressWarnings(as.numeric(unlist(x, use.names = FALSE)))
  length(values) > 0 && any(!is.na(values))
}

prefer_metric_input <- function(primary, fallback) {
  if (has_series_data(primary)) primary else fallback
}

ensure_metric_column <- function(df, metric) {
  if (!metric %in% names(df)) {
    df[[metric]] <- rep(list(NA_real_), nrow(df))
  }
  df
}

empty_311_tbl <- tibble(
  SRType = character(),
  SRStatus = character(),
  CreatedDate = numeric(),
  Longitude = numeric(),
  Latitude = numeric()
)

coerce_311_schema <- function(df) {
  if (is.null(df) || nrow(df) == 0) {
    return(empty_311_tbl)
  }

  df <- as_tibble(df)

  if (!"SRType" %in% names(df)) df$SRType <- NA_character_
  if (!"SRStatus" %in% names(df)) df$SRStatus <- NA_character_
  if (!"CreatedDate" %in% names(df)) df$CreatedDate <- NA_real_
  if (!"Longitude" %in% names(df)) df$Longitude <- NA_real_
  if (!"Latitude" %in% names(df)) df$Latitude <- NA_real_

  df %>%
    transmute(
      SRType = as.character(SRType),
      SRStatus = as.character(SRStatus),
      CreatedDate = as.numeric(CreatedDate),
      Longitude = as.numeric(Longitude),
      Latitude = as.numeric(Latitude)
    )
}

parse_arcgis_batch <- function(txt) {
  parsed_fast <- tryCatch(fromJSON(txt), error = function(e) NULL)

  if (
    !is.null(parsed_fast) &&
    !is.null(parsed_fast$features) &&
    is.data.frame(parsed_fast$features) &&
    "attributes" %in% names(parsed_fast$features)
  ) {
    attrs <- parsed_fast$features$attributes
    geoms <- parsed_fast$features$geometry
    
    # Extract the GPS coordinates from the spatial JSON
    if (!is.null(geoms) && "x" %in% names(geoms) && "y" %in% names(geoms)) {
      attrs$Longitude <- geoms$x
      attrs$Latitude <- geoms$y
    }
    return(coerce_311_schema(attrs))
  }

  parsed_safe <- tryCatch(fromJSON(txt, simplifyVector = FALSE), error = function(e) NULL)

  if (!is.null(parsed_safe) && !is.null(parsed_safe$features) && length(parsed_safe$features) > 0) {
    rows <- purrr::map_dfr(parsed_safe$features, function(feature) {
      attr <- feature$attributes %||% list()
      geom <- feature$geometry %||% list()

      tibble(
        SRType = if (is.null(attr$SRType)) NA_character_ else as.character(attr$SRType),
        SRStatus = if (is.null(attr$SRStatus)) NA_character_ else as.character(attr$SRStatus),
        CreatedDate = if (is.null(attr$CreatedDate)) NA_real_ else as.numeric(attr$CreatedDate),
        Longitude = if (is.null(geom$x)) NA_real_ else as.numeric(geom$x),
        Latitude = if (is.null(geom$y)) NA_real_ else as.numeric(geom$y)
      )
    })
    return(coerce_311_schema(rows))
  }

  empty_311_tbl
}

# 1. SETUP: Authenticate Census API
census_key <- trimws(Sys.getenv("CENSUS_API_KEY"))
has_census_key <- !identical(census_key, "")
if (has_census_key) {
  Sys.setenv(CENSUS_API_KEY = census_key)
} else {
  warning("CENSUS_API_KEY is not set. ACS poverty refresh will be skipped.")
}

# 2. EXTRACT: Fetch Spatial Boundaries (CSAs)
print("Loading Local CSA Boundaries...")
# Accept either the canonical name or the original upload name
geojson_path <- if (file.exists("csa_boundaries.geojson")) {
  "csa_boundaries.geojson"
} else if (file.exists("csa_boundaries_geojson__1_.geojson")) {
  "csa_boundaries_geojson__1_.geojson"
} else {
  stop("Missing CSA boundaries GeoJSON. Expected csa_boundaries.geojson in repo root.")
}

csa_boundaries <- st_read(geojson_path, quiet = TRUE) %>%
  st_transform(crs = 4326)

# Auto-detect BNIA's CSA name column (Community, CSA2010, CSA2020, or Name)
csa_col <- names(csa_boundaries)[str_detect(str_to_lower(names(csa_boundaries)), "^community$|csa2010|csa2020|csa_name|^name$")][1]
if(is.na(csa_col)) stop("Could not detect the CSA name column in the geojson.")

csa_boundaries <- csa_boundaries %>%
  rename(CSA = !!sym(csa_col)) %>%
  select(CSA, geometry) %>%
  mutate(
    CSA = trimws(CSA),
    CSA_key = normalize_name(CSA)
  )

csa_lookup <- csa_boundaries %>%
  st_drop_geometry() %>%
  distinct(CSA, CSA_key)

bnia_import <- load_bnia_longitudinal(csa_lookup, years = years)
bnia_service_import <- load_bnia_service_longitudinal(csa_lookup, years = years)
cdc_asthma_import <- load_cdc_asthma_longitudinal(csa_boundaries, years = years)

# 3. EXTRACT & TRANSFORM: Historical 311 Environmental Hazards
print("Fetching historical 311 data with GPS coordinates...")

fetch_arcgis_layer <- function(service_url, out_fields) {
  meta_response <- GET(service_url, query = list(f = "json"), timeout(60))
  stop_for_status(meta_response)

  meta_text <- content(meta_response, "text", encoding = "UTF-8")
  meta <- tryCatch(fromJSON(meta_text), error = function(e) NULL)

  page_size <- if (!is.null(meta$maxRecordCount)) meta$maxRecordCount else 2000L
  page_size <- min(as.integer(page_size), 2000L)
  offset <- 0L
  batches <- list()

  repeat {
    response <- GET(
      service_url,
      query = list(
        where = "1=1",
        outFields = paste(out_fields, collapse = ","),
        returnGeometry = "true", # FORCE SERVER TO SEND GPS POINTS
        outSR = "4326",
        orderByFields = "RowID ASC",
        resultOffset = offset,
        resultRecordCount = page_size,
        f = "json"
      ),
      timeout(120)
    )
    stop_for_status(response)
    txt <- content(response, "text", encoding = "UTF-8")
    batch <- parse_arcgis_batch(txt)

    if (nrow(batch) == 0) break
    batches[[length(batches) + 1L]] <- batch
    if (!str_detect(txt, '"exceededTransferLimit"\\s*:\\s*true')) break
    offset <- offset + nrow(batch)
  }

  if (length(batches) == 0) return(empty_311_tbl)
  dplyr::bind_rows(batches)
}

three11_sources <- c(
  "2016" = "https://services1.arcgis.com/UWYHeuuJISiGmgXx/ArcGIS/rest/services/311_Customer_Service_Requests_Yearly/FeatureServer/6",
  "2017" = "https://services1.arcgis.com/UWYHeuuJISiGmgXx/ArcGIS/rest/services/311_Customer_Service_Requests_Yearly/FeatureServer/5",
  "2018" = "https://services1.arcgis.com/UWYHeuuJISiGmgXx/ArcGIS/rest/services/311_Customer_Service_Requests_Yearly/FeatureServer/4",
  "2019" = "https://services1.arcgis.com/UWYHeuuJISiGmgXx/ArcGIS/rest/services/311_Customer_Service_Requests_Yearly/FeatureServer/3",
  "2020" = "https://services1.arcgis.com/UWYHeuuJISiGmgXx/ArcGIS/rest/services/311_Customer_Service_Requests_Yearly/FeatureServer/2",
  "2021" = "https://services1.arcgis.com/UWYHeuuJISiGmgXx/ArcGIS/rest/services/311_Customer_Service_Requests_Yearly/FeatureServer/1",
  "2022" = "https://services1.arcgis.com/UWYHeuuJISiGmgXx/ArcGIS/rest/services/311_Customer_Service_Requests_Yearly/FeatureServer/0",
  "2023" = "https://services1.arcgis.com/UWYHeuuJISiGmgXx/ArcGIS/rest/services/311_Customer_Service_Requests_2023/FeatureServer/0"
)

three11_batches <- vector("list", length(three11_sources))
names(three11_batches) <- names(three11_sources)

for (year_label in names(three11_sources)) {
  print(paste("  Downloading 311 records for", year_label, "..."))
  three11_batches[[year_label]] <- tryCatch(
    fetch_arcgis_layer(
      three11_sources[[year_label]],
      out_fields = c("SRType", "SRStatus", "CreatedDate")
    ),
    error = function(e) {
      warning(paste("311 import failed for", year_label, ":", conditionMessage(e)))
      empty_311_tbl
    }
  )
}

raw_311 <- dplyr::bind_rows(three11_batches) %>% coerce_311_schema()

# The Spatial Intersection: Map exact GPS points to BNIA CSA Polygons
if (nrow(raw_311) > 0) {
  csa_311_clean <- raw_311 %>%
    filter(!is.na(CreatedDate), !is.na(Longitude), !is.na(Latitude)) %>%
    mutate(Year = lubridate::year(lubridate::as_datetime(CreatedDate / 1000, tz = "UTC"))) %>%
    st_as_sf(coords = c("Longitude", "Latitude"), crs = 4326) %>%
    st_join(csa_boundaries, join = st_intersects) %>%
    st_drop_geometry() %>%
    filter(!is.na(CSA), Year >= min(years), Year <= max(years), SRStatus != "Closed (Duplicate)") %>%
    transmute(
      CSA = CSA,
      CSA_key = CSA_key,
      Category = case_when(
        str_detect(str_to_lower(SRType), "rat|rodent") ~ "rt",
        str_detect(str_to_lower(SRType), "trash|dumping") ~ "dp",
        str_detect(str_to_lower(SRType), "water|sewer") ~ "ws",
        TRUE ~ "other"
      ),
      Year = Year
    ) %>%
    filter(Category != "other")
} else {
  csa_311_clean <- tibble(
    CSA = character(),
    CSA_key = character(),
    Category = character(),
    Year = integer()
  )
}

has_311_data <- nrow(csa_311_clean) > 0

csa_311_yearly <- csa_311_clean %>%
  group_by(CSA, CSA_key, Category, Year) %>%
  summarize(Total = n(), .groups = "drop")

total_hazards <- csa_311_yearly %>%
  group_by(CSA, CSA_key, Year) %>%
  summarize(Total = sum(Total), .groups = "drop") %>%
  mutate(Category = "hz")

all_311_yearly <- bind_rows(csa_311_yearly, total_hazards)

complete_grid <- tidyr::crossing(
  csa_boundaries %>% st_drop_geometry() %>% select(CSA, CSA_key),
  Category = c("rt", "dp", "ws", "hz"),
  Year = years
)

csa_311_arrays <- complete_grid %>%
  left_join(all_311_yearly, by = c("CSA", "CSA_key", "Category", "Year")) %>%
  mutate(Total = replace_na(Total, 0)) %>%
  arrange(CSA, Category, Year) %>%
  group_by(CSA, CSA_key, Category) %>%
  summarize(yearly_array = list(as.numeric(Total)), .groups = "drop") %>%
  pivot_wider(names_from = Category, values_from = yearly_array)

# 4. EXTRACT & TRANSFORM: ACS Demographic Baselines
print("Fetching and processing Census data...")
acs_vars <- c(Poverty = "B17001_002", Total_Pop = "B17001_001")

if (has_census_key) {
  csa_acs_summary <- tryCatch(
    {
      baltimore_acs <- get_acs(
        geography = "tract",
        variables = acs_vars,
        state = "MD",
        county = "Baltimore city",
        year = 2022,
        geometry = TRUE,
        output = "wide"
      ) %>%
        st_transform(crs = 4326) %>%
        mutate(
          pv = round((PovertyE / Total_PopE) * 100, 1),
          pv = replace_na(pv, 0)
        ) %>%
        select(GEOID, pv, geometry)

      st_intersection(
        csa_boundaries %>% select(CSA, CSA_key),
        baltimore_acs
      ) %>%
        mutate(intersect_area = st_area(geometry)) %>%
        st_drop_geometry() %>%
        group_by(CSA, CSA_key) %>%
        summarize(
          pv = round(weighted.mean(pv, as.numeric(intersect_area), na.rm = TRUE), 1),
          .groups = "drop"
        )
    },
    error = function(e) {
      warning(paste("ACS poverty refresh failed:", conditionMessage(e)))
      tibble(CSA = character(), CSA_key = character(), pv = numeric())
    }
  )
} else {
  csa_acs_summary <- tibble(CSA = character(), CSA_key = character(), pv = numeric())
}

print("Fetching state and federal benchmark series...")
acs_benchmark_import <- load_acs_benchmark_series(years = years, has_census_key = has_census_key)
fred_benchmark_import <- load_fred_unemployment_benchmarks(years = years)
cdc_benchmark_import <- load_cdc_asthma_benchmark_series(years = years)
cdc_life_expectancy_benchmark_import <- load_cdc_life_expectancy_benchmark_series(years = years)
cdc_lead_benchmark_import <- load_cdc_lead_benchmark_series(years = years)

state_benchmark_series <- merge_benchmark_tables(list(
  acs_benchmark_import$state,
  fred_benchmark_import$state,
  cdc_benchmark_import$state,
  cdc_life_expectancy_benchmark_import$state,
  cdc_lead_benchmark_import$state
))

federal_benchmark_series <- merge_benchmark_tables(list(
  acs_benchmark_import$federal,
  fred_benchmark_import$federal,
  cdc_benchmark_import$federal,
  cdc_life_expectancy_benchmark_import$federal,
  cdc_lead_benchmark_import$federal
))

legacy_rates <- c(
  hi = 0.4, le = 0.08, as = -0.6, la = -0.5, va = -0.2, pv = -0.3,
  un = -0.25, hs = 0.15, fd = 0.3, gs = 0.1, hw = -0.2, cr = -0.5,
  "in" = 0.8, tp = 0.2, dp = 0.4, rt = 0.25, ws = 0.3, hz = 0.45
)
benchmark_metrics <- c("hi", "le", "as", "la", "va", "pv", "un", "hs", "fd", "gs", "cr")
inverse_benchmark_metrics <- c("as", "la", "va", "pv", "un", "cr")
benchmark_metric_bounds <- list(
  hi = c(0, 100),
  le = c(0, Inf),
  as = c(0, 100),
  la = c(0, 100),
  va = c(0, 100),
  pv = c(0, 100),
  un = c(0, 100),
  hs = c(0, 100),
  fd = c(0, 100),
  gs = c(0, 100),
  cr = c(0, 100)
)

clamp_benchmark_value <- function(metric, value) {
  if (is.null(value) || is.na(value)) {
    return(value)
  }

  bounds <- benchmark_metric_bounds[[metric]]
  if (is.null(bounds) || length(bounds) != 2) {
    return(round(as.numeric(value), 1))
  }

  lower <- bounds[[1]]
  upper <- bounds[[2]]
  clamped <- max(as.numeric(value), lower)
  if (is.finite(upper)) {
    clamped <- min(clamped, upper)
  }

  round(clamped, 1)
}

metric_series <- function(x, metric, years = 2016:2023) {
  idx <- seq_along(years) - 1

  if (is.null(x)) {
    return(rep(NA_real_, length(years)))
  }

  if (is.list(x) && length(x) == 1) {
    x <- x[[1]]
  }

  if (is.atomic(x) && !is.character(x) && length(x) == length(years)) {
    return(round(as.numeric(x), 1))
  }

  if (is.atomic(x) && !is.character(x) && length(x) == 1 && !is.na(x)) {
    base <- as.numeric(x)
    rate <- if (!is.null(legacy_rates[[metric]])) legacy_rates[[metric]] else 0
    return(round((base + rate * (idx - 7) + sin(idx * 17 + base) * 0.25), 1))
  }

  if (is.atomic(x) && !is.character(x) && length(x) > 1) {
    out <- as.numeric(x)
    length(out) <- length(years)
    return(round(out, 1))
  }

  rep(NA_real_, length(years))
}

extract_current_data <- function(raw, years = 2016:2023) {
  if (!is.null(raw$neighborhoods) && is.list(raw$neighborhoods)) {
    year_keys <- as.character(years)
    metric_names <- raw$neighborhoods %>%
      map(~ .x[intersect(names(.x), year_keys)]) %>%
      map(~ unique(unlist(map(.x, names)))) %>%
      unlist() %>%
      unique()

    rows <- imap(raw$neighborhoods, function(year_records, csa) {
      row <- list(CSA = csa)

      for (metric in metric_names) {
        values <- map_dbl(year_keys, function(year_key) {
          year_record <- year_records[[year_key]]
          if (is.null(year_record) || is.null(year_record[[metric]])) {
            return(NA_real_)
          }
          as.numeric(year_record[[metric]])
        })

        if (!all(is.na(values))) {
          row[[metric]] <- list(values)
        }
      }

      as_tibble(row)
    })

    return(bind_rows(rows) %>% mutate(CSA_key = normalize_name(CSA)))
  }

  if (is.null(names(raw))) {
    stop(paste0(
      "data.json must be either the normalized dashboard object or a named object keyed by Neighborhood. ",
      "Example: {\"Canton\": {\"hi\": 84, \"le\": 80, ...}}"
    ))
  }

  raw <- raw[nchar(names(raw)) > 0]

  tibble(
    CSA = names(raw),
    data = unname(raw)
  ) %>%
    unnest_wider(data) %>%
    mutate(CSA_key = normalize_name(CSA))
}

build_yearly_entry <- function(row, metric_cols, years = 2016:2023) {
  year_keys <- as.character(years)
  out <- set_names(vector("list", length(year_keys)), year_keys)

  for (idx in seq_along(years)) {
    year_record <- list()

    for (metric in metric_cols) {
      series <- row[[metric]][[1]]
      value <- if (length(series) >= idx) series[[idx]] else NA_real_
      if (!is.na(value)) year_record[[metric]] <- round(as.numeric(value), 1)
    }

    out[[idx]] <- year_record
  }

  out
}

build_fallback_level_record <- function(city_record, level = c("state", "federal")) {
  level <- match.arg(level)
  step <- if (identical(level, "state")) 1 else 2
  out <- list()

  for (metric in benchmark_metrics) {
    value <- city_record[[metric]]
    if (is.null(value) || is.na(value)) next

    delta <- if (metric %in% inverse_benchmark_metrics) {
      -0.8 * step
    } else {
      0.8 * step
    }

    out[[metric]] <- clamp_benchmark_value(metric, as.numeric(value) + delta)
  }

  out
}

derive_benchmark_health_index <- function(neighborhood_data, benchmark_record, year_index) {
  component_metrics <- c("le", "as", "la", "va", "pv", "un", "hs")
  inverse_metrics <- c("as", "la", "va", "pv", "un")
  scores <- numeric()

  for (metric in component_metrics) {
    benchmark_value <- benchmark_record[[metric]]
    if (is.null(benchmark_value) || is.na(benchmark_value) || !metric %in% names(neighborhood_data)) {
      next
    }

    neighborhood_values <- map_dbl(neighborhood_data[[metric]], function(series) {
      value <- series[[year_index]]
      if (is.null(value) || is.na(value)) return(NA_real_)
      as.numeric(value)
    })

    usable <- neighborhood_values[!is.na(neighborhood_values)]
    if (!length(usable)) {
      next
    }

    score <- if (dplyr::n_distinct(usable) <= 1) {
      50
    } else {
      scaled <- (as.numeric(benchmark_value) - min(usable, na.rm = TRUE)) /
        (max(usable, na.rm = TRUE) - min(usable, na.rm = TRUE)) * 100
      if (metric %in% inverse_metrics) scaled <- 100 - scaled
      max(0, min(100, scaled))
    }

    scores <- c(scores, round(score, 1))
  }

  if (length(scores) < 3) {
    return(NULL)
  }

  round(mean(scores), 1)
}

# 5. MERGE: Combine Data
base_data_path <- "data.json"

if (!file.exists(base_data_path)) {
  stop("Missing initial data.json to use as a base.")
}

current_data_raw <- read_json(base_data_path, simplifyVector = FALSE)
current_data <- extract_current_data(current_data_raw, years = years)

final_dashboard_data <- current_data

if (nrow(csa_acs_summary) > 0) {
  final_dashboard_data <- final_dashboard_data %>%
    left_join(
      csa_acs_summary %>% select(CSA_key, pv_acs = pv),
      by = "CSA_key"
    )

  final_dashboard_data <- ensure_metric_column(final_dashboard_data, "pv")
  final_dashboard_data$pv <- map2(final_dashboard_data$pv_acs, final_dashboard_data$pv, prefer_metric_input)
  final_dashboard_data <- final_dashboard_data %>% select(-pv_acs)
}

if (has_311_data && nrow(csa_311_arrays) > 0) {
  final_dashboard_data <- final_dashboard_data %>%
    left_join(
      csa_311_arrays %>% rename(rt_311 = rt, dp_311 = dp, ws_311 = ws, hz_311 = hz) %>%
        select(CSA_key, rt_311, dp_311, ws_311, hz_311),
      by = "CSA_key"
    )

  for (metric in c("rt", "dp", "ws", "hz")) {
    final_dashboard_data <- ensure_metric_column(final_dashboard_data, metric)
    override_col <- paste0(metric, "_311")
    final_dashboard_data[[metric]] <- map2(
      final_dashboard_data[[override_col]],
      final_dashboard_data[[metric]],
      prefer_metric_input
    )
  }

  final_dashboard_data <- final_dashboard_data %>% select(-ends_with("_311"))
}

if (nrow(bnia_service_import$data) > 0) {
  service_metric_cols <- setdiff(names(bnia_service_import$data), c("CSA", "CSA_key"))

  final_dashboard_data <- final_dashboard_data %>%
    left_join(
      bnia_service_import$data %>%
        rename_with(~ paste0(.x, "_service"), all_of(service_metric_cols)) %>%
        select(CSA_key, ends_with("_service")),
      by = "CSA_key"
    )

  for (metric in service_metric_cols) {
    final_dashboard_data <- ensure_metric_column(final_dashboard_data, metric)
    override_col <- paste0(metric, "_service")
    final_dashboard_data[[metric]] <- map2(
      final_dashboard_data[[override_col]],
      final_dashboard_data[[metric]],
      prefer_metric_input
    )
  }

  final_dashboard_data <- final_dashboard_data %>% select(-ends_with("_service"))
} else {
  service_metric_cols <- character()
}

if (nrow(cdc_asthma_import$data) > 0) {
  cdc_metric_cols <- setdiff(names(cdc_asthma_import$data), c("CSA", "CSA_key"))

  final_dashboard_data <- final_dashboard_data %>%
    left_join(
      cdc_asthma_import$data %>%
        rename_with(~ paste0(.x, "_cdc"), all_of(cdc_metric_cols)) %>%
        select(CSA_key, ends_with("_cdc")),
      by = "CSA_key"
    )

  for (metric in cdc_metric_cols) {
    final_dashboard_data <- ensure_metric_column(final_dashboard_data, metric)
    override_col <- paste0(metric, "_cdc")
    final_dashboard_data[[metric]] <- map2(
      final_dashboard_data[[override_col]],
      final_dashboard_data[[metric]],
      prefer_metric_input
    )
  }

  final_dashboard_data <- final_dashboard_data %>% select(-ends_with("_cdc"))
} else {
  cdc_metric_cols <- character()
}

if (nrow(bnia_import$data) > 0) {
  bnia_metric_cols <- setdiff(names(bnia_import$data), c("CSA", "CSA_key"))

  final_dashboard_data <- final_dashboard_data %>%
    left_join(
      bnia_import$data %>%
        rename_with(~ paste0(.x, "_bnia"), all_of(bnia_metric_cols)) %>%
        select(CSA_key, ends_with("_bnia")),
      by = "CSA_key"
    )

  for (metric in bnia_metric_cols) {
    final_dashboard_data <- ensure_metric_column(final_dashboard_data, metric)
    override_col <- paste0(metric, "_bnia")
    final_dashboard_data[[metric]] <- map2(
      final_dashboard_data[[override_col]],
      final_dashboard_data[[metric]],
      prefer_metric_input
    )
  }

  final_dashboard_data <- final_dashboard_data %>% select(-ends_with("_bnia"))
} else {
  bnia_metric_cols <- character()
}

metric_cols <- setdiff(names(final_dashboard_data), c("CSA", "CSA_key"))

derive_hi_from_components <- !"hi" %in% bnia_metric_cols

for (metric in setdiff(metric_cols, if (derive_hi_from_components) "hi" else character())) {
  final_dashboard_data[[metric]] <- map(
    final_dashboard_data[[metric]],
    metric_series,
    metric = metric,
    years = years
  )
}

if (derive_hi_from_components) {
  final_dashboard_data <- ensure_metric_column(final_dashboard_data, "hi")
  final_dashboard_data$hi <- derive_health_index_series(final_dashboard_data, years = years)
} else if ("hi" %in% names(final_dashboard_data)) {
  final_dashboard_data$hi <- map(
    final_dashboard_data$hi,
    metric_series,
    metric = "hi",
    years = years
  )
}

metric_cols <- union(setdiff(metric_cols, "hi"), "hi")

# 6. LOAD: Write normalized JSON
print("Formatting and writing normalized output...")

neighborhoods_output <- list()

for (row_idx in seq_len(nrow(final_dashboard_data))) {
  row <- final_dashboard_data[row_idx, ]
  neighborhoods_output[[row$CSA]] <- build_yearly_entry(row, metric_cols, years = years)
}

benchmarks_output <- list(city = list(), state = list(), federal = list())
state_real_benchmark_metrics <- sort(setdiff(names(state_benchmark_series), "Year"))
federal_real_benchmark_metrics <- sort(setdiff(names(federal_benchmark_series), "Year"))
shared_real_benchmark_metrics <- intersect(state_real_benchmark_metrics, federal_real_benchmark_metrics)
state_only_real_benchmark_metrics <- setdiff(state_real_benchmark_metrics, federal_real_benchmark_metrics)
federal_only_real_benchmark_metrics <- setdiff(federal_real_benchmark_metrics, state_real_benchmark_metrics)
real_benchmark_metrics <- sort(union(state_real_benchmark_metrics, federal_real_benchmark_metrics))

for (idx in seq_along(years)) {
  year_key <- as.character(years[[idx]])
  city_record <- list()

  for (metric in metric_cols) {
    values <- map_dbl(final_dashboard_data[[metric]], function(series) {
      value <- series[[idx]]
      if (is.null(value) || is.na(value)) return(NA_real_)
      as.numeric(value)
    })

    usable <- values[!is.na(values)]
    if (length(usable)) {
      city_record[[metric]] <- round(mean(usable), 1)
    }
  }

  benchmarks_output$city[[year_key]] <- city_record

  state_record <- utils::modifyList(
    build_fallback_level_record(city_record, "state"),
    year_benchmark_record(state_benchmark_series, years[[idx]])
  )
  federal_record <- utils::modifyList(
    build_fallback_level_record(city_record, "federal"),
    year_benchmark_record(federal_benchmark_series, years[[idx]])
  )

  state_hi <- derive_benchmark_health_index(final_dashboard_data, state_record, idx)
  federal_hi <- derive_benchmark_health_index(final_dashboard_data, federal_record, idx)

  if (!is.null(state_hi)) {
    state_record$hi <- state_hi
  }

  if (!is.null(federal_hi)) {
    federal_record$hi <- federal_hi
  }

  benchmarks_output$state[[year_key]] <- state_record
  benchmarks_output$federal[[year_key]] <- federal_record
}

json_ready_data <- list(
  meta = list(
    schema_version = 2,
    years = as.list(years),
    provisional = TRUE,
    note = {
      note_parts <- character()

      if (length(bnia_metric_cols) > 0) {
        note_parts <- c(
          note_parts,
          paste0(
            "Real BNIA longitudinal values imported from a local file for: ",
            paste(sort(bnia_metric_cols), collapse = ", "),
            "."
          )
        )
      }

      if (length(service_metric_cols) > 0) {
        note_parts <- c(
          note_parts,
          paste0(
            "BNIA ArcGIS services supplied: ",
            paste(sort(service_metric_cols), collapse = ", "),
            "."
          )
        )
      }

      if ("as" %in% cdc_metric_cols) {
        note_parts <- c(
          note_parts,
          "CDC 500 Cities / PLACES tract releases supplied `as` as an adult current asthma prevalence proxy, aggregated to CSAs using tract centroid assignment and population weighting."
        )
      }

      if (derive_hi_from_components) {
        note_parts <- c(
          note_parts,
          "The dashboard `hi` series is derived from official component metrics (le, as, la, va, pv, un, hs) using yearly min-max normalization and equal weighting."
        )
      }

      if (!length(note_parts)) {
        note_parts <- "No BNIA longitudinal file, CDC asthma proxy, or live BNIA service metrics were imported."
      }

      paste(
        c(
          note_parts,
          "Missing metrics still fall back to existing data.json values and modeled series where needed."
        ),
        collapse = " "
      )
    },
    benchmark_note = if (length(real_benchmark_metrics) > 0) {
      paste(
        c(
          "City benchmarks are derived from Baltimore CSA values.",
          if (length(shared_real_benchmark_metrics) > 0) {
            paste0(
              "State and federal benchmarks use real official series for: ",
              paste(shared_real_benchmark_metrics, collapse = ", "),
              "."
            )
          },
          if (length(state_only_real_benchmark_metrics) > 0) {
            paste0(
              "State-only official benchmark coverage is available for: ",
              paste(state_only_real_benchmark_metrics, collapse = ", "),
              "."
            )
          },
          if (length(federal_only_real_benchmark_metrics) > 0) {
            paste0(
              "Federal-only official benchmark coverage is available for: ",
              paste(federal_only_real_benchmark_metrics, collapse = ", "),
              "."
            )
          },
          "Remaining benchmark cells still fall back to the provisional scaffold.",
          "Benchmark `hi` is derived from available real benchmark components when enough official inputs are present."
        ),
        collapse = " "
      )
    } else {
      "City benchmarks are derived from Baltimore CSA values. State and federal benchmarks are provisional scaffolds until ACS/FRED imports are connected."
    },
    source_files = list(
      neighborhood = if (!is.na(bnia_import$source_path)) basename(bnia_import$source_path) else NULL,
      neighborhood_services = if (length(bnia_service_import$sources)) unname(as.list(bnia_service_import$sources)) else NULL,
      asthma_proxy = if (length(cdc_asthma_import$sources)) unname(as.list(cdc_asthma_import$sources)) else NULL,
      state_benchmarks = compact(list(
        acs = if (length(acs_benchmark_import$sources)) acs_benchmark_import$sources[["state"]] else NULL,
        fred = if (length(fred_benchmark_import$sources)) fred_benchmark_import$sources[["state"]] else NULL,
        cdc_asthma = if (length(cdc_benchmark_import$sources)) "CDC PLACES / 500 Cities aggregate queries (2018-2023)" else NULL,
        cdc_life_expectancy = if (length(cdc_life_expectancy_benchmark_import$sources)) cdc_life_expectancy_benchmark_import$sources[["state"]] else NULL,
        cdc_lead = if (length(cdc_lead_benchmark_import$sources)) cdc_lead_benchmark_import$sources[["state"]] else NULL
      )),
      federal_benchmarks = compact(list(
        acs = if (length(acs_benchmark_import$sources)) acs_benchmark_import$sources[["federal"]] else NULL,
        fred = if (length(fred_benchmark_import$sources)) fred_benchmark_import$sources[["federal"]] else NULL,
        cdc_asthma = if (length(cdc_benchmark_import$sources)) "CDC PLACES aggregate queries (2018-2023)" else NULL,
        cdc_life_expectancy = if (length(cdc_life_expectancy_benchmark_import$sources)) cdc_life_expectancy_benchmark_import$sources[["federal"]] else NULL
      )),
      hazards = if (has_311_data) "Open Baltimore 311 ArcGIS services" else NULL,
      poverty = if (nrow(csa_acs_summary) > 0) "ACS 2022 tract estimates weighted to CSA" else NULL
    ),
    derived_metrics = if (derive_hi_from_components) as.list("hi") else NULL,
    proxy_metrics = if ("as" %in% cdc_metric_cols) as.list("as") else NULL,
    real_benchmark_metrics = if (length(real_benchmark_metrics)) as.list(real_benchmark_metrics) else NULL,
    state_benchmark_metrics = if (length(state_real_benchmark_metrics)) as.list(state_real_benchmark_metrics) else NULL,
    federal_benchmark_metrics = if (length(federal_real_benchmark_metrics)) as.list(federal_real_benchmark_metrics) else NULL,
    imported_metrics = sort(unique(c(
      service_metric_cols,
      cdc_metric_cols,
      bnia_metric_cols,
      if (derive_hi_from_components) "hi",
      if (nrow(csa_acs_summary) > 0) "pv",
      if (has_311_data) c("rt", "dp", "ws", "hz")
    )))
  ),
  neighborhoods = neighborhoods_output,
  benchmarks = benchmarks_output
)

write_json(json_ready_data, "data.json", auto_unbox = TRUE, pretty = TRUE)
print("Pipeline Complete! Normalized longitudinal CSA data.json updated.")
