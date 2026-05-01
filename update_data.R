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

legacy_rates <- c(
  hi = 0.4, le = 0.08, as = -0.6, la = -0.5, va = -0.2, pv = -0.3,
  un = -0.25, hs = 0.15, fd = 0.3, gs = 0.1, hw = -0.2, cr = -0.5,
  "in" = 0.8, tp = 0.2, dp = 0.4, rt = 0.25, ws = 0.3, hz = 0.45
)
benchmark_metrics <- c("hi", "le", "as", "la", "va", "pv", "un", "hs", "fd", "gs", "cr")
inverse_benchmark_metrics <- c("as", "la", "va", "pv", "un", "cr")

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

build_level_record <- function(city_record, level = c("state", "federal")) {
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

    out[[metric]] <- round(as.numeric(value) + delta, 1)
  }

  out
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

for (metric in metric_cols) {
  final_dashboard_data[[metric]] <- map(
    final_dashboard_data[[metric]],
    metric_series,
    metric = metric,
    years = years
  )
}

# 6. LOAD: Write normalized JSON
print("Formatting and writing normalized output...")

neighborhoods_output <- list()

for (row_idx in seq_len(nrow(final_dashboard_data))) {
  row <- final_dashboard_data[row_idx, ]
  neighborhoods_output[[row$CSA]] <- build_yearly_entry(row, metric_cols, years = years)
}

benchmarks_output <- list(city = list(), state = list(), federal = list())

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
  benchmarks_output$state[[year_key]] <- build_level_record(city_record, "state")
  benchmarks_output$federal[[year_key]] <- build_level_record(city_record, "federal")
}

json_ready_data <- list(
  meta = list(
    schema_version = 2,
    years = as.list(years),
    provisional = TRUE,
    note = if (length(bnia_metric_cols)) {
      paste0(
        "Real BNIA longitudinal values imported for: ",
        paste(sort(bnia_metric_cols), collapse = ", "),
        ". Missing metrics still fall back to existing data.json values and modeled series where needed."
      )
    } else {
      "No BNIA longitudinal file was imported. Neighborhood yearly records still fall back to existing data.json values and modeled series where needed."
    },
    benchmark_note = "City benchmarks are derived from Baltimore CSA values. State and federal benchmarks are provisional scaffolds until ACS/FRED imports are connected.",
    source_files = list(
      neighborhood = if (!is.na(bnia_import$source_path)) basename(bnia_import$source_path) else NULL,
      hazards = if (has_311_data) "Open Baltimore 311 ArcGIS services" else NULL,
      poverty = if (nrow(csa_acs_summary) > 0) "ACS 2022 tract estimates weighted to CSA" else NULL
    ),
    imported_metrics = sort(unique(c(
      bnia_metric_cols,
      if (nrow(csa_acs_summary) > 0) "pv",
      if (has_311_data) c("rt", "dp", "ws", "hz")
    )))
  ),
  neighborhoods = neighborhoods_output,
  benchmarks = benchmarks_output
)

write_json(json_ready_data, "data.json", auto_unbox = TRUE, pretty = TRUE)
print("Pipeline Complete! Normalized longitudinal CSA data.json updated.")
