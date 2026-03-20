library(tidyverse)
library(sf)
library(jsonlite)
library(tidycensus)
library(httr)

print("Starting Baltimore CSA dashboard update...")

`%||%` <- function(x, y) if (is.null(x)) y else x

script_arg <- commandArgs()[grepl("^--file=", commandArgs())]
script_path <- if (length(script_arg)) sub("^--file=", "", script_arg[1]) else "."
repo_root <- normalizePath(file.path(dirname(normalizePath(script_path)), ".."), mustWork = FALSE)

data_path <- file.path(repo_root, "data.json")
csa_geojson_path <- file.path(repo_root, "csa_boundaries.geojson")

normalize_name <- function(x) {
  x %>%
    replace_na("") %>%
    stringr::str_replace_all("&", " and ") %>%
    stringr::str_replace_all("[^[:alnum:]]+", " ") %>%
    stringr::str_squish() %>%
    stringr::str_to_lower()
}

fetch_arcgis_features <- function(service_url, out_fields, where = "1=1", page_size = 2000L) {
  offset <- 0L
  batches <- list()

  repeat {
    response <- httr::GET(
      paste0(service_url, "/query"),
      query = list(
        where = where,
        outFields = paste(out_fields, collapse = ","),
        returnGeometry = "false",
        orderByFields = "RowID ASC",
        resultOffset = offset,
        resultRecordCount = page_size,
        f = "json"
      ),
      httr::timeout(120)
    )

    httr::stop_for_status(response)

    payload <- jsonlite::fromJSON(
      httr::content(response, "text", encoding = "UTF-8"),
      simplifyVector = FALSE
    )

    if (!is.null(payload$error)) {
      stop(
        paste(
          "ArcGIS query failed for",
          service_url,
          ":",
          payload$error$message
        )
      )
    }

    features <- payload$features
    if (is.null(features) || length(features) == 0) {
      break
    }

    batch <- purrr::map_dfr(features, function(feature) {
      attrs <- feature$attributes %||% list()
      tibble::as_tibble(attrs)
    })

    if (nrow(batch) == 0) {
      break
    }

    batches[[length(batches) + 1L]] <- batch

    if (!isTRUE(payload$exceededTransferLimit)) {
      break
    }

    offset <- offset + nrow(batch)
  }

  dplyr::bind_rows(batches)
}

print("Fetching CSA boundaries...")

csa_url <- paste0(
  "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/",
  "Lifexp/FeatureServer/0/query?",
  "where=1%3D1&outFields=CSA2010&returnGeometry=true&f=geojson"
)

csa_response <- httr::GET(
  csa_url,
  httr::add_headers(
    `User-Agent` = "Mozilla/5.0",
    Accept = "application/geo+json, application/json;q=0.9, */*;q=0.8"
  ),
  httr::timeout(60)
)

httr::stop_for_status(csa_response)

csa_text <- httr::content(csa_response, "text", encoding = "UTF-8")
writeLines(csa_text, csa_geojson_path, useBytes = TRUE)

csa_boundaries <- sf::st_read(csa_geojson_path, quiet = TRUE) %>%
  sf::st_transform(4326) %>%
  dplyr::select(Neighborhood = CSA2010, geometry) %>%
  dplyr::mutate(
    Neighborhood = stringr::str_squish(Neighborhood),
    Neighborhood_key = normalize_name(Neighborhood)
  )

print("Fetching 311 point data...")

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

raw_311 <- purrr::imap_dfr(three11_sources, function(service_url, year_label) {
  print(paste("  Downloading", year_label, "311 records..."))
  fetch_arcgis_features(
    service_url = service_url,
    out_fields = c("SRType", "SRStatus", "CreatedDate", "Latitude", "Longitude")
  )
})

required_cols <- c("SRType", "SRStatus", "CreatedDate", "Latitude", "Longitude")
missing_cols <- setdiff(required_cols, names(raw_311))
if (length(missing_cols) > 0) {
  stop(
    paste(
      "311 data is missing required columns:",
      paste(missing_cols, collapse = ", ")
    )
  )
}

nsa_311_clean <- raw_311 %>%
  dplyr::transmute(
    SRType = as.character(SRType),
    SRStatus = as.character(SRStatus),
    CreatedDate = as.numeric(CreatedDate),
    Latitude = as.numeric(Latitude),
    Longitude = as.numeric(Longitude)
  ) %>%
  dplyr::filter(
    !is.na(CreatedDate),
    !is.na(Latitude),
    !is.na(Longitude)
  ) %>%
  dplyr::mutate(
    Year = lubridate::year(lubridate::as_datetime(CreatedDate / 1000, tz = "UTC"))
  ) %>%
  dplyr::filter(Year >= 2016, Year <= 2023) %>%
  sf::st_as_sf(coords = c("Longitude", "Latitude"), crs = 4326, remove = FALSE) %>%
  sf::st_join(csa_boundaries %>% dplyr::select(Neighborhood), join = sf::st_intersects, left = FALSE) %>%
  sf::st_drop_geometry() %>%
  dplyr::filter(SRStatus != "Closed (Duplicate)") %>%
  dplyr::mutate(
    Category = dplyr::case_when(
      stringr::str_detect(stringr::str_to_lower(SRType), "rat|rodent") ~ "rt",
      stringr::str_detect(stringr::str_to_lower(SRType), "trash|dumping") ~ "dp",
      stringr::str_detect(stringr::str_to_lower(SRType), "water|sewer") ~ "ws",
      TRUE ~ "other"
    )
  ) %>%
  dplyr::filter(Category != "other") %>%
  dplyr::select(Neighborhood, Category, Year)

nsa_311_yearly <- nsa_311_clean %>%
  dplyr::group_by(Neighborhood, Category, Year) %>%
  dplyr::summarize(Total = dplyr::n(), .groups = "drop")

total_hazards <- nsa_311_yearly %>%
  dplyr::group_by(Neighborhood, Year) %>%
  dplyr::summarize(Total = sum(Total), .groups = "drop") %>%
  dplyr::mutate(Category = "hz")

all_311_yearly <- dplyr::bind_rows(nsa_311_yearly, total_hazards)

complete_grid <- tidyr::crossing(
  Neighborhood = unique(csa_boundaries$Neighborhood),
  Category = c("rt", "dp", "ws", "hz"),
  Year = 2016:2023
)

nsa_311_arrays <- complete_grid %>%
  dplyr::left_join(all_311_yearly, by = c("Neighborhood", "Category", "Year")) %>%
  dplyr::mutate(Total = tidyr::replace_na(Total, 0)) %>%
  dplyr::arrange(Neighborhood, Category, Year) %>%
  dplyr::group_by(Neighborhood, Category) %>%
  dplyr::summarize(yearly_array = list(as.numeric(Total)), .groups = "drop") %>%
  tidyr::pivot_wider(names_from = Category, values_from = yearly_array)

print("Fetching ACS poverty data...")

baltimore_acs <- tidycensus::get_acs(
  geography = "tract",
  variables = c(Poverty = "B17001_002", Total_Pop = "B17001_001"),
  state = "MD",
  county = "Baltimore city",
  year = 2022,
  geometry = TRUE,
  output = "wide"
) %>%
  sf::st_transform(4326) %>%
  dplyr::mutate(
    pv = round((PovertyE / Total_PopE) * 100, 1),
    pv = tidyr::replace_na(pv, 0)
  ) %>%
  dplyr::select(GEOID, pv, geometry)

csa_acs_summary <- sf::st_intersection(csa_boundaries %>% dplyr::select(Neighborhood), baltimore_acs) %>%
  dplyr::mutate(intersect_area = sf::st_area(geometry)) %>%
  sf::st_drop_geometry() %>%
  dplyr::group_by(Neighborhood) %>%
  dplyr::summarize(
    pv = round(weighted.mean(pv, as.numeric(intersect_area), na.rm = TRUE), 1),
    .groups = "drop"
  )

print("Merging into data.json...")

if (!file.exists(data_path)) {
  stop("Missing data.json in the repo root.")
}

current_data_raw <- jsonlite::read_json(data_path, simplifyVector = FALSE)
if (!is.list(current_data_raw) || is.null(names(current_data_raw)) || any(names(current_data_raw) == "")) {
  stop("data.json must be a named object keyed by CSA.")
}

current_data <- tibble::tibble(
  Neighborhood = names(current_data_raw),
  data = unname(current_data_raw)
) %>%
  tidyr::unnest_wider(data)

final_dashboard_data <- current_data %>%
  dplyr::select(-dplyr::any_of(c("pv", "rt", "dp", "ws", "hz"))) %>%
  dplyr::left_join(csa_acs_summary, by = "Neighborhood") %>%
  dplyr::left_join(nsa_311_arrays, by = "Neighborhood") %>%
  dplyr::mutate(pv = tidyr::replace_na(pv, 0))

array_cols <- intersect(c("rt", "dp", "ws", "hz"), names(final_dashboard_data))
if (length(array_cols) > 0) {
  final_dashboard_data <- final_dashboard_data %>%
    dplyr::mutate(dplyr::across(dplyr::all_of(array_cols), ~ purrr::map(., function(x) {
      if (is.null(x) || all(is.na(x))) rep(0, 8) else as.numeric(x)
    })))
}

json_ready_data <- split(final_dashboard_data, final_dashboard_data$Neighborhood) %>%
  purrr::map(~ .x %>% dplyr::select(-Neighborhood) %>% as.list() %>% purrr::map(~ .x[[1]]))

jsonlite::write_json(json_ready_data, data_path, auto_unbox = TRUE, pretty = TRUE)

print("Pipeline complete. data.json updated.")
