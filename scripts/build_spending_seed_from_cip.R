#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(dplyr)
  library(sf)
  library(stringr)
})

args <- commandArgs(trailingOnly = FALSE)
file_arg <- grep("^--file=", args, value = TRUE)
script_dir <- dirname(normalizePath(sub("^--file=", "", file_arg[1])))
repo_root <- normalizePath(file.path(script_dir, ".."))

csa_path <- file.path(repo_root, "csa_boundaries.geojson")
seed_path <- file.path(repo_root, "supabase", "seeds", "spending_events_sample.sql")
migration_path <- file.path(repo_root, "supabase", "migrations", "003_spending_seed.sql")
source_tag <- "baltimore_cip_fy14_20"
source_base <- "https://geodata.baltimorecity.gov/egis/rest/services/Planning/CIP_Equity_layers/MapServer"

normalize_name <- function(x) {
  x %>%
    str_to_lower() %>%
    str_replace_all("&", " and ") %>%
    str_replace_all("[^a-z0-9]+", " ") %>%
    str_squish()
}

sql_escape <- function(x) {
  x <- ifelse(is.na(x), NA_character_, as.character(x))
  x <- gsub("'", "''", x, fixed = TRUE)
  x
}

sql_text <- function(x) {
  ifelse(
    is.na(x) | x == "",
    "NULL",
    paste0("'", sql_escape(x), "'")
  )
}

sql_num <- function(x) {
  ifelse(
    is.na(x),
    "NULL",
    format(round(as.numeric(x), 2), nsmall = 2, scientific = FALSE, trim = TRUE)
  )
}

build_query_url <- function(layer_id) {
  paste0(
    source_base,
    "/",
    layer_id,
    "/query?where=1%3D1&outFields=*&f=geojson"
  )
}

guess_category <- function(title, agency, descriptor) {
  haystack <- str_to_lower(str_c(title, agency, descriptor, sep = " | "))

  case_when(
    str_detect(haystack, "lead|abatement|hazard control") ~ "Lead paint remediation",
    str_detect(haystack, "demolition|vacant|blight|stabilization|greening") ~ "Vacant property demolition",
    str_detect(haystack, "dump|trash|sanitation|sweep|clean") ~ "Sanitation / illegal dumping cleanup",
    str_detect(haystack, "sewer|water|storm|drain|utility") ~ "Water / sewer infrastructure",
    str_detect(haystack, "road|street|bridge|sidewalk|traffic|resurfac|paving|repaving|corridor|transit") ~ "Road repaving / transportation",
    str_detect(haystack, "lighting|led") ~ "Lighting upgrades",
    str_detect(haystack, "housing|home ownership|homeowner|residential") ~ "Housing / neighborhood development",
    str_detect(haystack, "school|elementary|middle school|high school|academy|classroom|student|city school system|bcps|e/ms|school #") ~ "School facility maintenance",
    str_detect(haystack, "\\bpark\\b|playground|recreation|athletic field|ball field|court renovation|greenspace|nature center|trail|rec center") ~ "Park improvements / recreation",
    agency %in% c("DOT", "DPW") ~ "Road repaving / transportation",
    agency %in% c("DHCD", "HCD") ~ "Housing / neighborhood development",
    agency %in% c("BCRP") ~ "Park improvements / recreation",
    agency %in% c("BCPS") ~ "School facility maintenance",
    agency %in% c("BCHD") ~ "Public health intervention",
    TRUE ~ "Capital improvement project"
  )
}

infer_status <- function(fy) {
  case_when(
    is.na(fy) ~ "planned",
    fy < 2020 ~ "completed",
    fy == 2020 ~ "active",
    TRUE ~ "planned"
  )
}

load_csa_boundaries <- function(path) {
  csa_boundaries <- st_read(path, quiet = TRUE)
  csa_col <- c("Community", "CSA2010", "CSA2020", "Name")[c("Community", "CSA2010", "CSA2020", "Name") %in% names(csa_boundaries)][1]
  if (is.na(csa_col)) stop("Could not detect the CSA name column in csa_boundaries.geojson.")

  csa_boundaries %>%
    rename(CSA = !!sym(csa_col)) %>%
    select(CSA, geometry) %>%
    mutate(
      CSA = str_squish(as.character(CSA)),
      CSA_key = normalize_name(CSA)
    ) %>%
    filter(CSA != "Unassigned -- Jail") %>%
    st_transform(2248)
}

read_cip_layer <- function(layer_id, geometry_kind) {
  message("Loading CIP layer ", layer_id, " (", geometry_kind, ")...")
  st_read(build_query_url(layer_id), quiet = TRUE) %>%
    st_transform(2248) %>%
    mutate(
      layer_id = layer_id,
      geometry_kind = geometry_kind
    )
}

normalize_cip_rows <- function(x) {
  x %>%
    mutate(
      cip_no = coalesce(.data$CIPNo, .data$CIPno),
      agency_name = coalesce(.data$agency, .data$Agency_CWm),
      project_title = coalesce(.data$Project_Title, .data$Name, .data$name_1, .data$name_1jpeg),
      project_name = coalesce(.data$Name, .data$Project_Title, .data$name_1, .data$name_1jpeg),
      descriptor = coalesce(.data$CIP__, .data$project_title),
      location_label = coalesce(.data$Location, .data$Council_Di),
      total_amount = coalesce(.data$Totals_1, .data$Totals),
      fy = suppressWarnings(as.integer(round(.data$FY))),
      project_key = coalesce(.data$Unique_Identifier, str_c("layer", .data$layer_id, "-", row_number())),
      category = guess_category(project_title, agency_name, descriptor),
      status = infer_status(fy)
    ) %>%
    filter(!is.na(total_amount), total_amount > 0, !is.na(project_title)) %>%
    select(
      project_key,
      geometry_kind,
      project_name,
      project_title,
      agency_name,
      cip_no,
      descriptor,
      location_label,
      fy,
      category,
      status,
      total_amount,
      geometry
    )
}

allocate_points <- function(points, csa_boundaries) {
  if (!nrow(points)) return(tibble())

  joined <- st_join(points, csa_boundaries %>% select(CSA, CSA_key), join = st_within, left = TRUE)
  missing_idx <- which(is.na(joined$CSA))
  if (length(missing_idx)) {
    nearest_idx <- st_nearest_feature(joined[missing_idx, ], csa_boundaries)
    joined$CSA[missing_idx] <- csa_boundaries$CSA[nearest_idx]
    joined$CSA_key[missing_idx] <- csa_boundaries$CSA_key[nearest_idx]
  }

  joined %>%
    st_drop_geometry() %>%
    transmute(
      nsa = CSA,
      project_key,
      project_name,
      project_title,
      agency_name,
      cip_no,
      descriptor,
      location_label,
      fy,
      category,
      status,
      share = 1,
      amount = total_amount,
      geometry_kind
    )
}

allocate_areal <- function(features, csa_boundaries, measure_fun) {
  if (!nrow(features)) return(tibble())

  features <- features %>%
    mutate(total_measure = as.numeric(measure_fun(geometry))) %>%
    filter(is.finite(total_measure), total_measure > 0)

  if (!nrow(features)) return(tibble())

  intersections <- suppressWarnings(
    st_intersection(
      features %>% select(-geometry_kind),
      csa_boundaries %>% select(CSA, CSA_key)
    )
  )

  if (!nrow(intersections)) return(tibble())

  intersections %>%
    mutate(overlap_measure = as.numeric(measure_fun(geometry))) %>%
    filter(is.finite(overlap_measure), overlap_measure > 0) %>%
    st_drop_geometry() %>%
    mutate(
      share = pmin(1, overlap_measure / total_measure),
      amount = total_amount * share
    ) %>%
    transmute(
      nsa = CSA,
      project_key,
      project_name,
      project_title,
      agency_name,
      cip_no,
      descriptor,
      location_label,
      fy,
      category,
      status,
      share,
      amount,
      geometry_kind = if_else(is.na(location_label), "spatial", "spatial")
    )
}

build_details <- function(df) {
  share_pct <- round(df$share * 100, 1)
  mapply(
    function(fy, agency_name, cip_no, location_label, descriptor, share_pct) {
      parts <- c(
        "Official Baltimore FY14-20 CIP layer",
        if (!is.na(fy)) paste0("FY", fy) else NA_character_,
        agency_name,
        if (!is.na(cip_no) && nzchar(cip_no)) paste0("CIP ", cip_no) else NA_character_,
        if (!is.na(location_label) && nzchar(location_label)) location_label else NA_character_,
        if (!is.na(descriptor) && nzchar(descriptor)) descriptor else NA_character_,
        paste0("share ", share_pct, "%"),
        "status inferred from fiscal year; exact work dates unavailable from source"
      )
      paste(parts[!is.na(parts) & nzchar(parts)], collapse = " | ")
    },
    df$fy,
    df$agency_name,
    df$cip_no,
    df$location_label,
    df$descriptor,
    share_pct,
    USE.NAMES = FALSE
  )
}

write_seed_sql <- function(rows, path, is_migration = FALSE) {
  header <- c(
    if (is_migration) {
      "-- Official Baltimore CIP-derived spending seed for pilot/admin workflows"
    } else {
      "-- Official Baltimore CIP-derived spending seed"
    },
    "-- Source: Baltimore City Planning CIP_Equity_layers (FY14-20 capital improvement projects)",
    "-- Generated from point, line, and polygon project layers and allocated to CSAs spatially.",
    "-- Status is inferred from fiscal year because the source does not expose execution status or exact work dates.",
    ""
  )

  delete_stmt <- c(
    "DELETE FROM public.spending_events",
    sprintf("WHERE source = '%s';", source_tag),
    ""
  )

  value_lines <- rows %>%
    mutate(
      value_sql = str_c(
        "(",
        sql_text(nsa), ", ",
        sql_text(category), ", ",
        sql_text(project_name), ", ",
        sql_num(amount), ", ",
        "NULL, ",
        "NULL, ",
        sql_text(status), ", ",
        sql_text(source_tag), ", ",
        sql_text(details),
        ")"
      )
    ) %>%
    pull(value_sql)

  insert_stmt <- c(
    "INSERT INTO public.spending_events (",
    "  nsa,",
    "  category,",
    "  program_name,",
    "  amount,",
    "  started_on,",
    "  completed_on,",
    "  status,",
    "  source,",
    "  details",
    ")",
    "VALUES",
    paste0("  ", value_lines, collapse = ",\n"),
    ";"
  )

  writeLines(c(header, delete_stmt, insert_stmt), path, useBytes = TRUE)
}

csa_boundaries <- load_csa_boundaries(csa_path)

cip_raw <- bind_rows(
  read_cip_layer(0, "point"),
  read_cip_layer(1, "line"),
  read_cip_layer(2, "polygon")
)

cip_clean <- normalize_cip_rows(cip_raw)

point_rows <- cip_clean %>%
  filter(geometry_kind == "point") %>%
  allocate_points(csa_boundaries)

line_rows <- cip_clean %>%
  filter(geometry_kind == "line") %>%
  allocate_areal(csa_boundaries, st_length)

polygon_rows <- cip_clean %>%
  filter(geometry_kind == "polygon") %>%
  allocate_areal(csa_boundaries, st_area)

all_rows <- bind_rows(point_rows, line_rows, polygon_rows) %>%
  group_by(
    nsa,
    category,
    project_key,
    project_name,
    project_title,
    agency_name,
    cip_no,
    descriptor,
    location_label,
    fy,
    status
  ) %>%
  summarise(
    amount = sum(amount, na.rm = TRUE),
    share = sum(share, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  filter(is.finite(amount), amount > 0) %>%
  mutate(details = build_details(.)) %>%
  arrange(desc(fy), nsa, project_name)

if (!nrow(all_rows)) {
  stop("No spending rows were generated from the Baltimore CIP layers.")
}

write_seed_sql(all_rows, seed_path, is_migration = FALSE)
write_seed_sql(all_rows, migration_path, is_migration = TRUE)

message("Wrote ", nrow(all_rows), " spending rows to:")
message("  ", seed_path)
message("  ", migration_path)
