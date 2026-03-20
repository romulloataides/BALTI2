library(jsonlite)
library(readxl)
library(dplyr)
library(purrr)
library(stringr)
library(tidyr)
library(tibble)

script_arg <- commandArgs()[grepl("^--file=", commandArgs())]
script_path <- if (length(script_arg)) {
  normalizePath(sub("^--file=", "", script_arg[1]))
} else {
  normalizePath(".")
}

repo_root <- normalizePath(file.path(dirname(script_path), ".."))
downloads_root <- Sys.getenv("VITAL_SIGNS_DIR", unset = "/Users/RomulloAtaides/Downloads/Vital Signs 10 Data Tables")
output_path <- file.path(repo_root, "data.json")

official_csas <- c(
  "Allendale/Irvington/S. Hilton",
  "Beechfield/Ten Hills/West Hills",
  "Belair-Edison",
  "Brooklyn/Curtis Bay/Hawkins Point",
  "Canton",
  "Cedonia/Frankford",
  "Cherry Hill",
  "Chinquapin Park/Belvedere",
  "Claremont/Armistead",
  "Clifton-Berea",
  "Cross-Country/Cheswolde",
  "Dickeyville/Franklintown",
  "Dorchester/Ashburton",
  "Downtown/Seton Hill",
  "Edmondson Village",
  "Fells Point",
  "Forest Park/Walbrook",
  "Glen-Fallstaff",
  "Greater Charles Village/Barclay",
  "Greater Govans",
  "Greater Mondawmin",
  "Greater Roland Park/Poplar Hill",
  "Greater Rosemont",
  "Greenmount East",
  "Hamilton",
  "Harbor East/Little Italy",
  "Harford/Echodale",
  "Highlandtown",
  "Howard Park/West Arlington",
  "Inner Harbor/Federal Hill",
  "Lauraville",
  "Loch Raven",
  "Madison/East End",
  "Medfield/Hampden/Woodberry/Remington",
  "Midtown",
  "Midway/Coldstream",
  "Morrell Park/Violetville",
  "Mount Washington/Coldspring",
  "North Baltimore/Guilford/Homeland",
  "Northwood",
  "Oldtown/Middle East",
  "Orangeville/East Highlandtown",
  "Patterson Park North & East",
  "Penn North/Reservoir Hill",
  "Pimlico/Arlington/Hilltop",
  "Poppleton/The Terraces/Hollins Market",
  "Sandtown-Winchester/Harlem Park",
  "South Baltimore",
  "Southeastern",
  "Southern Park Heights",
  "Southwest Baltimore",
  "The Waverlies",
  "Upton/Druid Heights",
  "Washington Village/Pigtown",
  "Westport/Mount Winans/Lakeland"
)

required_files <- c(
  file.path(downloads_root, "Vital Signs 10 - Children and Family Health.xlsx"),
  file.path(downloads_root, "Vital Signs 10 - Crime and Safety.xlsx"),
  file.path(downloads_root, "Vital Signs 10 - Education and Youth.xlsx"),
  file.path(downloads_root, "Vital Signs 10 - Environment.xlsx"),
  file.path(downloads_root, "Vital Signs 10 - Housing and Community Development.xlsx"),
  file.path(downloads_root, "Vital Signs 10 - Neighborhood Action.xlsx"),
  file.path(downloads_root, "Vital Signs 10 - Workforce and Economic Development.xlsx"),
  file.path(repo_root, "data_sources", "lifexp_csa2010.json"),
  file.path(repo_root, "index.html")
)

missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(
    paste(
      "Missing required files:",
      paste(missing_files, collapse = ", ")
    )
  )
}

alias_map <- c(
  "Fell's Point" = "Fells Point",
  "Glen-Falstaff" = "Glen-Fallstaff",
  "Harford/Perring" = "Harford/Echodale",
  "Medfield/Hampden/Woodberry" = "Medfield/Hampden/Woodberry/Remington",
  "Midtown*" = "Midtown",
  "Mt. Washington/Coldspring" = "Mount Washington/Coldspring",
  "Northeast Baltimore/Parkville" = "Hamilton",
  "Patterson Park Neighborhood" = "Patterson Park North & East",
  "Upton/Druid Heights*" = "Upton/Druid Heights",
  "Washington Village" = "Washington Village/Pigtown",
  "Westport/Mt. Winans/Lakeland" = "Westport/Mount Winans/Lakeland"
)

canonicalize_name <- function(x) {
  x <- as.character(x)
  x <- str_squish(x)
  x <- str_replace_all(x, "\\*$", "")
  x <- str_replace_all(x, "^Mt\\.", "Mount")
  x <- ifelse(x %in% names(alias_map), alias_map[x], x)
  x
}

read_metric <- function(path, sheet, value_col) {
  raw <- suppressMessages(read_excel(path, sheet = sheet, col_names = FALSE))

  tibble(
    raw_name = as.character(raw[[1]]),
    raw_value = raw[[value_col]]
  ) %>%
    mutate(
      Neighborhood = canonicalize_name(raw_name),
      value = suppressWarnings(as.numeric(raw_value))
    ) %>%
    filter(Neighborhood %in% official_csas) %>%
    group_by(Neighborhood) %>%
    summarize(
      value = if (all(is.na(value))) NA_real_ else dplyr::first(na.omit(value)),
      .groups = "drop"
    )
}

parse_legacy_base <- function(index_path) {
  text <- paste(readLines(index_path, warn = FALSE), collapse = "\n")
  match <- regmatches(
    text,
    regexpr("const BASE=\\{[\\s\\S]*?\\};\\nconst RATES=", text, perl = TRUE)
  )

  if (!length(match)) {
    stop("Could not find const BASE in index.html")
  }

  block <- match[[1]]
  block <- sub("^const BASE=\\{", "", block)
  block <- sub("\\};\\nconst RATES=$", "", block)

  items <- str_match_all(block, "\"([^\"]+)\"\\s*:\\s*\\{([^}]*)\\}")[[1]]

  rows <- map(seq_len(nrow(items)), function(i) {
    name <- items[i, 2]
    body <- items[i, 3]
    metrics <- str_split(body, ",")[[1]] %>%
      str_trim() %>%
      discard(~ .x == "")

    values <- set_names(
      map_dbl(metrics, ~ as.numeric(str_trim(str_split_fixed(.x, ":", 2)[, 2]))),
      map_chr(metrics, ~ str_trim(str_split_fixed(.x, ":", 2)[, 1]))
    )

    tibble(Neighborhood = canonicalize_name(name), !!!as.list(values))
  })

  bind_rows(rows) %>%
    filter(Neighborhood %in% official_csas) %>%
    group_by(Neighborhood) %>%
    summarize(across(everything(), dplyr::first), .groups = "drop")
}

rank_score <- function(x, higher_is_better = TRUE, min_out = 0, max_out = 100) {
  out <- rep(NA_real_, length(x))
  ok <- !is.na(x)

  if (!any(ok)) {
    return(out)
  }

  vals <- x[ok]
  ord <- rank(vals, ties.method = "average")
  if (!higher_is_better) {
    ord <- max(ord) - ord + 1
  }

  if (length(vals) == 1) {
    scaled <- 50
  } else {
    scaled <- (ord - 1) / (length(vals) - 1)
    scaled <- min_out + scaled * (max_out - min_out)
  }

  out[ok] <- scaled
  out
}

zscore <- function(x) {
  m <- mean(x, na.rm = TRUE)
  s <- stats::sd(x, na.rm = TRUE)
  if (is.na(s) || s == 0) {
    return(rep(0, length(x)))
  }
  (x - m) / s
}

weighted_knn_fill <- function(row_index, train_idx, field, data, predictor_cols, k = 5) {
  train_mat <- as.matrix(data[train_idx, predictor_cols, drop = FALSE])
  target <- as.numeric(data[row_index, predictor_cols, drop = FALSE])
  dists <- sqrt(rowSums((sweep(train_mat, 2, target, "-"))^2))
  ord <- order(dists)
  keep <- train_idx[ord[seq_len(min(k, length(ord)))]]
  keep_dists <- dists[ord[seq_len(min(k, length(ord)))]]
  weights <- 1 / (keep_dists + 1e-6)
  sum(data[[field]][keep] * weights, na.rm = TRUE) / sum(weights, na.rm = TRUE)
}

children_path <- file.path(downloads_root, "Vital Signs 10 - Children and Family Health.xlsx")
crime_path <- file.path(downloads_root, "Vital Signs 10 - Crime and Safety.xlsx")
education_path <- file.path(downloads_root, "Vital Signs 10 - Education and Youth.xlsx")
environment_path <- file.path(downloads_root, "Vital Signs 10 - Environment.xlsx")
housing_path <- file.path(downloads_root, "Vital Signs 10 - Housing and Community Development.xlsx")
neighborhood_action_path <- file.path(downloads_root, "Vital Signs 10 - Neighborhood Action.xlsx")
workforce_path <- file.path(downloads_root, "Vital Signs 10 - Workforce and Economic Development.xlsx")

legacy_base <- parse_legacy_base(file.path(repo_root, "index.html"))

lifeexp_raw <- fromJSON(file.path(repo_root, "data_sources", "lifexp_csa2010.json"))
lifeexp_df <- as_tibble(lifeexp_raw$features$attributes) %>%
  transmute(
    Neighborhood = canonicalize_name(CSA2010),
    le = round(as.numeric(lifexp18), 1)
  ) %>%
  filter(Neighborhood %in% official_csas)

la_df <- read_metric(children_path, 2, 6) %>% rename(la = value)
pv_df <- read_metric(children_path, 3, 3) %>% rename(pv = value)
va_df <- read_metric(housing_path, 8, 11) %>% rename(va = value)
un_df <- read_metric(workforce_path, 1, 5) %>% rename(un = value)
hs_df <- read_metric(education_path, 10, 7) %>% rename(hs = value)
cr_df <- read_metric(crime_path, 1, 12) %>% rename(cr = value)

hazard_df <- read_metric(environment_path, 2, 2) %>% rename(hazard_sites = value)
no_car_df <- read_metric(environment_path, 3, 3) %>% rename(no_car_share = value)
transit_df <- read_metric(environment_path, 4, 3) %>% rename(public_transit_share = value)
steward_df <- read_metric(neighborhood_action_path, 4, 2) %>% rename(steward_groups = value)
garden_df <- read_metric(neighborhood_action_path, 5, 2) %>% rename(community_gardens = value)
vote_df <- read_metric(neighborhood_action_path, 9, 7) %>% rename(vote_share = value)

source_df <- tibble(Neighborhood = official_csas) %>%
  left_join(lifeexp_df, by = "Neighborhood") %>%
  left_join(la_df, by = "Neighborhood") %>%
  left_join(pv_df, by = "Neighborhood") %>%
  left_join(va_df, by = "Neighborhood") %>%
  left_join(un_df, by = "Neighborhood") %>%
  left_join(hs_df, by = "Neighborhood") %>%
  left_join(cr_df, by = "Neighborhood") %>%
  left_join(hazard_df, by = "Neighborhood") %>%
  left_join(no_car_df, by = "Neighborhood") %>%
  left_join(transit_df, by = "Neighborhood") %>%
  left_join(steward_df, by = "Neighborhood") %>%
  left_join(garden_df, by = "Neighborhood") %>%
  left_join(vote_df, by = "Neighborhood") %>%
  mutate(
    greenspace_raw = coalesce(steward_groups, 0) + coalesce(community_gardens, 0)
  )

predictor_cols <- c(
  "le",
  "la",
  "pv",
  "va",
  "un",
  "hs",
  "cr",
  "hazard_sites",
  "no_car_share",
  "public_transit_share",
  "greenspace_raw",
  "vote_share"
)

model_df <- source_df %>%
  left_join(
    legacy_base %>%
      select(Neighborhood, hi, as, fd, gs, hw, `in`, tp, dp),
    by = "Neighborhood"
  )

for (col in predictor_cols) {
  med <- median(model_df[[col]], na.rm = TRUE)
  model_df[[col]][is.na(model_df[[col]])] <- med
}

for (col in predictor_cols) {
  model_df[[paste0(col, "_z")]] <- zscore(model_df[[col]])
}

distance_cols <- paste0(predictor_cols, "_z")
legacy_fields <- c("hi", "as", "fd", "gs", "hw", "in", "tp", "dp")
train_idx <- which(!is.na(model_df$hi))

if (length(train_idx) < 5) {
  stop("Not enough matched legacy CSA records to impute the remaining dashboard fields.")
}

for (field in legacy_fields) {
  missing_idx <- which(is.na(model_df[[field]]))
  if (!length(missing_idx)) {
    next
  }

  for (i in missing_idx) {
    model_df[[field]][i] <- weighted_knn_fill(
      row_index = i,
      train_idx = train_idx,
      field = field,
      data = model_df,
      predictor_cols = distance_cols,
      k = 5
    )
  }
}

source_backed <- source_df %>%
  transmute(
    Neighborhood,
    le = round(le, 1),
    la = round(la, 1),
    va = round(va, 1),
    pv = round(pv, 1),
    un = round(un, 1),
    hs = round(hs, 1),
    cr = round(cr, 1)
  )

proxy_scores <- source_df %>%
  transmute(
    Neighborhood,
    gs_proxy = rank_score(greenspace_raw, higher_is_better = TRUE),
    hw_proxy = rank_score(log1p(hazard_sites), higher_is_better = FALSE, min_out = 5, max_out = 60),
    in_proxy = round(vote_share, 1)
  )

final_df <- tibble(Neighborhood = official_csas) %>%
  left_join(source_backed, by = "Neighborhood") %>%
  left_join(proxy_scores, by = "Neighborhood") %>%
  left_join(model_df %>% select(Neighborhood, all_of(legacy_fields)), by = "Neighborhood") %>%
  mutate(
    hi = round(hi, 1),
    as = round(as, 1),
    fd = round(fd, 1),
    gs = round(coalesce(gs_proxy, gs), 1),
    hw = round(coalesce(hw_proxy, hw), 1),
    `in` = round(coalesce(in_proxy, `in`), 1),
    tp = round(tp, 1),
    dp = round(dp, 1)
  ) %>%
  select(Neighborhood, hi, le, as, la, va, pv, un, hs, fd, gs, hw, cr, `in`, tp, dp)

missing_core <- final_df %>%
  summarize(across(-Neighborhood, ~ sum(is.na(.x)))) %>%
  pivot_longer(everything(), names_to = "field", values_to = "missing_n") %>%
  filter(missing_n > 0)

if (nrow(missing_core) > 0) {
  stop(
    paste(
      "Some fields still have missing values:",
      paste(paste0(missing_core$field, "=", missing_core$missing_n), collapse = ", ")
    )
  )
}

json_ready <- split(final_df, final_df$Neighborhood) %>%
  map(~ .x %>% select(-Neighborhood) %>% as.list() %>% map(~ .x[[1]]))

write_json(json_ready, output_path, auto_unbox = TRUE, pretty = TRUE)

cat("Wrote", output_path, "with", nrow(final_df), "CSA keys.\n")
