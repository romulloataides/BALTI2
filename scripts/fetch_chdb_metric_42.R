library(httr)
library(jsonlite)
library(readr)

token <- Sys.getenv("CHDB_API_KEY")
if (!nzchar(token)) {
  stop("Set CHDB_API_KEY before running this script. Request a key from City Health Dashboard Data Access.", call. = FALSE)
}

cache_dir <- "data/cache/chdb"
dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)

fetch_json <- function(path, query = list(), pages = FALSE) {
  base <- paste0("https://www.cityhealthdashboard.com/api/data/", path)
  out <- list()
  page <- 1
  repeat {
    q <- c(list(token = token), query, if (pages) list(page = page) else list())
    response <- GET(base, query = q, timeout(120))
    stop_for_status(response)
    txt <- content(response, "text", encoding = "UTF-8")
    parsed <- fromJSON(txt, flatten = TRUE)
    out[[length(out) + 1]] <- parsed
    rows <- if (is.data.frame(parsed)) nrow(parsed) else if (is.data.frame(parsed$data)) nrow(parsed$data) else 0
    if (!pages || rows < 1000) break
    page <- page + 1
  }
  out
}

write_json <- function(x, path) {
  write(toJSON(x, pretty = TRUE, auto_unbox = TRUE, na = "null"), path)
}

metrics <- fetch_json("metrics")
metric_42 <- fetch_json("metric-data/42", list(geo_name = "Baltimore"), pages = TRUE)

write_json(metrics, file.path(cache_dir, "metrics.json"))
write_json(metric_42, file.path(cache_dir, "metric_42_baltimore.json"))

if (is.data.frame(metric_42[[1]])) {
  write_csv(metric_42[[1]], file.path(cache_dir, "metric_42_baltimore.csv"))
} else if (is.data.frame(metric_42[[1]]$data)) {
  write_csv(metric_42[[1]]$data, file.path(cache_dir, "metric_42_baltimore.csv"))
}

message("Wrote CHDB metrics and Baltimore metric 42 cache to ", cache_dir)
