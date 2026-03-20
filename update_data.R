library(tidyverse)
library(sf)
library(jsonlite)
library(RSocrata)
library(tidycensus)

print("Starting Baltimore Health Data Pipeline...")

# 1. SETUP: Authenticate Census API
# GitHub Actions will securely pass this key from your repository secrets
census_api_key(Sys.getenv("CENSUS_API_KEY"))

# 2. EXTRACT: Fetch Spatial Boundaries (NSAs)
print("Fetching NSA Boundaries...")
nsa_url <- "https://opendata.baltimorecity.gov/egis/rest/services/Hosted/Neighborhood_Statistical_Areas/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
nsa_boundaries <- st_read(nsa_url, quiet = TRUE) %>%
  st_transform(crs = 4326) %>%
  select(Neighborhood = Name, geometry) %>%
  mutate(Neighborhood = trimws(Neighborhood))

# 3. EXTRACT & TRANSFORM: Live 311 Environmental Hazards
print("Fetching and processing 311 data...")
# Pull last 90 days to capture current acute hazards
three11_url <- "https://data.baltimorecity.gov/resource/ni4d-8w7k.json?$where=createddate >= '2025-12-01T00:00:00'"
raw_311 <- read.socrata(three11_url)

nsa_311_summary <- raw_311 %>%
  filter(srstatus != "Closed (Duplicate)", !is.na(latitude), !is.na(longitude)) %>%
  mutate(latitude = as.numeric(latitude), longitude = as.numeric(longitude)) %>%
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%
  # Spatial join: map points to neighborhoods
  st_join(nsa_boundaries, join = st_intersects) %>%
  st_drop_geometry() %>%
  filter(!is.na(Neighborhood)) %>%
  mutate(
    Category = case_when(
      str_detect(str_to_lower(srtype), "rat|rodent") ~ "rt",
      str_detect(str_to_lower(srtype), "trash|dumping") ~ "dp",
      str_detect(str_to_lower(srtype), "water|sewer") ~ "ws",
      TRUE ~ "other"
    )
  ) %>%
  filter(Category != "other") %>%
  group_by(Neighborhood, Category) %>%
  summarize(Total = n(), .groups = 'drop') %>%
  pivot_wider(names_from = Category, values_from = Total, values_fill = list(Total = 0)) %>%
  # Create a composite environmental hazard score
  mutate(hz = rt + dp + ws)

# 4. EXTRACT & TRANSFORM: ACS Demographic Baselines
print("Fetching and processing Census data...")
# Variables: B17001_002 (Poverty), B23025_005 (Unemployed), B15003_017+ (HS Grad proxy)
acs_vars <- c(Poverty = "B17001_002", Total_Pop = "B17001_001")

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
  mutate(pv = round((PovertyE / Total_PopE) * 100, 1)) %>%
  select(GEOID, pv, geometry) %>%
  mutate(pv = replace_na(pv, 0))

# Spatial interpolation: Map Census Tracts to NSAs using geometric intersection
nsa_acs_summary <- st_intersection(nsa_boundaries, baltimore_acs) %>%
  mutate(intersect_area = st_area(geometry)) %>%
  st_drop_geometry() %>%
  group_by(Neighborhood) %>%
  # Calculate area-weighted poverty average for the neighborhood
  summarize(pv = round(weighted.mean(pv, as.numeric(intersect_area), na.rm = TRUE), 1))

# 5. MERGE: Combine Data and Preserve Static Baselines
# We load the hardcoded base data to preserve metrics we haven't automated yet (like Life Expectancy)
# and then overwrite the specific SDoH metrics we just calculated live.
base_data_path <- "data.json"
if(file.exists(base_data_path)) {
  current_data <- read_json(base_data_path, simplifyVector = TRUE) %>%
    as_tibble(rownames = "Neighborhood")
} else {
  stop("Missing initial data.json to use as a base.")
}

final_dashboard_data <- current_data %>%
  # Remove the old poverty and 311 columns so we can replace them with live data
  select(-any_of(c("pv", "rt", "dp", "ws", "hz"))) %>%
  left_join(nsa_acs_summary, by = "Neighborhood") %>%
  left_join(nsa_311_summary, by = "Neighborhood") %>%
  mutate(across(c(pv, rt, dp, ws, hz), ~replace_na(., 0)))

# 6. LOAD: Write to JSON
print("Formatting and writing output...")
json_ready_data <- final_dashboard_data %>%
  column_to_rownames(var = "Neighborhood") %>%
  as.list() %>%
  purrr::transpose()

write_json(json_ready_data, "data.json", auto_unbox = TRUE, pretty = TRUE)
print("Pipeline Complete! data.json updated.")
