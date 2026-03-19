# update_data.R

# Load required libraries
# We use jsonlite to convert our R dataframes into the exact JSON format the HTML needs
library(tidyverse)
library(jsonlite)

# 1. EXTRACT: Pull your data
# In a full production environment, this is where you would use httr2 or tidycensus 
# to pull live data from Open Baltimore or the Census API. 
# For this script, we will simulate the structural transformation of raw data.

# Simulated raw data frame (e.g., pulled from BNIA and BCHD APIs)
raw_health_data <- tibble(
  Neighborhood = c("Greater Roland Park/Poplar Hill", "Sandtown-Winchester/Harlem Park"),
  Health_Index = c(84, 30),
  Life_Expectancy = c(80, 63),
  Asthma_ED = c(18, 87),
  Lead_Exposure = c(9, 61),
  Vacant_Props = c(1.8, 18.7),
  Poverty_Rate = c(5, 43),
  Unemployment = c(3.2, 18.8),
  HS_Grad = c(98, 67),
  Food_Access = c(88, 34),
  Green_Space = c(56, 9),
  Crime_Rate = c(9, 85)
)

# 2. TRANSFORM: Shape the data for the JavaScript frontend
# The JS expects a nested JSON object where the neighborhood name is the key.
formatted_data <- raw_health_data %>%
  # Rename columns to match the short keys in your JavaScript (hi, le, as, etc.)
  rename(
    hi = Health_Index,
    le = Life_Expectancy,
    as = Asthma_ED,
    la = Lead_Exposure,
    va = Vacant_Props,
    pv = Poverty_Rate,
    un = Unemployment,
    hs = HS_Grad,
    fd = Food_Access,
    gs = Green_Space,
    cr = Crime_Rate
  ) %>%
  # Convert the tibble into a named list where the Neighborhood is the list name
  column_to_rownames(var = "Neighborhood") %>%
  as.list() %>%
  # Transpose the list so it nests correctly for JSON conversion
  purrr::transpose()

# 3. LOAD: Export to JSON
# Write the clean data to the JSON file your index.html is looking for
write_json(formatted_data, "data.json", auto_unbox = TRUE, pretty = TRUE)

print("Data successfully updated and written to data.json")
