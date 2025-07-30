library(dplyr)
library(stringr)
library(textclean)
library(lubridate)

# Load the data
# Depending on the exact structure of your data you may need to modify
# this import or the merge below
county_tax_data <- read.csv(
  "Raw_Data/Tax_Credit_Claims.csv")

# List all CSV files with county property (and owner) data
county_property_data <- read.csv("Raw_Data/Owner_Details.csv")

# this merge will have to modified if your column names are different!
full_county_data <- merge(county_tax_data, county_property_data, 
                          by.x = "MPropertyNumber", by.y = "Parcel")

# ─── Analysis Functions ─────────────────────────────────────────────

# Filters by credit or homestead flag
# NOTE: This check isn't strictly necessary with the data as given because it
# is already filtered for properties claiming one or more of the tax credits,
# but it is still good practice to have in case that ever is not the case
filter_tax_credits <- function(df) {
  df %>%
    filter(
      !is.na(Homestead) |!is.na(Reduction2Half)
    )
}

# Normalizes owner names
normalize_names <- function(df) {
  df %>%
    mutate(
      normed_owner_name = DeededOwner %>%
        # normalizes names to uppercase, quite significant
        str_to_upper() %>%
        # removes punctuation, quite significant
        str_replace_all("[[:punct:]&&[^&]]", "") %>%
        # removes extra spaces, minor
        str_squish()
    )
}

# filters out known non-problematic names
filter_names <- function(df) {
  df %>%
    filter(
      normed_owner_name != "",
      !str_detect(normed_owner_name, regex("LISTED WITH|DELETE", 
                                           ignore_case = TRUE))
    )
}

# filters out properties transferred in the last year (which are likely
# still eligible)
filter_dates <- function(df) {
  df %>% filter(Date.Acquired < Sys.time() - years(1))
}

# Extracts duplicate names
# NOTE: Does not account for "listed with" properties
get_duplicate_names <- function(df) {
  df %>%
    group_by(normed_owner_name) %>%
    filter(n() > 1) %>%
    ungroup()
}

# ─── Runs Analysis Pipeline ─────────────────────────────────────
run_analysis <- function(df, output_path){
  
  to_return <- df %>%
    filter_tax_credits() %>%
    normalize_names() %>%
    filter_names() %>%
    # Not currently used as my current dataset does not have dates
    # filter_dates() %>%
    get_duplicate_names() %>%
    group_by(normed_owner_name) %>%
    mutate(number_times_claimed = n()) %>%
    ungroup() %>%
    arrange(desc(number_times_claimed), normed_owner_name)
  
  # Analyzes the frequency of repeated appearances of names (e.g. do most repeat
  # owners own 2 properties or 10 or 100)
  summary <- tibble(owner_name = to_return$normed_owner_name) %>%
    group_by(owner_name) %>%
    mutate(appearance = row_number()) %>%
    count(appearance, name = "count") %>%
    group_by(appearance) %>%
    summarise(total = sum(count), .groups = "drop") %>%
    mutate(appearance = as.character(appearance)) %>%
    bind_rows(
      tibble(appearance = "Total", total = length(to_return$normed_owner_name))
    )
  
  print(summary)

  # Derive base name for output files (without .rds extension)
  base_path <- sub("\\.rds$", "", output_path)
  
  # Save both outputs
  saveRDS(summary, file = paste0(base_path, "_summary.rds"))
  saveRDS(to_return, file = paste0(base_path, "_details.rds"))
  
  return(list(summary = summary, details = to_return))
}


analyzed_data <- run_analysis(
  df = full_county_data,
  output_path = "Data_Output/owner_occupancy_analysis_results.rds"
)

# writes outputs to csv's for export
write.csv(analyzed_data$details, "Data_Output/Owner_Occupancy_Duplicates.csv", 
          row.names = FALSE)
write.csv(analyzed_data$summary, "Data_Output/Owner_Occupancy_Summary_Data.csv",
          row.names = FALSE)
