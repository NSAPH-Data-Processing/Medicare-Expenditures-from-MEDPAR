library(dplyr)
library(stringi)
library(data.table)
library(arrow)

# Loop through years 2012-2014
for (year in 2012:2014) {
data_path <- paste0("../data/output/aggregate/expenditures_aggregated_", year, ".feather")

expenditure_data <- read_feather(data_path, as_data_frame = TRUE)
yearss <- year 
# Load covariates data for the year
covariates_path <- paste0("../data/intermediate/covariates/medicare_mortality_pm25_zip_", year, ".feather")
covariates_data <- read_feather(covariates_path, as_data_frame = TRUE) %>%
  filter(year == yearss) 
# Convert covariates_data to a data.table
setDT(covariates_data)
setDT(expenditure_data)
# Select first value of covariates by zip and year
covariates_data_first <- covariates_data[, .(pm25_ensemble = pm25_ensemble[1],
                                             mean_bmi = mean_bmi[1],
                                             smoke_rate = smoke_rate[1],
                                             hispanic = hispanic[1],
                                             pct_blk = pct_blk[1],
                                             medhouseholdincome = medhouseholdincome[1],
                                             medianhousevalue = medianhousevalue[1],
                                             poverty = poverty[1],
                                             education = education[1],
                                             popdensity = popdensity[1],
                                             pct_owner_occ = pct_owner_occ[1],
                                             summer_tmmx = summer_tmmx[1],
                                             winter_tmmx = winter_tmmx[1],
                                             summer_rmax = summer_rmax[1],
                                             winter_rmax = winter_rmax[1],
                                             region = region[1]), 
                                         by = c("zip", "year")]

# Left join expenditure and covariates data by zipcode

joined_data <- merge(expenditure_data, covariates_data_first, by.x = "zipcode", by.y = "zip", all.x = TRUE)

# Select distinct rows where pm25_ensemble is not NA
distinct_rows <- joined_data[!is.na(pm25_ensemble)]
# Save aggregated data
filepath_agg <- paste0("../data/output/covariate_expenditure_aggregate/covariate_expenditure_aggregated_", year, ".feather")
write_feather(distinct_rows, filepath_agg)

}