## load libraries ----
library(tidyverse)
library(magrittr)
library(fst)
library(arrow)
library(tictoc)

#### #### ####
## Read adm_expenditures_data ----
#### #### ####

tic("read compressed rds")
medpar_data <- read_rds("../data/input/xw-causal-inf-quantiles-yw/zipcode_er_num.rds")
toc()
#read compressed rds: 766.894 sec elapsed
#12 mins
#40GB
#A tibble: 114,882,647 × 47

# names(medpar_data)
 # "age"                    "sex"                    "race"                   "SSA_State_CD"          
 # "SSA_CNTY_CD"            "DODFLAG"                "LOS_DAY_CNT"            "BENE_PTA_COINSRNC_AMT" 
 # "BENE_IP_DDCTBL_AMT"     "BENE_BLOOD_DDCTBL_AMT"  "BENE_PRMRY_PYR_AMT"     "DRG_OUTLIER_PMT_AMT"   
 # "IP_DSPRPRTNT_SHR_AMT"   "IME_AMT"                "DRG_PRICE_AMT"          "PASS_THRU_AMT"         
 # "TOT_PPS_CPTL_AMT"       "total_charge"           "Total_covered_charge"   "Total_Medicare_Payment"
 # "ACMDTNS_TOT_CHRG_AMT"   "DPRTMNTL_TOT_CHRG_AMT"  "ICU_day"                "CCI_day"               
 # "ICU"                    "CCI"                    "diag1"                  "diag2"                 
 # "diag3"                  "diag4"                  "diag5"                  "diag6"                 
 # "diag7"                  "diag8"                  "diag9"                  "diag10"                
 # "Discharge_CD"           "adm_source"             "adm_type"               "year"                  
 # "adate"                  "BENE_DOD"               "ddate"                  "prov_num"              
 # "QID"                    "ccs1"                   "zipcode_R" 

## prepare columns
cols <- c("LOS_DAY_CNT", "Total_Medicare_Payment", "year", "QID")
medpar_data <- medpar_data %>% 
  select(cols) %>% 
  rename(length_of_stay_days = LOS_DAY_CNT, 
         total_medicare_payment = Total_Medicare_Payment, 
         bene_id = QID)
#5.91GB

## write parquet files
years_ <- unique(medpar_data$year)
# years_
# 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014

for(y_ in years_) {
  medpar_data_subset <- medpar_data[medpar_data$year == y_, ]
  file_name = paste0("../data/intermediate/scratch/adm_expenditures_", y_, ".parquet")
  write_parquet(medpar_data_subset, file_name)
}

#### #### ####
## Read zip_data
#### #### ####

tic("read aggregate_data.feather") 
zip_data <- read_feather("../data/input/aggregated_2000-2016_medicare_mortality_pm25_zip/aggregate_data.feather")
toc()

# names(zip_data)
 # "zip"                "year"               "sex"                "race"               "dual"               "entry_age_break"   
 # "followup_year"      "dead"               "time_count"         "pm25_ensemble"      "mean_bmi"           "smoke_rate"        
 # "hispanic"           "pct_blk"            "medhouseholdincome" "medianhousevalue"   "poverty"            "education"         
 # "popdensity"         "pct_owner_occ"      "summer_tmmx"        "winter_tmmx"        "summer_rmax"        "winter_rmax"       
 # "region"


## prepare columns
zip_data <- zip_data %>% 
  distinct(zip, year, region, 
           pm25_ensemble, smoke_rate, mean_bmi, hispanic, pct_blk, 
           medhouseholdincome, medianhousevalue, poverty, education, popdensity, 
           pct_owner_occ, summer_tmmx, winter_tmmx, summer_rmax, winter_rmax)

## write parquet files
years_ <- unique(zip_data$year)
#years_
# 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016

for (y_ in years_) {
  file_name <- paste0("../data/intermediate/scratch/zip_data_", y_, ".parquet")
  zip_data %>% 
    filter(year == y_) %>% 
    write_parquet(file_name)
}

#### #### ####
## Read denom_data ----
#### #### ####

tic("read denom_by_year")
denom_data <- read_fst("../data/input/denom_by_year/confounder_exposure_merged_nodups_health_2005.fst",)
toc()
#read denom_by_year: 23.706 sec elapsed

# names(denom_data)
# "zip"                          "year"                         "qid"                         
# "dodflag"                      "bene_dod"                     "sex"                         
# "race"                         "age"                          "hmo_mo"                      
# "hmoind"                       "statecode"                    "latitude"                    
# "longitude"                    "dual"                         "death"                       
# "dead"                         "entry_age"                    "entry_year"                  
# "entry_age_break"              "followup_year"                "followup_year_plus_one"      
# "pm25_ensemble"                "pm25_no_interp"               "pm25_nn"                     
# "ozone"                        "ozone_no_interp"              "zcta"                        
# "poverty"                      "popdensity"                   "medianhousevalue"            
# "pct_blk"                      "medhouseholdincome"           "pct_owner_occ"               
# "hispanic"                     "education"                    "population"                  
# "zcta_no_interp"               "poverty_no_interp"            "popdensity_no_interp"        
# "medianhousevalue_no_interp"   "pct_blk_no_interp"            "medhouseholdincome_no_interp"
# "pct_owner_occ_no_interp"      "hispanic_no_interp"           "education_no_interp"         
# "population_no_interp"         "smoke_rate"                   "mean_bmi"                    
# "smoke_rate_no_interp"         "mean_bmi_no_interp"           "amb_visit_pct"               
# "a1c_exm_pct"                  "amb_visit_pct_no_interp"      "a1c_exm_pct_no_interp"       
# "tmmx"                         "rmax"                         "pr"                          
# "cluster_cat"                  "fips_no_interp"               "fips"                        
# "summer_tmmx"                  "summer_rmax"                  "winter_tmmx"                 
# "winter_rmax"
