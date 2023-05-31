## load libraries ----
library(tidyverse)
library(magrittr)
library(fst)
library(arrow)

#### #### ####
## Read adm_expenditures_data ----
#### #### ####

medpar_data <- read_rds("../data/input/xw-causal-inf-quantiles-yw/zipcode_er_num.rds")
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

years_ <- unique(medpar_data$year)
# years_
 # 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014

#### #### ####
## Read dead_in_5_data ----
#### #### ####

dead_in_5_data <- read_fst("../data/input/aggregate_medicare_data_2010to2016/aggregate_medicare_data_2010to2016.fst")
# names(dead_in_5_data)
 # "qid"                    "pm25_ens_2010"          "year"                   "zip"                   
 # "sex"                    "race"                   "age"                    "dual"                  
 # "entry_age_break"        "statecode"              "followup_year"          "followup_year_plus_one"
 # "dead"                   "pm25_ens_2011"          "mean_bmi"               "smoke_rate"            
 # "hispanic"               "pct_blk"                "medhouseholdincome"     "medianhousevalue"      
 # "poverty"                "education"              "popdensity"             "pct_owner_occ"         
 # "summer_tmmx"            "winter_tmmx"            "summer_rmax"            "winter_rmax"           
 # "dead_in_5"              "pm25_avg"               "pm25_12"                "pm25_10"

years_ <- unique(dead_in_5_data$year)
# years_
 # 2011

#### #### ####
## Read zip_data
#### #### ####

filepath_data <- "../data/input/aggregated_2000-2016_medicare_mortality_pm25_zip/aggregate_data.feather"
zip_data <- read_feather(filepath_data, as_data_frame = TRUE)
# names(zip_data)
 # "zip"                "year"               "sex"                "race"               "dual"               "entry_age_break"   
 # "followup_year"      "dead"               "time_count"         "pm25_ensemble"      "mean_bmi"           "smoke_rate"        
 # "hispanic"           "pct_blk"            "medhouseholdincome" "medianhousevalue"   "poverty"            "education"         
 # "popdensity"         "pct_owner_occ"      "summer_tmmx"        "winter_tmmx"        "summer_rmax"        "winter_rmax"       
 # "region"
