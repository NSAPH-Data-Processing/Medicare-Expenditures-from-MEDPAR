# Medicare-Expenditures-from-MEDPAR

Aggregating medicare expenditures data with the covariates. The level of aggregation is yearly from 2012-2014 with total_medicare_payment aggregated per year per beneficiary ID:



## Dataset 1: 

* Year coverage: 2012-2014
* FASSE Location: n/dominici_nsaph_l3/Lab/data_processing/Medicare-Expenditures-from-MEDPAR/data/output/aggregate
* Aggregation by: beneficiary ID, adate, ddate, year, zipcode


|  Column Name | Data Type  |
|  --- | ---  |
|  adate  |  Date  |
|  LOS_DAY_CNT  |  numeric  |
|  ddate  |  Date  |
|  DRG_PRICE_AMT  |  numeric  |
|  DRG_OUTLIER_PMT_AMT  |  numeric  |
|  PASS_THRU_AMT  |  numeric  |
|  Total_Medicare_Payment  |  numeric  |
|  year  |  numeric  |
|  zipcode  |  character  |
|  QID  |  character  |
|  prov_num  |  character  |
|  age  |  numeric  |
|  sex  |  character  |
|  race  |  character  |


## Dataset 2:

* Year coverage: 2011 
* FASSE Location: n/dominici_nsaph_l3/Lab/data_processing/Medicare-Expenditures-from-MEDPAR/data/output/expenditure_covariates_2011.feather
* Aggregation by: beneficiary ID, adate, ddate, year, zipcode


|  Column Name | Data Type  |
|  --- | ---  |
|  adate  |  Date  |
|  LOS_DAY_CNT  |  numeric  |
|  ddate  |  Date  |
|  DRG_PRICE_AMT  |  numeric  |
|  DRG_OUTLIER_PMT_AMT  |  numeric  |
|  PASS_THRU_AMT  |  numeric  |
|  Total_Medicare_Payment  |  numeric  |
|  year  |  numeric  |
|  zipcode  |  character  |
|  QID  |  character  |
|  prov_num  |  character  |
|  age  |  numeric  |
|  sex  |  character  |
|  race  |  character  |
|  pm25_ens_2010  |  numeric  |
|  zip  |  integer  |
|  dual  |  character  |
|  entry_age_break  |  integer  |
|  statecode  |  character  |
|  followup_year  |  numeric  |
|  followup_year_plus_one  |  numeric  |
|  dead  |  logical  |
|  pm25_ens_2011  |  numeric  |
|  mean_bmi  |  numeric  |
|  smoke_rate  |  numeric  |
|  hispanic  |  numeric  |
|  pct_blk  |  numeric  |
|  medhouseholdincome  |  numeric  |
|  medianhousevalue  |  numeric  |
|  poverty  |  numeric  |
|  education  |  numeric  |
|  popdensity  |  numeric  |
|  pct_owner_occ  |  numeric  |
|  summer_tmmx  |  numeric  |
|  winter_tmmx  |  numeric  |
|  summer_rmax  |  numeric  |
|  winter_rmax  |  numeric  |
|  dead_in_5  |  logical  |
|  pm25_avg  |  numeric  |
|  pm25_12  |  numeric  |
|  pm25_10  |  numeric  |


## Dataset 3: 

* Year coverage: 2012-2014 
* FASSE Location: n/dominici_nsaph_l3/Lab/data_processing/Medicare-Expenditures-from-MEDPAR/data/output/covariate_expenditure_aggregate
* Aggregation by: year, zipcode


|  Column Name | Data Type  |
|  --- | ---  |
|  zipcode  |  character  |
|  year  |  numeric  |
|  QID  |  character  |
|  total_yearly_medicare_payment  |  numeric  |
|  age  |  numeric  |
|  sex  |  character  |
|  race  |  character  |
|  pm25_ensemble  |  numeric  |
|  mean_bmi  |  numeric  |
|  smoke_rate  |  numeric  |
|  hispanic  |  numeric  |
|  pct_blk  |  numeric  |
|  medhouseholdincome  |  numeric  |
|  medianhousevalue  |  numeric  |
|  poverty  |  numeric  |
|  education  |  numeric  |
|  popdensity  |  numeric  |
|  pct_owner_occ  |  numeric  |
|  summer_tmmx  |  numeric  |
|  winter_tmmx  |  numeric  |
|  summer_rmax  |  numeric  |
|  winter_rmax  |  numeric  |
|  region  |  character  |