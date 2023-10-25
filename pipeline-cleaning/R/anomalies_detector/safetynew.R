library(dplyr)
library(lubridate)
library(magrittr)
library(paws)
library(config)
library(cloudbrewr)
library(logger)
library(config)
library(tictoc)
library(data.table)
library(sf)
library(sp)
source('R/anomalies_utils.R')

# start timer
tic()

# create log message
logger::log_info('Starting Anomaly Detection for safetynew')

# variables / creds
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
Sys.setenv(R_CONFIG_ACTIVE=env_pipeline_stage)
BUCKET_NAME <- 'bohemia-lake-db'
ROLE_NAME <- 'cloudbrewr-aws-role'
OUTPUT_FILEPATH <- 'output/safetynew_anomalies_detection.csv'
PROJECT_SOURCE <- 'kwale'

unlink('report/data', recursive = TRUE)
dir.create('report/data')

# create connection to AWS
tryCatch({
  # login to AWS - this will be bypassed if executed in CI/CD environment
  cloudbrewr::aws_login(
    role_name = ROLE_NAME,
    profile_name =  ROLE_NAME,
    pipeline_stage = env_pipeline_stage)

}, error = function(e){
  logger::log_error('AWS Login Failed')
  stop(e$message)
})


pad_hhid <- function(data){
  if('hhid' %in% names(data)){
    data %>%
      dplyr::mutate(hhid = stringr::str_pad(hhid, 5, pad = "0"))
  }else{
    data
  }
}

##############################
## Fetch safetynew
##############################

safetynew <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = glue::glue('{PROJECT_SOURCE}/clean-form/safetynew/safetynew.csv')) %>%
  pad_hhid()


safetynew_repeat_individual <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = glue::glue('{PROJECT_SOURCE}/clean-form/safetynew/safetynew-repeat_individual.csv')
)

safetynew_merged_df <- safetynew_repeat_individual %>%
  dplyr::inner_join(safetynew, by = c('PARENT_KEY'='KEY'))

anomalies_list <- list()
final_col_list <- c('KEY',
                    'form_id',
                    'anomalies_id',
                    'anomalies_description',
                    'anomalies_reports_to_wid')

##############################
## Anomalies Detecton
##############################

# repeat visit number selected for household
anomalies_list$visit_already_in_dataset <- safetynew %>%
  dplyr::group_by(hhid, visit) %>%
  dplyr::mutate(n = n(),
                key_list = paste0(KEY, collapse = ',')) %>%
  dplyr::filter(n > 1) %>%
  dplyr::mutate(form_id = 'safetynew',
                anomalies_id = glue::glue('hh_visit_already_in_dataset'),
                anomalies_description = glue::glue('hhid:{hhid} visit:{visit} already in dataset, please check these keys {key_list}'),
                anomalies_reports_to_wid = glue::glue('{wid}')) %>%
  dplyr::ungroup() %>%
  dplyr::select(all_of(final_col_list))

# suspicious height based on age
# anomalies_list$sus_height <- safetynew_merged_df %>%
#   dplyr::filter((age >= 12 & height < 100) | (age < 12 & height > 150)) %>%
#   dplyr::mutate(form_id = 'safetynew-repeat_individual',
#                 anomalies_id = 'ind_suspicious_height_based_on_age',
#                 anomalies_description = glue::glue('extid:{extid} is {age} years old with height {height} cm'),
#                 anomalies_reports_to_wid = glue::glue('{wid}')) %>%
#   dplyr::select(all_of(final_col_list))
#
# # suspicious weight based on age
# anomalies_list$sus_weight <- safetynew_merged_df %>%
#   dplyr::filter((age >= 12 & weight < 30) | (age < 12 & weight > 50)) %>%
#   dplyr::mutate(form_id = 'safetynew-repeat_individual',
#                 anomalies_id = 'ind_suspicious_weight_based_on_age',
#                 anomalies_description = glue::glue('extid:{extid} is {age} years old with weight {weight} kg'),
#                 anomalies_reports_to_wid = glue::glue('{wid}')) %>%
#   dplyr::select(all_of(final_col_list))
#
# # abnormal BMIs
# anomalies_list$sus_bmi <- safetynew_merged_df %>%
#   dplyr::mutate(bmi = weight / ((height/100)**2)) %>%
#   dplyr::filter(bmi < 16 | bmi > 35) %>%
#   dplyr::mutate(form_id = 'safetynew-repeat_individual',
#                 anomalies_id = 'ind_suspicious_bmi',
#                 anomalies_description = glue::glue('extid:{extid} has bmi of {bmi}'),
#                 anomalies_reports_to_wid = glue::glue('{wid}')) %>%
#   dplyr::select(all_of(final_col_list))


# hh gps accuracy too high
anomalies_list$sus_gps <-  safetynew %>%
  dplyr::filter(Accuracy > 15) %>%
  dplyr::mutate(form_id = 'safetynew',
                anomalies_id = 'hh_gps_accuracy_too_high',
                anomalies_description = glue::glue('hhid:{hhid} gps accuracy too high: {Accuracy}'),
                anomalies_reports_to_wid = glue::glue('{wid}')) %>%
  dplyr::ungroup() %>%
  dplyr::select(all_of(final_col_list))

anomalies_list$outside_cluster <- safetynew %>%
  dplyr::filter(is.na(geo_cluster_num) |
                  geo_cluster_num %in% c(1,4,6,32,35,47,52,66,71, 76, 86, 89)) %>%
  dplyr::mutate(form_id = 'safety',
                anomalies_id = 'hh_outside_cluster',
                anomalies_description = glue::glue('hhid:{hhid} is outside cluster by geo but entered as cluster:{cluster}'),
                anomalies_reports_to_wid = glue::glue('{wid}')) %>%
  dplyr::ungroup() %>%
  dplyr::select(all_of(final_col_list))


#################################
# Consolidate
#################################
anomalies_list %>%
  purrr::reduce(dplyr::bind_rows) %>%
  fwrite(OUTPUT_FILEPATH)
