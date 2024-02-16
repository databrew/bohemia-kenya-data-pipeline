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
logger::log_info('Starting Anomaly Detection for efficacy')

# variables / creds
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
Sys.setenv(R_CONFIG_ACTIVE=env_pipeline_stage)
BUCKET_NAME <- 'bohemia-lake-db'
ROLE_NAME <- 'cloudbrewr-aws-role'
OUTPUT_FILEPATH <- 'output/efficacy_anomalies_detection.csv'
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
## Fetch data
##############################

efficacy <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = glue::glue('{PROJECT_SOURCE}/clean-form/efficacy/efficacy.csv')) %>%
  pad_hhid()

anomalies_list <- list()
final_col_list <- c('KEY', 'form_id', 'anomalies_id', 'anomalies_description', 'anomalies_reports_to_wid')

##############################
## Anomalies Detecton
##############################

# 2 RDT tests invalid (alert the lab) in same visit-household
anomalies_list$sus_rdt <- efficacy %>%
  dplyr::filter(control_validity == 'invalid') %>%
  dplyr::group_by(visit, hhid) %>%
  dplyr::mutate(n = n()) %>%
  dplyr::filter(n >= 2) %>%
  dplyr::mutate(form_id = 'efficacy',
                anomalies_id = 'hh_2_rdt_invalid',
                anomalies_description = glue::glue('hhid:{hhid} from visit:{visit} has {n} invalid test from control_validity'),
                anomalies_reports_to_wid = glue::glue('{wid}')) %>%
  dplyr::ungroup() %>%
  dplyr::select(all_of(final_col_list))


# Start and End time of RDT test
anomalies_list$sus_rdt_time_diff <- efficacy %>%
  dplyr::filter(rdt_time_diff < 10) %>%
  dplyr::mutate(form_id = 'efficacy',
                anomalies_id = 'hh_rdt_time_diff_less_than_10_mins',
                anomalies_description = glue::glue('hhid:{hhid} from visit:{visit} rdt was collected in {rdt_time_diff}'),
                anomalies_reports_to_wid = glue::glue('{wid}')) %>%
  dplyr::ungroup() %>%
  dplyr::select(all_of(final_col_list))

# repeat visit number selected for individual
anomalies_list$sus_individual_repeat_visit <- efficacy %>%
  dplyr::group_by(visit, extid) %>%
  dplyr::mutate(n = n(),
                key_list = paste0(KEY, collapse = ',')) %>%
  dplyr::filter(n > 1) %>%
  dplyr::mutate(form_id = 'efficacy',
                anomalies_id = glue::glue('ind_visit_already_in_dataset'),
                anomalies_description = glue::glue('hhid:{hhid} extid:{extid}, visit:{visit} already in dataset; here are the instanceIDs: {key_list}'),
                anomalies_reports_to_wid = glue::glue('{wid}')) %>%
  dplyr::ungroup() %>%
  dplyr::select(all_of(final_col_list))

# hh gps accuracy too high
anomalies_list$sus_gps <-  efficacy %>%
  dplyr::filter(Accuracy > 15) %>%
  dplyr::mutate(form_id = 'efficacy',
                anomalies_id = 'hh_gps_accuracy_too_high',
                anomalies_description = glue::glue('hhid:{hhid} from visit:{visit} gps accuracy too high: {Accuracy}'),
                anomalies_reports_to_wid = glue::glue('{wid}')) %>%
  dplyr::ungroup() %>%
  dplyr::select(all_of(final_col_list))

# hh gps accuracy too high
anomalies_list$detected_multiple_clusters_hh <- efficacy %>%
  dplyr::group_by(hhid) %>%
  dplyr::mutate(n = n_distinct(cluster),
                key_list = paste0(KEY, collapse = ',')) %>%
  dplyr::filter(n > 1) %>%
  dplyr::mutate(form_id = 'efficacy',
                anomalies_id = glue::glue('hh_detected_multiple_clusters'),
                anomalies_description = glue::glue('hhid:{hhid} detected multiple clusters in the same households; here are the instanceIDs: {key_list}'),
                anomalies_reports_to_wid = glue::glue('{wid}')) %>%
  dplyr::ungroup() %>%
  dplyr::select(all_of(final_col_list))



#################################
## Consolidate
#################################
anomalies_list %>%
  purrr::reduce(dplyr::bind_rows) %>%
  fwrite(OUTPUT_FILEPATH)


