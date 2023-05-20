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
source('R/anomalies_utils.R')

# start timer
tic()

# create log message
logger::log_info('Starting Anomaly Detection')

# variables / creds
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
Sys.setenv(R_CONFIG_ACTIVE=env_pipeline_stage)


# EXCLUDED FILEPATHS FOR PASS THROUGH
EXCLUDED_FILEPATHS <-
  c('projects/kwale/raw-form/reconaregistration/reconaregistration.csv',
    'projects/kwale/raw-form/reconbhousehold/reconbhousehold.csv')
EXCLUDED_FORM_ID <- c('reconaregistration','reconbhousehold')

# AWS related variables
S3_RESOLUTION_OBJECT_KEY <- 'fix_anomalies_manual_upload/google_sheets/odk_form_anomalies - resolution.csv'
BUCKET_NAME <- 'databrew.org'
ROLE_NAME <- 'cloudbrewr-aws-role'

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


# Get Files mapping from AWS S3 sync
files_orig <- tibble::tibble(file_path =
                               list.files('projects',
                                          full.names = TRUE,
                                          recursive = TRUE,
                                          pattern = ".csv"))  %>%
  dplyr::filter(stringr::str_detect(file_path, 'clean-form')) %>%
  dplyr::mutate(form_id = stringr::str_split(file_path, '/')) %>%
  dplyr::rowwise() %>%
  dplyr::mutate(form_id = form_id[3]) %>%
  dplyr::ungroup() %>%
  dplyr::filter(!file_path %in% EXCLUDED_FILEPATHS,
                !form_id %in% EXCLUDED_FORM_ID)



# 1. Detect duplicate columns
detect_by_cols <- c('hhid')

# Read local file mapping and create tibble-list inside the dataframe for
# easier indexing
tbl_nest <- files_orig %>%
  dplyr::mutate(clean = purrr::map(file_path, function(cf){
    fread(cf) })
  ) %>%
  dplyr::rowwise() %>%
  dplyr::mutate(duplicate_target = list(detect_by_cols)) %>%
  dplyr::select(file_path, form_id, clean, duplicate_target)



# 2.


toc()
unlink('projects', recursive = TRUE)
logger::log_info('Finish Anomaly Detection')
