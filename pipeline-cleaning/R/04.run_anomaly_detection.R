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
logger::log_info('Starting Anomaly Detection')

# variables / creds
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
Sys.setenv(R_CONFIG_ACTIVE=env_pipeline_stage)
BUCKET_NAME <- 'bohemia-lake-db'
ROLE_NAME <- 'cloudbrewr-aws-role'

#################################
# 1. Authenticate
####################################

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

#################################
# 2. Crawl files
####################################

unlink('output', recursive = TRUE)
dir.create('output')
list.files('R/anomalies_detector', full.names = TRUE) %>% purrr::map(source)

final <- list.files('output', full.names = TRUE) %>%
  purrr::map_dfr(function(f){fread(f) %>%
      tibble::as_tibble() %>%
      mutate(across(everything(), as.character))})  %>%
  mutate(across(everything(), .fns = ~tidyr::replace_na(.,''))) %>%
  mutate(resolution_id = glue::glue('{form_id}__{KEY}__{anomalies_id}')) %>%
  dplyr::mutate(resolution_status = 'to_do') %>%
  dplyr::select(resolution_id,
                KEY,
                form_id,
                anomalies_id,
                anomalies_description,
                resolution_status,
                anomalies_reports_to_wid) %>%
  dplyr::filter(!stringr::str_detect(form_id, 'v0'))

#################################
# 3. Consolidate current and historical
####################################
OUTPUT_FILEPATH <- '/tmp/anomalies_detection.csv'
OUTPUT_FILEPATH_SUMMARY <- '/tmp/anomalies_detection_summary.csv'

final %>%
  fwrite(OUTPUT_FILEPATH)


cloudbrewr::aws_s3_store(
  bucket = 'bohemia-lake-db',
  filename = OUTPUT_FILEPATH,
  key = glue::glue('bohemia_prod/anomalies_detection/{file}', file = basename(OUTPUT_FILEPATH))
)

cloudbrewr::aws_s3_store(
  bucket = 'bohemia-lake-db',
  filename = OUTPUT_FILEPATH,
  key = glue::glue('bohemia_prod/anomalies_detection_hist/partition_date={lubridate::today()}/{file}', file = basename(OUTPUT_FILEPATH))
)

#################################
# 4. Consolidate Summary for faster query
#################################
final_history <- final %>%
  dplyr::group_by(
    form_id,
    anomalies_id) %>%
  dplyr::summarise(anomalies_count = n()) %>%
  dplyr::mutate(snapshot_date = lubridate::today())
final_history %>%
  fwrite(OUTPUT_FILEPATH_SUMMARY)
cloudbrewr::aws_s3_store(
  bucket = 'bohemia-lake-db',
  filename = OUTPUT_FILEPATH_SUMMARY,
  key = glue::glue('bohemia_prod/anomalies_detection_summary_hist/partition_date={lubridate::today()}/{file}',
                   file = basename(OUTPUT_FILEPATH_SUMMARY))
)
