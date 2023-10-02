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
OUTPUT_FILEPATH <- '/tmp/anomalies_detection.csv'
OUTPUT_FILEPATH_SUMMARY <- '/tmp/anomalies_detection_summary.csv'

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


#######################

#################################
# 1. v0 demography
#################################
data <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = 'kwale/clean-form/v0demography/v0demography.csv'
)

anomalies_list <- list()

# Strange combination in house materials
anomalies_list$v0_demo1 <- data %>%
  dplyr::filter(house_wall=='mud',
                house_roof== 'concrete') %>%
  dplyr::mutate(
    form_id = 'v0demography',
    anomalies_id = 'hh_strange_materials',
    anomalies_description = glue::glue('household strange materials combination, house_wall=mud and house_roof=concrete'))   %>%
  dplyr::select(KEY, form_id, anomalies_id, anomalies_description)


# Household completed in less than 4 minutes
anomalies_list$v0_demo2 <- data %>%
  dplyr::mutate(duration = end_time - start_time) %>%
  detect_threshold(
    col = 'duration',
    form_id = 'v0demography',
    anomalies_id = 'hh_completed_in_less_than_4_mins',
    anomalies_description = glue::glue('household completed in less than 4 mins (end_time - start_time)'),
    threshold = 4,
    direction = 'less')


# Duplicated households
anomalies_list$v0_demo3 <- data %>%
  detect_duplication(col = 'hhid',
                     form_id = 'v0demography',
                     anomalies_id = 'hh_duplicated',
                     anomalies_description = 'household duplicated')

# GPS accuracy too high
anomalies_list$v0_demo4 <- data %>%
  detect_threshold(
    col = 'Accuracy',
    form_id = 'v0demography',
    anomalies_id = 'hh_gps_accuracy_too_high',
    anomalies_description = glue::glue('household GPS accuracy too high'),
    threshold = 15,
    direction = 'more')

# Cluster error
anomalies_list$v0_demo5 <- data %>%
  detect_outside_cluster_boundaries(form_id = 'v0demography')

#################################
# 2. v0 demography repeat individual
#################################
data <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = 'kwale/clean-form/v0demography/v0demography-repeat_individual.csv'
)

# GPS accuracy too high
anomalies_list$v0_demo_repeat_indiv_1 <- data %>%
  detect_threshold(
    col = 'age',
    form_id = 'v0demography-repeat_individual',
    anomalies_id = 'hh_individual_age_too_high',
    anomalies_description = 'household repeat individual age more than 100',
    threshold = 101,
    direction = 'more',
    key = 'KEY')

#################################
# 3. Consolidate
#################################
final <- anomalies_list %>%
  purrr::reduce(dplyr::bind_rows) %>%
  dplyr::mutate(resolution_id = glue::glue('{form_id}__{KEY}__{anomalies_id}'),
                resolution_status = 'to_do') %>%
  dplyr::select(resolution_id, everything())
final %>%
  fwrite(OUTPUT_FILEPATH)
cloudbrewr::aws_s3_store(
  bucket = 'bohemia-lake-db',
  filename = OUTPUT_FILEPATH,
  key = glue::glue('bohemia_prod/anomalies_detection/{file}', file = basename(OUTPUT_FILEPATH))
)


#################################
# 4. Consolidate History
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
