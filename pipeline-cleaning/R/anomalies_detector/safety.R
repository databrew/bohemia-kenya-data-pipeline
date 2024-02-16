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
logger::log_info('Starting Anomaly Detection for safety')

# variables / creds
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
Sys.setenv(R_CONFIG_ACTIVE=env_pipeline_stage)
BUCKET_NAME <- 'bohemia-lake-db'
ROLE_NAME <- 'cloudbrewr-aws-role'
OUTPUT_FILEPATH <- 'output/safety_anomalies_detection.csv'
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
## Fetch Safety
##############################

safety <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = glue::glue('{PROJECT_SOURCE}/clean-form/safety/safety.csv')) %>%
  pad_hhid()

safety_repeat_ae_symptom <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = glue::glue('{PROJECT_SOURCE}/clean-form/safety/safety-repeat_ae_symptom.csv')
)

safety_repeat_drug <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = glue::glue('{PROJECT_SOURCE}/clean-form/safety/safety-repeat_drug.csv')
)

safety_repeat_individual <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = glue::glue('{PROJECT_SOURCE}/clean-form/safety/safety-repeat_individual.csv')
)

safety_merged_df <- safety_repeat_individual %>%
  dplyr::inner_join(safety, by = c('PARENT_KEY'='KEY'))

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
anomalies_list$visit_already_in_dataset <- safety %>%
dplyr::group_by(visit, hhid) %>%
  dplyr::mutate(n = n(),
                key_list = paste0(KEY, collapse = ',')) %>%
  dplyr::filter(n > 1) %>%
  dplyr::mutate(form_id = 'safety',
                anomalies_id = glue::glue('hh_visit_already_in_dataset'),
                anomalies_description = glue::glue('hhid:{hhid} visit:{visit} already in dataset, please check these keys {key_list}'),
                anomalies_reports_to_wid = glue::glue('{wid}')) %>%
  dplyr::ungroup() %>%
  dplyr::select(all_of(final_col_list))


# hh gps accuracy too high
anomalies_list$detected_multiple_clusters_hh <- safety %>%
  dplyr::group_by(hhid) %>%
  dplyr::mutate(n = n_distinct(cluster),
                key_list = paste0(KEY, collapse = ',')) %>%
  dplyr::filter(n > 1) %>%
  dplyr::mutate(form_id = 'safety',
                anomalies_id = glue::glue('hh_detected_multiple_clusters'),
                anomalies_description = glue::glue('hhid:{hhid} detected multiple clusters in the same households; here are the instanceIDs: {key_list}'),
                anomalies_reports_to_wid = glue::glue('{wid}')) %>%
  dplyr::ungroup() %>%
  dplyr::select(all_of(final_col_list))


# repeat visit number selected for household
# anomalies_list$visit_already_in_dataset <- safety_merged_df %>%
#   dplyr::group_by(visit, extid) %>%
#   dplyr::mutate(
#     n = n(),
#     key_list = paste0(KEY, collapse = ',')) %>%
#   dplyr::filter(n > 1) %>%
#   dplyr::mutate(form_id = 'safety',
#                 anomalies_id = glue::glue('ind_visit_already_in_dataset'),
#                 anomalies_description = glue::glue('exitd:{extid} visit:{visit} already in dataset, please check these keys {key_list}'),
#                 anomalies_reports_to_wid = glue::glue('{wid}')) %>%
#   dplyr::ungroup() %>%
#   dplyr::select(all_of(final_col_list))



# suspicious height based on age
# anomalies_list$sus_height <- safety_merged_df %>%
#   dplyr::filter((age >= 12 & height < 100) | (age < 12 & height > 150)) %>%
#   dplyr::mutate(form_id = 'safety-repeat_individual',
#                 anomalies_id = 'ind_suspicious_height_based_on_age',
#                 anomalies_description = glue::glue('extid:{extid} is {age} years old with height {height} cm'),
#                 anomalies_reports_to_wid = glue::glue('{wid}')) %>%
#   dplyr::select(all_of(final_col_list))

# suspicious weight based on age
# anomalies_list$sus_weight <- safety_merged_df %>%
#   dplyr::filter((age >= 12 & weight < 30) | (age < 12 & weight > 50)) %>%
#   dplyr::mutate(form_id = 'safety-repeat_individual',
#                 anomalies_id = 'ind_suspicious_weight_based_on_age',
#                 anomalies_description = glue::glue('extid:{extid} is {age} years old with weight {weight} kg'),
#                 anomalies_reports_to_wid = glue::glue('{wid}')) %>%
#   dplyr::select(all_of(final_col_list))

# abnormal BMIs
# anomalies_list$sus_bmi <- safety_merged_df %>%
#   dplyr::mutate(bmi = weight / ((height/100)**2)) %>%
#   dplyr::filter(bmi < 16 | bmi > 35) %>%
#   dplyr::mutate(form_id = 'safety-repeat_individual',
#                 anomalies_id = 'ind_suspicious_bmi',
#                 anomalies_description = glue::glue('extid:{extid} has bmi of {bmi}'),
#                 anomalies_reports_to_wid = glue::glue('{wid}')) %>%
#   dplyr::select(all_of(final_col_list))

# too many people leaving household
anomalies_list$sus_died_or_migrated <- safety_merged_df %>%
  dplyr::group_by(hhid, visit) %>%
  dplyr::mutate(died = n_distinct(extid[person_absent_reason == 'Died']),
                migrated = n_distinct(extid[person_absent_reason == 'Migrated'])) %>%
  dplyr::ungroup() %>%
  dplyr::filter(died > 3 | migrated > 3) %>%
  dplyr::filter(person_absent_reason != "") %>%
  dplyr::group_by(hhid, visit) %>%
  dplyr::mutate(key_list = paste0(extid, collapse = ",")) %>%
  dplyr::mutate(form_id = 'safety',
                anomalies_id = 'hh_more_than_3_members_died_or_migrated',
                anomalies_description = glue::glue('hhid:{hhid} from visit:{visit} members {died} died and {migrated} migrated, here are the extids: {key_list}'),
                anomalies_reports_to_wid = glue::glue('{wid}'),
                KEY = PARENT_KEY) %>%
  dplyr::ungroup() %>%
  dplyr::select(all_of(final_col_list)) %>%
  dplyr::distinct()

# too many absent members
anomalies_list$sus_absent <- safety_merged_df %>%
  dplyr::group_by(hhid, visit) %>%
  dplyr::mutate(absent = n_distinct(extid[person_absent_reason == 'Absent'])) %>%
  dplyr::ungroup() %>%
  dplyr::filter(absent > 3) %>%
  dplyr::filter(person_absent_reason != "") %>%
  dplyr::group_by(hhid, visit) %>%
  dplyr::mutate(key_list = paste0(extid, collapse = ",")) %>%
  dplyr::mutate(form_id = 'safety',
                anomalies_id = 'hh_more_than_3_members_absent',
                anomalies_description = glue::glue('hhid:{hhid} from visit:{visit} members {absent} absent, here are the extids: {key_list}'),
                anomalies_reports_to_wid = glue::glue('{wid}'),
                KEY = PARENT_KEY) %>%
  dplyr::ungroup() %>%
  dplyr::select(all_of(final_col_list)) %>%
  dplyr::distinct()

# hh gps accuracy too high
anomalies_list$sus_gps <-  safety %>%
  dplyr::filter(Accuracy > 15) %>%
  dplyr::mutate(form_id = 'safety',
                anomalies_id = 'hh_gps_accuracy_too_high',
                anomalies_description = glue::glue('hhid:{hhid} gps accuracy too high: {Accuracy}'),
                anomalies_reports_to_wid = glue::glue('{wid}')) %>%
  dplyr::ungroup() %>%
  dplyr::select(all_of(final_col_list))

# out of cluster
anomalies_list$outside_cluster <- safety %>%
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


