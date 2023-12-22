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
logger::log_info('Starting Anomaly Detection for PK')

# variables / creds
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
Sys.setenv(R_CONFIG_ACTIVE=env_pipeline_stage)
BUCKET_NAME <- 'bohemia-lake-db'
ROLE_NAME <- 'cloudbrewr-aws-role'
OUTPUT_FILEPATH <- 'output/pk_anomalies_detection.csv'
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
## Fetch PK
##############################

pkday0 <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = glue::glue('{PROJECT_SOURCE}/clean-form/pkday0/pkday0.csv')) %>%
  pad_hhid() %>%
  dplyr::mutate(`KEY` = as.character(`KEY`),
                pk_id = as.character(pk_id),
                extid = as.character(extid))

pkdays123 <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = glue::glue('{PROJECT_SOURCE}/clean-form/pkdays123/pkdays123.csv')
) %>%
  pad_hhid() %>%
  dplyr::mutate(`KEY` = as.character(`KEY`),
                pk_id = as.character(pk_id),
                extid = as.character(extid))

pkfollowup <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = glue::glue('{PROJECT_SOURCE}/clean-form/pkfollowup/pkfollowup.csv')
) %>%
  pad_hhid() %>%
  dplyr::mutate(`KEY` = as.character(`KEY`),
                pk_id = as.character(pk_id),
                extid = as.character(extid))

anomalies_list <- list()
final_col_list <- c('KEY',
                    'form_id',
                    'anomalies_id',
                    'anomalies_description',
                    'anomalies_reports_to_wid')

##############################
## Anomalies Detecton
##############################

# duplicate pk_id
anomalies_list$duplicate_pkday0_pkid <- pkday0 %>%
  tidyr::drop_na(pk_id) %>%
  dplyr::group_by(pk_id) %>%
  dplyr::mutate(n = n(),
                key_list = paste0(KEY, collapse = ',')) %>%
  dplyr::filter(n > 1) %>%
  dplyr::mutate(form_id = 'pkday0',
                anomalies_id = glue::glue('pk_id_duplicates'),
                anomalies_description = glue::glue('pk_id:{pk_id} has duplicates, please check these keys {key_list}'),
                anomalies_reports_to_wid = glue::glue('{wid}')) %>%
  dplyr::ungroup() %>%
  dplyr::select(all_of(final_col_list))

# duplicate pk_id
anomalies_list$duplicate_pkdays123_pkid <- pkdays123 %>%
  dplyr::mutate(`KEY` = as.character(`KEY`)) %>%
  tidyr::drop_na(pk_id) %>%
  dplyr::group_by(pk_id,visit_day) %>%
  dplyr::mutate(n = n(),
                key_list = paste0(KEY, collapse = ',')) %>%
  dplyr::filter(n > 1) %>%
  dplyr::mutate(form_id = 'pkdays123',
                anomalies_id = glue::glue('pk_id_duplicates'),
                anomalies_description = glue::glue('pk_id:{pk_id} has duplicates, please check these keys {key_list}'),
                anomalies_reports_to_wid = glue::glue('{wid}')) %>%
  dplyr::ungroup() %>%
  dplyr::select(all_of(final_col_list))

# duplicate pk_id
anomalies_list$duplicate_pkfollowup_pkid <- pkfollowup %>%
  dplyr::mutate(`KEY` = as.character(`KEY`)) %>%
  tidyr::drop_na(pk_id) %>%
  dplyr::group_by(pk_id) %>%
  dplyr::mutate(n = n(),
                key_list = paste0(KEY, collapse = ',')) %>%
  dplyr::filter(n > 1) %>%
  dplyr::mutate(form_id = 'pkfollowup',
                anomalies_id = glue::glue('pkfollowup_id_duplicates'),
                anomalies_description = glue::glue('pk_id:{pk_id} has duplicates, please check these keys {key_list}'),
                anomalies_reports_to_wid = glue::glue('{wid}')) %>%
  dplyr::ungroup() %>%
  dplyr::select(all_of(final_col_list))


# mapping source of truth
pkday0_mapping <- pkday0 %>%
  tidyr::drop_na(pk_id) %>%
  dplyr::select(extid, pk_id) %>% distinct()

# pkdays123 mapping
anomalies_list$mapping_mismatch_pkdays123 <- pkdays123 %>%
  dplyr::select(`KEY`, extid, pk_id, wid) %>%
  dplyr::distinct() %>%
  dplyr::left_join(pkday0_mapping, by = c('pk_id')) %>%
  dplyr::filter(extid.x != extid.y) %>%
  dplyr::group_by(pk_id) %>%
  dplyr::mutate(n = n(),
                key_list = paste0(KEY, collapse = ',')) %>%
  dplyr::filter(n > 1) %>%
  dplyr::mutate(form_id = 'pkdays123',
                anomalies_id = glue::glue('pkdays123_mapping_mistmach'),
                anomalies_description = glue::glue('pk_id:{pk_id} is mapped to different extids:{extid.x} and {extid.y}, please check these keys {key_list}'),
                anomalies_reports_to_wid = glue::glue('{wid}')) %>%
  dplyr::ungroup() %>%
  dplyr::select(all_of(final_col_list))

# pkdays123 mapping
anomalies_list$mapping_mismatch_pkfollowup <- pkfollowup %>%
  dplyr::select(`KEY`, extid, pk_id, wid) %>%
  dplyr::distinct() %>%
  dplyr::left_join(pkday0_mapping, by = c('pk_id')) %>%
  dplyr::filter(extid.x != extid.y) %>%
  dplyr::group_by(pk_id) %>%
  dplyr::mutate(n = n(),
                key_list = paste0(KEY, collapse = ',')) %>%
  dplyr::filter(n > 1) %>%
  dplyr::mutate(form_id = 'pkfollowup',
                anomalies_id = glue::glue('pkdfollowup_mapping_mistmach'),
                anomalies_description = glue::glue('pk_id:{pk_id} is mapped to different extids:{extid.x} and {extid.y}, please check these keys {key_list}'),
                anomalies_reports_to_wid = glue::glue('{wid}')) %>%
  dplyr::ungroup() %>%
  dplyr::select(all_of(final_col_list))


#################################
# Consolidate
#################################
anomalies_list %>%
  purrr::reduce(dplyr::bind_rows) %>%
  fwrite(OUTPUT_FILEPATH)


