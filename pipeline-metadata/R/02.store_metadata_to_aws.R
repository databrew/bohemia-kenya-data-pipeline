#' Description:
#' This script is used for extracting data from
#' ODK Postgres and save it to DataBrew S3 bucket
#' This is script used as a temp format due to
#' physical server set up
#'
#' Author:
#' Aryton Tediarjo (atediarjo@gmail.com)
library(dplyr)
library(lubridate)
library(magrittr)
library(paws)
library(config)
library(cloudbrewr)
library(logger)
library(config)
library(data.table)
library(arrow)

# create log message
logger::log_info('Starting ODK Form Extraction')

# variables / creds
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
Sys.setenv(R_CONFIG_ACTIVE=env_pipeline_stage)
env_server_endpoint <- config::get('server_name')
bucket <- config::get('bucket')

pad_hhid <- function(data){
  if('hhid' %in% names(data)){
    data %>%
      dplyr::mutate(hhid = stringr::str_pad(hhid, 5, pad = "0"))
  }else{
    data
  }
}

create_parquet <- function(data){
  if('hhid' %in% names(data)){
    data  %>%
      arrow_table() %>%
      mutate(hhid = cast(hhid, string()))
  }else{
    data
  }
}


# create connection to AWS
tryCatch({
  # login to AWS - this will be bypassed if executed in CI/CD environment
  cloudbrewr::aws_login(
    role_name = 'cloudbrewr-aws-role',
    profile_name =  'cloudbrewr-aws-role',
    pipeline_stage = env_pipeline_stage)

}, error = function(e){
  logger::log_error('AWS Login Failed')
  stop(e$message)
})



#### First, save metadata as zip files

# bulk store zip file to AWS
cloudbrewr::aws_s3_bulk_store(
  bucket = bucket,
  prefix = '/metadata',
  target_dir = 'metadata_zip_files'
)


#### Reshape metadata to parquet
unlink('parquet', recursive = TRUE, force = TRUE)
dir.create('parquet')

version <- as.character(lubridate::today())

# safety metadata household
safety_metadata_hh <- fread('safety_metadata/household_data.csv') %>%
  tidyr::drop_na(visits_done) %>%
  dplyr::rowwise() %>%
  mutate(
    most_recent_visit = stringr::str_split(visits_done, ", ") %>% .[[1]] %>% purrr::reduce(max)
  ) %>%
  dplyr::select(
    arm,
    hhid,
    num_members,
    cluster,
    household_head,
    village,
    ward,
    most_recent_visit,
    visits_done) %>%
  tibble::tibble() %>%
  pad_hhid() %>%
  dplyr::mutate(across(where(is.numeric), ~tidyr::replace_na(., -1))) %>%
  dplyr::mutate(across(where(is.character), ~tidyr::replace_na(., '')))

# safety metadata individual
safety_metadata_ind <- fread('safety_metadata/individual_data.csv') %>%
  dplyr::select(hhid,
                extid,
                firstname,
                lastname,
                sex,
                dob,
                dead,
                migrated,
                village,
                ward,
                cluster,
                starts_with('starting')) %>%
  pad_hhid()

# merge safety data
safety_arrow <- safety_metadata_hh %>%
  dplyr::select(hhid, household_head, most_recent_visit, num_members, visits_done) %>%
  dplyr::inner_join(safety_metadata_ind, by = c('hhid')) %>%
  create_parquet()

dir_target <- 'parquet/safety'
dir_target_hist <- glue::glue('parquet/safety_hist/run_date={version}')

dir.create(dir_target, recursive = TRUE)
dir.create(dir_target_hist, recursive = TRUE)
arrow::write_parquet(
  safety_arrow,
  glue::glue("{dir_target}/safety.parquet"))
arrow::write_parquet(
  safety_arrow,
  glue::glue("{dir_target_hist}/safety.parquet"))


# efficacy metadata individual
efficacy_arrow <- fread('efficacy_metadata/individual_data.csv') %>%
  dplyr::select(hhid,
                extid,
                firstname,
                lastname,
                sex,
                dob,
                dead,
                migrated,
                village,
                ward,
                cluster,
                starts_with('starting'),
                starts_with('efficacy')) %>%
  tidyr::drop_na(efficacy_visits_done) %>%
  dplyr::rowwise() %>%
  dplyr::mutate(
    most_recent_visit = stringr::str_split(efficacy_visits_done, ", ") %>% .[[1]] %>% purrr::reduce(max)
  ) %>%
  dplyr::ungroup() %>%
  pad_hhid() %>%
  dplyr::mutate(efficacy_most_recent_present_date =
                  stringr::str_remove(efficacy_most_recent_present_date, "."),
                efficacy_most_recent_present_date = case_when(efficacy_most_recent_present_date == "" ~ NA),
                efficacy_most_recent_present_date = lubridate::date(efficacy_most_recent_present_date)) %>%
  create_parquet()

dir_target <- 'parquet/efficacy'
dir_target_hist <- glue::glue('parquet/efficacy_hist/run_date={version}')

dir.create(dir_target, recursive = TRUE)
dir.create(dir_target_hist, recursive = TRUE)
arrow::write_parquet(
  efficacy_arrow,
  glue::glue("{dir_target}/efficacy.parquet"))
arrow::write_parquet(
  efficacy_arrow,
  glue::glue("{dir_target_hist}/efficacy.parquet"))



# efficacy metadata individual
arrow_tbl <- fread('icf_metadata/individual_data.csv') %>%
  pad_hhid() %>%
  create_parquet()

dir_target <- 'parquet/icf'
dir_target_hist <- glue::glue('parquet/icf_hist/run_date={version}')

dir.create(dir_target, recursive = TRUE)
dir.create(dir_target_hist, recursive = TRUE)
arrow::write_parquet(
  arrow_tbl,
  glue::glue("{dir_target}/icf.parquet"))
arrow::write_parquet(
  arrow_tbl,
  glue::glue("{dir_target_hist}/icf.parquet"))



# lab metadata individual
arrow_tbl <- fread('lab_metadata/lab_data.csv') %>%
  pad_hhid() %>%
  create_parquet()

dir_target <- 'parquet/lab'
dir_target_hist <- glue::glue('parquet/lab_hist/run_date={version}')

dir.create(dir_target, recursive = TRUE)
dir.create(dir_target_hist, recursive = TRUE)
arrow::write_parquet(
  arrow_tbl,
  glue::glue("{dir_target}/lab.parquet"))
arrow::write_parquet(
  arrow_tbl,
  glue::glue("{dir_target_hist}/lab.parquet"))


# pfu metadata individual
arrow_tbl <- fread('pfu_metadata/individual_data.csv') %>%
  pad_hhid() %>%
  create_parquet()

dir_target <- 'parquet/pfu'
dir_target_hist <- glue::glue('parquet/pfu_hist/run_date={version}')

dir.create(dir_target, recursive = TRUE)
dir.create(dir_target_hist, recursive = TRUE)
arrow::write_parquet(
  arrow_tbl,
  glue::glue("{dir_target}/pfu.parquet"))
arrow::write_parquet(
  arrow_tbl,
  glue::glue("{dir_target_hist}/pfu.parquet"))



# pk metadata individual
hh <- fread('pk_metadata/household_data.csv') %>% pad_hhid()
ind <- fread('pk_metadata/individual_data.csv') %>% pad_hhid()
arrow_tbl <- ind %>% dplyr::left_join(hh, by = c("hhid"))

dir_target <- 'parquet/pk'
dir_target_hist <- glue::glue('parquet/pk_hist/run_date={version}')

dir.create(dir_target, recursive = TRUE)
dir.create(dir_target_hist, recursive = TRUE)
arrow::write_parquet(
  arrow_tbl,
  glue::glue("{dir_target}/pk.parquet"))
arrow::write_parquet(
  arrow_tbl,
  glue::glue("{dir_target_hist}/pk.parquet"))


# healthecon baseline
hh <- fread('health_economics_metadata/healtheconbaseline_metadata/household_data.csv') %>%
  pad_hhid() %>%
  dplyr::select(-start_time)
ind <- fread('health_economics_metadata/healtheconbaseline_metadata/individual_data.csv') %>%
  pad_hhid()

arrow_tbl <- ind %>% dplyr::inner_join(hh, by = c("hhid"))

dir_target <- 'parquet/healtheconbaseline'
dir_target_hist <- glue::glue('parquet/healtheconbaseline_hist/run_date={version}')

dir.create(dir_target, recursive = TRUE)
dir.create(dir_target_hist, recursive = TRUE)
arrow::write_parquet(
  arrow_tbl,
  glue::glue("{dir_target}/healtheconbaseline.parquet"))
arrow::write_parquet(
  arrow_tbl,
  glue::glue("{dir_target_hist}/healtheconbaseline.parquet"))


# healthecon monthly
hh <- fread('health_economics_metadata/healtheconmonthly_metadata/household_data.csv') %>%
  pad_hhid() %>%
  dplyr::select(-start_time)
ind <- fread('health_economics_metadata/healtheconmonthly_metadata/individual_data.csv') %>%
  pad_hhid()

arrow_tbl <- ind %>% dplyr::inner_join(hh, by = c("hhid"))

dir_target <- 'parquet/healtheconmonthly'
dir_target_hist <- glue::glue('parquet/healtheconmonthly_hist/run_date={version}')

dir.create(dir_target, recursive = TRUE)
dir.create(dir_target_hist, recursive = TRUE)
arrow::write_parquet(
  arrow_tbl,
  glue::glue("{dir_target}/healtheconmonthly.parquet"))
arrow::write_parquet(
  arrow_tbl,
  glue::glue("{dir_target_hist}/healtheconmonthly.parquet"))



# bulk store zip file to AWS
cloudbrewr::aws_s3_bulk_store(
  bucket = 'bohemia-lake-db',
  prefix = '/bohemia_prod/metadata',
  target_dir = 'parquet'
)
