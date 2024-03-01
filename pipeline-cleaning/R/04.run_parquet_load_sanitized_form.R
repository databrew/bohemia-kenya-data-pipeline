# Author: atediarjo@gmail.com
# This file is used to load clean form as parquet files for athena queries
# Load library
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
library(arrow)
library(tools)
source('R/processing_utils.R')

# start timer
tic()

# create log message
logger::log_info('Serializing .csv to parquet')

# variables / creds
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
Sys.setenv(R_CONFIG_ACTIVE=env_pipeline_stage)
BUCKET_NAME <- 'databrew.org'
ROLE_NAME <- 'cloudbrewr-aws-role'


# Create connection to AWS
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
                               list.files('projects/sanitized-form',
                                          full.names = TRUE,
                                          recursive = TRUE,
                                          pattern = ".csv"))  %>%
  dplyr::mutate(form_id = stringr::str_split(file_path, '/')) %>%
  dplyr::rowwise() %>%
  dplyr::mutate(form_id = form_id[3]) %>%
  dplyr::ungroup()


# Read local file mapping and create tibble-list inside the dataframe for
# easier indexing
tbl_nest <- files_orig %>%
  dplyr::mutate(clean = purrr::map(file_path, function(cf){
    fread(cf) %>%
      tibble::as_tibble(.name_repair = 'unique')})) %>%
  dplyr::rowwise() %>%
  dplyr::mutate(parquet_file_path = glue::glue("projects/forms-pq/{folder_name}/{folder_name}.parquet",
                                               folder_name = gsub("-", "_",
                                                                  tools::file_path_sans_ext(basename(file_path))
                                               ))
  ) %>%
  dplyr::select(file_path, parquet_file_path, clean)


# create data parquet folder
unlink('projects/forms-pq', recursive = TRUE, force = TRUE)
dir.create('projects/forms-pq', recursive = TRUE)
tbl_final_mapping <- purrr::pmap(tbl_nest,
                                 function(file_path = ..1,
                                          parquet_file_path = ..2,
                                          clean = ..3){
                                   pq_data <- clean %>%
                                     pad_hhid()  %>%
                                     dplyr::select(-any_of('Status')) %>%
                                     create_parquet()

                                   dir.create(dirname(parquet_file_path),
                                              recursive = TRUE,
                                              showWarnings = FALSE)
                                   arrow::write_parquet(
                                     pq_data,
                                     parquet_file_path)
                                 })



# save object to s3
tryCatch({
  # do bulk store for speed
  logger::log_info('Bulk store parquet to AWS S3')
  cloudbrewr::aws_s3_bulk_store(bucket = 'bohemia-lake-db',
                                target = './projects/forms-pq',
                                prefix = '/bohemia_prod/sanitized_form')

  logger::log_success('Bulk upload completed')

}, error = function(e){
  logger::log_error('Error storing to AWS S3')
  stop(e$message)
})


# stop timer
toc()


