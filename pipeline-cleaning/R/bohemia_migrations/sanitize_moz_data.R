# Author: atediarjo@gmail.com
# This file is used to sanitize clean form from PII informaiton
# 1. Data sanitation to `sanitized-form`
# Load library
library(digest)
library(dplyr)
library(janitor)
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
logger::log_info('Run data sanitation')

# variables / creds
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
Sys.setenv(R_CONFIG_ACTIVE=env_pipeline_stage)
BUCKET_NAME <- 'bohemia-mozambique-data-repository'
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

unlink('R/bohemia_migrations/sanitation', recursive = TRUE, force = TRUE)
dir.create('R/bohemia_migrations/sanitation',recursive = TRUE)

# bulk retrieve all csv files
tryCatch({
  logger::log_info('bulk retrieval from s3')
  file_map <- cloudbrewr::aws_s3_bulk_get(
    bucket = BUCKET_NAME,
    output_dir = 'R/bohemia_migrations/sanitation'
  )
  logger::log_success('successful bulk retrieval')
}, error = function(e){
  logger::log_error(e$message)
  stop()
})


#############################
# 1. save sanitized forms
##############################

# Get Files mapping from AWS S3 sync
files_orig <- tibble::tibble(file_path =
                               list.files('R/bohemia_migrations/sanitation',
                                          full.names = TRUE,
                                          recursive = TRUE,
                                          pattern = ".csv"))  %>%
  dplyr::mutate(form_id = stringr::str_split(file_path, '/')) %>%
  dplyr::rowwise() %>%
  dplyr::mutate(
    server_id = form_id[4],
    form_id = form_id[6]) %>%
  dplyr::ungroup()


# Read local file mapping and create tibble-list inside the dataframe for
# easier indexing
tbl_nest <- files_orig %>%
  dplyr::mutate(clean = purrr::map(file_path, function(cf){
    logger::log_info(cf)
    fread(cf) %>%
      tibble::as_tibble(.name_repair = 'unique')})) %>%
  dplyr::rowwise() %>%
  dplyr::mutate(sanitized_file_path = file.path('R/bohemia_migrations/sanitation',
                                                server_id ,
                                                'sanitized-form',
                                                form_id ,basename(file_path))) %>%
  dplyr::select(file_path, sanitized_file_path, server_id, form_id, clean)


# create data sanitation folder
tbl_final_mapping <- purrr::pmap_dfr(tbl_nest,
                                     function(file_path = ..1,
                                              sanitized_file_path = ..2,
                                              server_id = ..3,
                                              form_id = ..4,
                                              clean = ..5){
                                       logger::log_info(glue::glue('Running {server_id}:{form_id}'))
                                       sanitized <- clean %>%
                                         clean_pii_columns() %>%
                                         hash_columns() %>%
                                         clean_artifacts() %>%
                                         jitter_location()

                                       dir.create(glue::glue('R/bohemia_migrations/sanitation/{server_id}/sanitized-form/{form_id}'),
                                                  recursive = TRUE,
                                                  showWarnings = FALSE)

                                       sanitized %>% fwrite(sanitized_file_path)

                                       tibble(
                                         file_path = file_path,
                                         sanitized_file_path = sanitized_file_path,
                                         server_id = server_id,
                                         form_id = form_id,
                                         clean = list(clean),
                                         sanitized = list(sanitized))
                                     })

# create zip file
tryCatch({
  folders <- tbl_final_mapping %>%
    dplyr::mutate(target_folder = dirname(sanitized_file_path)) %>%
    distinct(target_folder) %>% .$target_folder

  purrr::map(folders, function(x){
    zip_name = glue::glue('{x}/{basename(x)}.zip')
    zip(zipfile = zip_name, files = dir(x, full.names = TRUE), flags = '-r9Xj')
  })
})



# save object to s3
tryCatch({
  # do bulk store for speed
  logger::log_info('Bulk store to AWS S3')
  cloudbrewr::aws_s3_bulk_store(bucket = config::get('bucket'),
                                target = './projects/sanitized-form',
                                prefix = '/kwale/sanitized-form')

  logger::log_success('Bulk upload completed')

}, error = function(e){
  logger::log_error('Error storing to AWS S3')
  stop(e$message)
})

# stop timer
toc()

