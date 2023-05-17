#' Description:
#' This script is used for extracting data from
#' ODK Postgres and save it to DataBrew S3 bucket
#' This is script used as a temp format due to
#' physical server set up
#'
#' Author:
#' Aryton Tediarjo (atediarjo@gmail.com)
library(ruODK)
library(dplyr)
library(lubridate)
library(magrittr)
library(aws.s3)
library(paws)
library(config)
library(cloudbrewr)
library(logger)
library(config)
library(tictoc)
source("R/utils.R")

# start timer
tic()

# create log message
logger::log_info('Starting ODK Form Extraction')

# variables / creds
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
Sys.setenv(R_CONFIG_ACTIVE=env_pipeline_stage)
env_server_endpoint <- config::get('server_name')

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

# create connection to ODK
tryCatch({
  logger::log_info(glue::glue('Creating connection to {env_server_endpoint}'))

  # get ODK credentials via secrets manager
  svc  <- paws::secretsmanager()
  creds <- svc$get_secret_value(config::get('secret_name')) %>%
    .$SecretString %>%
    jsonlite::parse_json(.)
  env_odk_username <- creds$username
  env_odk_password <- creds$password

  # odk setup
  ruODK::ru_setup(fid = NULL,
                  url = env_server_endpoint,
                  un = env_odk_username,
                  pw = env_odk_password,
                  tz = 'UTC')

  logger::log_success(glue::glue('Creating connection to {env_server_endpoint} successful'))
}, error = function(e){
  logger::log_error('Connect ODK Enpoint Failed')
  stop(e$message)
})

# get projects, server name
projects <- config::get('odk_projects')
server_name <- config::get('server_name')

# create folder for output
logger::log_info('Creating output directory')
output_dir <- './odk_output_dir'
unlink(output_dir, recursive = TRUE)
dir.create(output_dir)


# create manifest and output dir
s3_manifest <- purrr::map_dfr(projects, function(project) {
  tryCatch({
    logger::log_info('Creating s3 upload manifest')
    create_s3_upload_manifest(
      server = server_name,
      project = project)
  }, error = function(e){
    logger::log_error('Error creating S3 manifest')
    stop(e$message)
  })
})


# save object to s3
tryCatch({
  # do bulk store for speed
  logger::log_info('Bulk store to AWS S3')
  cloudbrewr::aws_s3_bulk_store(bucket = config::get('bucket'),
                                target = './odk_output_dir')

  # remove directory once done
  unlink('./odk_output_dir', force = TRUE)
  logger::log_success('Bulk upload completed')

}, error = function(e){
  logger::log_error('Error storing to AWS S3')
  stop(e$message)
})

# stop timer
toc()
unlink('odk_output_dir', recursive = TRUE)
