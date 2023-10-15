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

# create log message
logger::log_info('Starting ODK Form Extraction')

# variables / creds
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
Sys.setenv(R_CONFIG_ACTIVE=env_pipeline_stage)
env_server_endpoint <- config::get('server_name')
bucket <- config::get('bucket')

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

# bulk store to AWS
cloudbrewr::aws_s3_bulk_store(
  bucket = bucket,
  prefix = '/metadata',
  target_dir = 'metadata_zip_files'
)
