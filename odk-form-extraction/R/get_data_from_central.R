#' Description:
#' This script is used for extracting data from
#' ODK Postgres and save it to DataBrew S3 bucket
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
source("R/utils.R")


# create log message
logger::log_info('Starting ODK Form Extraction')

# variables
credentials_file_path <- "~/.bohemia_credentials"
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
env_server_endpoint <- Sys.getenv("ODK_SERVER_ENDPOINT")


# create connection to AWS
tryCatch({
  # login to AWS
  cloudbrewr::aws_login(
    role_name = 'cloudbrewr-aws-role',
    profile_name =  'cloudbrewr-aws-role',
    pipeline_stage = env_pipeline_stage)

}, error = function(e){
  logger::log_error('AWS Login Failed')
  stop(e$message)
})

# create connection to ODK
conf <- tryCatch({
  logger::log_info(glue::glue('Creating connection to {env_server_endpoint}'))

  # parse ruODK credentials
  credentials_check(credentials_file_path)
  creds <- yaml::yaml.load_file(Sys.getenv('bohemia_credentials'))

  # odk setup
  ruODK::ru_setup(fid = NULL,
                  url = env_server_endpoint,
                  un = creds$un,
                  pw = creds$pw,
                  tz = 'UTC')

  # get configuration
  list(
    servers = env_server_endpoint,
    projects = glue::glue(
      'project_forms_{stage}',
      stage = env_pipeline_stage)
  )
}, error = function(e){
  logger::log_error('Connect ODK Enpoint Failed')
  stop(e$message)
})

# create s3 manifest file of all listed projects and forms from config file
s3_manifest <- tryCatch({
  logger::log_info('Creating s3 upload manifest')
  create_s3_upload_manifest(
    server = conf$server,
    project = conf$projects)
}, error = function(e){
  logger::log_error('Error creating S3 manifest')
  stop(e$message)
})


# save object to s3
tryCatch({
  create_bucket <- cloudbrewr::aws_s3_create_bucket('databrew.org')
  save_objects_to_s3 <- s3_manifest %>%
    dplyr::select(file_path, bucket_name, object_key) %>%
    dplyr::mutate_all(as.character) %>%
    purrr::pmap(~cloudbrewr::aws_s3_store(
      filename=..1,
      bucket=..2,
      key=..3
    ))
}, error = function(e){
  logger::log_error('Error storing to AWS S3')
  stop(e$message)
})


# Finish code pipeline
logger::log_success('ODK Forms Extracted to S3')




