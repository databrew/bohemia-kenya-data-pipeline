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


params = list(
  passphrase='testpw'
)

test<- httr::POST(url = 'https://databrew.org/v1/backup?passphrase=test-pw',
                  add_headers(ve))
