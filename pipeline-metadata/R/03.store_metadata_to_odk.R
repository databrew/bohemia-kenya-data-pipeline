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
  logger::log_success(glue::glue('Creating connection to {env_server_endpoint} successful'))
}, error = function(e){
  logger::log_error('Connect ODK Enpoint Failed')
  stop(e$message)
})




purrr::map(config::get('target'), function(obj){
  # unzip file
  target_dir <- glue::glue('/tmp')
  unlink(target_dir, recursive = TRUE, force = TRUE)
  dir.create(target_dir, recursive = TRUE)
  cloudbrewr::aws_s3_get_object(
    bucket = config::get('bucket'),
    key = obj$s3uri) %>%
    .$file_path %>%
    unzip(exdir = target_dir)

  target_file <- glue::glue('{target_dir}/{obj$fs[[1]]}')
  basic_authentication <- httr::authenticate(
    env_odk_username,
    env_odk_password)


  # Create a draft form
  # (the below fails if not already created on the server)
  logger::log_info('Making new draft')
  res <- httr::RETRY("POST",
                     paste0(env_server_endpoint,
                            "/v1/projects/",
                            config::get('pid'),
                            "/forms/",
                            obj$fid,
                            "/draft"),
                     basic_authentication
  ) %>%
    httr::content(.)


  logger::log_info('Uploading individual_data.csv')
  res <- httr::RETRY("POST",
                     paste0(env_server_endpoint,
                            "/v1/projects/",
                            config::get('pid'),
                            "/forms/",
                            obj$fid,
                            glue::glue("/draft/attachments/{basename(target_file)}")),
                     body = httr::upload_file(target_file),
                     basic_authentication
  )


  new_version <- strftime(lubridate::today(), format = "%y%m%d01")
  logger::log_info('Publishing new version with updated data as version ', new_version)
  res <- httr::RETRY("POST",
                     paste0(env_server_endpoint,
                            "/v1/projects/",
                            config::get('pid'),
                            "/forms/",
                            obj$fid,
                            "/draft/publish?version=",new_version),
                     basic_authentication) %>%
    httr::content(.)
})


logger::log_info('Metadata update succeed')
