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
source("R/utils.R")


# add in credentials
CREDENTIALS_FILE_PATH <- "~/.bohemia_credentials"

# instantiate credentials, s3 object
credentials_check(CREDENTIALS_FILE_PATH)
creds <- yaml::yaml.load_file(Sys.getenv('bohemia_credentials'))
s3obj <- paws::s3()

conf <- list(
  servers = c('https://databrew.org'),
  projects = c('kwale')
)

# odk setup
# Set up some parameters
ruODK::ru_setup(
  fid = NULL,
  # fid = 'ntd',
  url = creds$url,
  un = creds$un,
  pw = creds$pw,
  tz = 'UTC'
)

# create s3 manifest file of all listed projects and forms from config file
s3_manifest <- create_s3_upload_manifest(
  s3obj = s3obj,
  server = conf$server,
  projects = conf$projects)

# create s3 buckets
create_bucket <- s3_manifest$bucket_name %>%
  head(1) %>%
  create_s3_bucket(
  s3obj = s3obj,
  bucket_name = .)

# iteratively go through manifest and save to s3
save_objects_to_s3 <- s3_manifest %>%
  purrr::pmap(~save_to_s3_bucket(
  s3obj = s3obj,
  project_name = ..2,
  fid = ..3,
  file_path=..4,
  bucket_name=..5,
  object_key=..6))

