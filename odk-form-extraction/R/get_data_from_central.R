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
source("R/utils.R")


# login to AWS
# to test Sys.setenv(PIPELINE_STAGE = 'develop')
cloudbrewr::aws_login(pipeline_stage = Sys.getenv("PIPELINE_STAGE"))

# parse ruODK credentials
CREDENTIALS_FILE_PATH <- "~/.bohemia_credentials"
credentials_check(CREDENTIALS_FILE_PATH)
creds <- yaml::yaml.load_file(Sys.getenv('bohemia_credentials'))

# get configuration
conf <- list(
  servers = c('https://databrew.org'),
  projects = c('kwale')
)

# odk setup
# Set up some parameters
ruODK::ru_setup(fid = NULL,
                url = creds$url,
                un = creds$un,
                pw = creds$pw,
                tz = 'UTC')

#' create bucket, will bypass creation if bucket already exist
create_bucket <- cloudbrewr::aws_s3_create_bucket('databrew.org')

# create s3 manifest file of all listed projects and forms from config file
s3_manifest <- create_s3_upload_manifest(
  s3obj = s3obj,
  server = conf$server,
  projects = conf$projects)


# iteratively go through manifest and save to s3
save_objects_to_s3 <- s3_manifest %>%
  dplyr::select(file_path, bucket_name, object_key) %>%
  purrr::pmap(~cloudbrewr::aws_s3_store(
    filename=..1,
    bucket=..2,
    key=..3
  ))

