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


# create log message
message(glue::glue("[{lubridate::now()} BOHEMIA_PIPELINE_LOGS]: ", 'Starting ODK Extraction'))

# login to AWS
# to test Sys.setenv(PIPELINE_STAGE = 'develop')
cloudbrewr::aws_login(pipeline_stage = Sys.getenv("PIPELINE_STAGE"))

# parse ruODK credentials
CREDENTIALS_FILE_PATH <- "~/.bohemia_credentials"
credentials_check(CREDENTIALS_FILE_PATH)
creds <- yaml::yaml.load_file(Sys.getenv('bohemia_credentials'))

# get configuration
# add project name to this list to get more extraction
conf <- list(
  servers = c('https://databrew.org'),
  projects = c('kwale', 'Kwale Ento Testing')
)

# odk setup
# Set up some parameters
ruODK::ru_setup(fid = NULL,
                url = creds$url,
                un = creds$un,
                pw = creds$pw,
                tz = 'UTC')

# create bucket, will bypass creation if bucket already exist
create_bucket <- cloudbrewr::aws_s3_create_bucket('databrew.org')

# create s3 manifest file of all listed projects and forms from config file
s3_manifest <- create_s3_upload_manifest(
  s3obj = s3obj,
  server = conf$server,
  projects = conf$projects) %>%
  dplyr::select(file_path, bucket_name, object_key)


# iteratively go through manifest and save to s3
save_objects_to_s3 <- s3_manifest %>%
  dplyr::select(file_path, bucket_name, object_key) %>%
  dplyr::mutate_all(as.character) %>%
  purrr::pmap(~cloudbrewr::aws_s3_store(
    filename=..1,
    bucket=..2,
    key=..3
  ))

# indicate finished data extraction
message(glue::glue("[{lubridate::now()} BOHEMIA_PIPELINE_LOGS]: ", 'Cleaning Temp Folder'))
unlink('temp_form_output_file', recursive = T)

# Finish code pipeline
message(glue::glue("[{lubridate::now()} BOHEMIA_PIPELINE_LOGS]: ", 'ODK Forms Extracted to S3'))

