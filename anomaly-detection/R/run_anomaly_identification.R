#' This script is purposed to run anomaly identification
#' TBD on what kind of anomaly coding is required
library(dplyr)
library(magrittr)
library(purrr)
library(tidyr)
library(data.table)
library(glue)
source('R/utils.R')
source('R/anomaly_detection_function.R')

svc <- paws::s3()
S3_BUCKET_NAME <- glue::glue(
  Sys.getenv('BUCKET_PREFIX'),
  'databrew.org')
HH_S3_FILE_KEY <- 'kwale/clean-form/reconbhousehold/reconbhousehold.csv'
REGISTRATION_S3_FILE_KEY <- "kwale/clean-form/reconaregistration/reconaregistration.csv"
ANOMALIES_S3_FILE_KEY <- "kwale/anomalies/anomalies.csv"

get_registration_data <- function(){
  # Kwale Registration Forms
  filename <- tempfile(fileext = ".csv")
  bucket_name <- S3_BUCKET_NAME
  get_s3_data(
    s3obj = svc,
    bucket= S3_BUCKET_NAME,
    object_key = REGISTRATION_S3_FILE_KEY, # change this to clean data
    filename = filename) %>%
    fread(.) %>%
    as_tibble()
}

get_household_data <- function(){
  # Kwale Household Forms
  filename <- tempfile(fileext = ".csv")
  get_s3_data(
    s3obj = svc,
    bucket= S3_BUCKET_NAME,
    object_key = HH_S3_FILE_KEY, # change this to clean data
    filename = filename) %>%
    fread(.) %>%
    as_tibble()
}


# get registration data and its anomalies
reconaregistration <- get_registration_data()
reconbhousehold <- get_household_data()

current_anomaly_list <- dplyr::bind_rows(
  reconaregistration %>%
    get_duplicated_chv_id(.),
  reconaregistration %>%
    get_identical_cha_chv(.),
  reconbhousehold %>%
    get_duplicated_hh_id(.),
  reconaregistration %>%
    get_mistmached_cha_chv_numbers(.)
  ###############################################################
  # append more here, place function in anomaly_detection_function
  ###############################################################
) %>%
  dplyr::mutate(run_date = as.Date(lubridate::now()))


# save data to s3
partition_key = glue::glue("kwale/anomalies/anomalies-identification-history/run_date={date}/anomalies.csv",
                 date = as.Date(lubridate::now()))

# save as timestamp table
filename <- tempfile(fileext = ".csv")
current_anomaly_list %>%
  fwrite(filename, row.names = FALSE)
save_to_s3_bucket(
  s3obj = svc,
  file_path = filename,
  bucket_name = S3_BUCKET_NAME,
  object_key = partition_key)


# stopgap: save as is (will be deprecated) as we need time-series data to track
# fieldworker performance
filename <- tempfile(fileext = ".csv")
current_anomaly_list %>%
  fwrite(filename, row.names = FALSE)
save_to_s3_bucket(
  s3obj = svc,
  file_path = filename,
  bucket_name = S3_BUCKET_NAME,
  object_key = ANOMALIES_S3_FILE_KEY)

