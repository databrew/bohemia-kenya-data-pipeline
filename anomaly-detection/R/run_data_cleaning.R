#' @description: This script is purposed to clean survey forms and save to s3
#' @author: atediarjo@gmail.com
library(paws)
library(dplyr)
library(magrittr)
library(purrr)
library(tidyr)
library(data.table)
library(glue)
library(googlesheets4)
source('R/utils.R')
source('R/data_cleaning_function.R')

svc <- paws::s3()
S3_BUCKET_NAME <- glue::glue(Sys.getenv('BUCKET_PREFIX'), 'databrew.org')

INPUT_KEY <- list(
  household =  'kwale/raw-form/reconbhousehold/reconbhousehold.csv',
  registration = "kwale/raw-form/reconaregistration/reconaregistration.csv",
  resolution = "kwale/anomalies/anomalies-resolution/anomalies-resolution.csv"
)

OUTPUT_KEY <- list(
  household = 'kwale/clean-form/reconbhousehold/reconbhousehold.csv',
  registration = "kwale/clean-form/reconaregistration/reconaregistration.csv"
)


# read local resolution file
get_resolution <- function() {
  filename <- tempfile()
  bucket_name <- S3_BUCKET_NAME
  get_s3_data(
    s3obj = svc,
    bucket= S3_BUCKET_NAME,
    object_key = INPUT_KEY$resolution, # change this to clean data
    filename = filename) %>%
    fread(.) %>%
    as_tibble() %>%
    as_tibble(.name_repair = 'unique')
}


get_registration_data <- function(){
  # Kwale Registration Forms
  filename <- tempfile()
  bucket_name <- S3_BUCKET_NAME
  get_s3_data(
    s3obj = svc,
    bucket= S3_BUCKET_NAME,
    object_key = INPUT_KEY$registration, # change this to clean data
    filename = filename) %>%
    fread(.) %>%
    as_tibble() %>%
    as_tibble(.name_repair = 'unique')
}

get_household_data <- function(){
  # Kwale Household Forms
  filename <- tempfile()
  get_s3_data(
    s3obj = svc,
    bucket= S3_BUCKET_NAME,
    object_key = INPUT_KEY$household, # change this to clean data
    filename = filename) %>%
    fread(.) %>%
    as_tibble() %>%
    as_tibble(.name_repair = 'unique')
}

# get resolution data
resolution_data <- get_resolution()


# get registration forms and clean dataset
filename <- tempfile(fileext = ".csv")
registration <- get_registration_data() %>%
  clean_registration_data(., resolution_data)

# save registration file
registration %>%
  fwrite(filename, row.names = FALSE)
save_to_s3_bucket(
  s3obj = svc,
  file_path = filename,
  bucket_name = S3_BUCKET_NAME,
  object_key = OUTPUT_KEY$registration)


# get household forms and clean dataset
filename <- tempfile(fileext = ".csv")

# clean household
household <- get_household_data() %>%
  clean_household_data(., resolution_data)

# save to s3 bucket
household %>% fwrite(filename, row.names = FALSE)
save_to_s3_bucket(
  s3obj = svc,
  file_path = filename,
  bucket_name = S3_BUCKET_NAME,
  object_key = OUTPUT_KEY$household)

