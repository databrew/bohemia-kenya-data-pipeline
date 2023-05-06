# This script is purposed to clean survey forms based on
# user input and save the clean forms to S3
# Author: atediarjo@gmail.com
library(paws)
library(dplyr)
library(magrittr)
library(purrr)
library(tidyr)
library(data.table)
library(glue)
library(googlesheets4)
library(cloudbrewr)
source('R/data_cleaning_function.R')


# set your pipeline stage here to define prod/dev environment
# to test it locally do Sys.setenv(PIPELINE_STAGE = 'develop')
cloudbrewr::aws_login(pipeline_stage = Sys.getenv('PIPELINE_STAGE'))

# variables
S3_BUCKET_NAME <- 'databrew.org'
INPUT_KEY <- list(
  household =  'raw-form/reconbhousehold/reconbhousehold.csv',
  registration = "raw-form/reconaregistration/reconaregistration.csv",
  resolution = "anomalies/anomalies-resolution/anomalies-resolution.csv"
)

# output
OUTPUT_KEY <- list(
  household = 'kwale/recon/clean-form/reconbhousehold/reconbhousehold.csv',
  registration = "kwale/recon/clean-form/reconaregistration/reconaregistration.csv"
)


# get resolution data
resolution_data <- cloudbrewr::aws_s3_get_table(
  bucket = S3_BUCKET_NAME,
  key = INPUT_KEY$resolution
)

# get registration forms and clean dataset
filename <- tempfile(fileext = ".csv")
registration <- cloudbrewr::aws_s3_get_table(
  bucket = S3_BUCKET_NAME,
  key = INPUT_KEY$registration) %>%
  clean_registration_data(
    resolution_file = resolution_data)  %>%
  fwrite(filename, row.names = FALSE)
# save registration data to s3 bucket
cloudbrewr::aws_s3_store(
  filename,
  bucket = S3_BUCKET_NAME,
  key = OUTPUT_KEY$registration)



# get household and clean household dataset
filename <- tempfile(fileext = ".csv")
household <- cloudbrewr::aws_s3_get_table(
  bucket = S3_BUCKET_NAME,
  key = INPUT_KEY$household) %>%
  clean_household_data(
    resolution_file = resolution_data)  %>%
  fwrite(filename, row.names = FALSE)
# save registration data to s3 bucket
cloudbrewr::aws_s3_store(
  filename,
  bucket = S3_BUCKET_NAME,
  key = OUTPUT_KEY$household)




household_not_curated <- cloudbrewr::aws_s3_get_table(
  bucket = S3_BUCKET_NAME,
  key = INPUT_KEY$household)

household_curated <- cloudbrewr::aws_s3_get_table(
  bucket = S3_BUCKET_NAME,
  key = INPUT_KEY$household) %>%
  clean_household_data(
    resolution_file = resolution_data)

household_curated %>%
  dplyr::select(hh_id_clean = hh_id,
                ward,
                community_health_unit,
                village,
                roof_type,
                instanceID,
                todays_date) %>%
  dplyr::left_join(household_not_curated %>% dplyr::select(instanceID, hh_id_raw = hh_id)) %>%
  dplyr::select(instanceID, todays_date, hh_id_clean, hh_id_raw, ward,
                community_health_unit, village, roof_type) %>%
  fwrite('curated_recon_household_data.csv')

cloudbrewr::aws_s3_store(
  filename = 'curated_recon_household_data.csv',
  bucket = S3_BUCKET_NAME,
  key = 'kwale/recon/clean-form/reconbhousehold/curated_recon_household_data.csv'
)
