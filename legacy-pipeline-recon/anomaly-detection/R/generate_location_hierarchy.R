# This script is purposed to run anomaly identification
# TBD on what kind of anomaly coding is required
# Author: atediarjo@gmail.com
library(dplyr)
library(magrittr)
library(purrr)
library(tidyr)
library(data.table)
library(glue)
source('R/anomaly_detection_function.R')

# set your pipeline stage here to define prod/dev environment
# to test it locally do Sys.setenv(PIPELINE_STAGE = 'develop')
cloudbrewr::aws_login(pipeline_stage = Sys.getenv('PIPELINE_STAGE'))

# bucket name
S3_BUCKET_NAME <- 'databrew.org'

# input
INPUT_KEY <- list(
  household = 'kwale/clean-form/reconbhousehold/reconbhousehold.csv',
  registration = "kwale/clean-form/reconaregistration/reconaregistration.csv"
)


# output
OUTPUT_KEY <- list(
  anomalies_ts_placeholder = glue::glue(
    "kwale/anomalies/anomalies-identification-history/run_date={date}/anomalies.csv",
    date = as.Date(lubridate::now()))
)


# get registration data and its anomalies
reconaregistration <- cloudbrewr::aws_s3_get_table(
  bucket = S3_BUCKET_NAME,
  key = INPUT_KEY$registration)
reconbhousehold <- cloudbrewr::aws_s3_get_table(
  bucket = S3_BUCKET_NAME,
  key = INPUT_KEY$household)

reconbhousehold %>%
  dplyr::select(sub_county, ward, community_health_unit, village) %>%
  dplyr::distinct(.keep_all = TRUE) %>%
  data.table::fwrite('kwale_location_hierarchy.csv')


