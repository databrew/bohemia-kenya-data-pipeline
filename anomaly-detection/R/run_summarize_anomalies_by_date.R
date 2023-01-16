# This script is purposed to summarize the partitioned anomaly files
# in S3 and create it as summary table to reduce calculation overhead
# in RShiny
# Author: atediarjo@gmail.com
library(paws)
library(dplyr)
library(lubridate)
library(data.table)
library(cloudbrewr)
source('R/anomaly_detection_function.R')

# set your pipeline stage here to define prod/dev environment
# to test it locally do Sys.setenv(PIPELINE_STAGE = 'develop')
cloudbrewr::aws_login(pipeline_stage = Sys.getenv('PIPELINE_STAGE'))

# bucket name
S3_BUCKET_NAME <- 'databrew.org'

# input
INPUT_KEY <- list(
  partition_files = 'kwale/anomalies/anomalies-identification-history/'
)

# output
OUTPUT_KEY <- list(
  summary = "kwale/anomalies/anomalies-identification-history-summary/anomalies-identification-history-summary.csv"
)


# get time series table
anomalies_summary <- cloudbrewr::aws_s3_get_table_ts(
  bucket = S3_BUCKET_NAME,
  key = INPUT_KEY$partition_files) %>%
  dplyr::group_by(run_date, type) %>%
  dplyr::summarise(number_of_anomalies = n()) %>%
  dplyr::mutate(run_date = lubridate::as_date(run_date))

# get anomalies summary
filename <- tempfile(fileext = "__summary.csv")
anomalies_summary %>%
  fwrite(filename, row.names = FALSE)

# save to s3
cloudbrewr::aws_s3_store(
  filename = filename,
  bucket = S3_BUCKET_NAME,
  key = OUTPUT_KEY$summary
)

