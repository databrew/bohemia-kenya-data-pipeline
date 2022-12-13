library(paws)
library(dplyr)
library(lubridate)
library(data.table)
source('R/utils.R')
source('R/anomaly_detection_function.R')


# variables
s3obj <- paws::s3()
S3_BUCKET_NAME <- glue::glue(
  Sys.getenv('BUCKET_PREFIX'),
  'databrew.org')
OUTPUT_KEY <- "kwale/anomalies/anomalies-identification-history-summary/anomalies-identification-history-summary.csv"


# get anomalies summary
filename <- tempfile(fileext = "__summary.csv")
anomalies_summary <- s3obj$list_objects_v2(
  Bucket = S3_BUCKET_NAME,
  Prefix = 'kwale/anomalies/anomalies-identification-history/') %>%
  .$Contents %>%
  purrr::map_dfr(function(content){
    get_s3_data(
      s3obj = s3obj,
      bucket= S3_BUCKET_NAME,
      object_key = content$Key, # change this to clean data
      filename = filename) %>%
      fread() %>%
      as_tibble() %>%
      dplyr::group_by(run_date, type) %>%
      dplyr::summarise(number_of_anomalies = n()) %>%
      dplyr::mutate(run_date = lubridate::as_date(run_date))
  })


# save files to s3
filename <- tempfile(fileext = ".csv")
anomalies_summary %>%
  fwrite(filename, row.names = FALSE)
save_to_s3_bucket(
  s3obj = s3obj,
  file_path = filename,
  bucket_name = S3_BUCKET_NAME,
  object_key = OUTPUT_KEY)

