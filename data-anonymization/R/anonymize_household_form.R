# This script is used to replace hh_id with the encrypted format,
# and enable us to share the data as an open-source data repo
# @author atediarjo@gmail.com

# import libraries
library(dplyr)
library(data.table)
library(digest)
library(paws)
library(glue)

# set your pipeline stage here to define prod/dev environment
# to test it locally do Sys.setenv(PIPELINE_STAGE = 'develop')
cloudbrewr::aws_login(pipeline_stage = Sys.getenv('PIPELINE_STAGE'))

# bucket name
S3_BUCKET_NAME <- 'databrew.org'

# input
INPUT_KEY <- list(
  household = 'kwale/clean-form/reconbhousehold/reconbhousehold.csv'
)

# output list keys
OUTPUT_KEY <- list(
  anonymized_household = "kwale/public-form/public-reconbhousehold/public-reconbhousehold.csv"
)

# get housedhold forms
anonymized_hh <- cloudbrewr::aws_s3_get_table(
  bucket = S3_BUCKET_NAME,
  key = INPUT_KEY$household) %>%
  dplyr::rowwise() %>%
  dplyr::mutate(hh_id = digest(hh_id, algo = "sha256"))


# household pii mask save to s3 bucket
filename <- tempfile(fileext = ".csv")
anonymized_hh %>% fwrite(filename, row.names = FALSE)
# save registration data to s3 bucket
cloudbrewr::aws_s3_store(
  filename,
  bucket = S3_BUCKET_NAME,
  key = OUTPUT_KEY$anonymized_household)
