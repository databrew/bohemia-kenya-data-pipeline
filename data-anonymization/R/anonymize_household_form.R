#' This script is used to replace hh_id with the encrypted format,
#' and enable us to share the data as an open-source data repo
#' @author atediarjo@gmail.com
#' @reviewer joe@brew.cc

# import libraries
library(dplyr)
library(data.table)
library(digest)
library(paws)
library(glue)
source("R/utils.R")

# instantiate s3
s3obj <- paws::s3()

# output list bucket and keys
OUTPUT_LIST <- list(
  bucket = glue::glue(Sys.getenv("BUCKET_PREFIX"), "databrew.org"),
  anonymized_hh_s3_key = "kwale/public-form/public-reconbhousehold/public-reconbhousehold.csv",
  hashmap_hh_s3_key = "kwale/hash-map/hash-map-reconbhousehold/hash-map-reconbhousehold.csv"
)

# get housedhold forms
anonymized_hh <- get_household_forms() %>%
  dplyr::rowwise() %>%
  dplyr::mutate(hh_id = digest(hh_id, algo = "sha256"))


# household pii mask save to s3 bucket
filename <- tempfile(fileext = ".csv")
anonymized_hh %>% fwrite(filename, row.names = FALSE)
save_to_s3_bucket(
  s3obj = s3obj,
  file_path = filename,
  bucket_name = OUTPUT_LIST$bucket,
  object_key = OUTPUT_LIST$anonymized_hh_s3_key)
