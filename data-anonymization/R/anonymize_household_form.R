library(dplyr)
library(data.table)
library(digest)
library(paws)
library(glue)
source("R/utils.R")

s3obj <- paws::s3()

OUTPUT_LIST <- list(
  bucket = glue::glue(Sys.getenv("BUCKET_PREFIX"), "databrew.org"),
  anonymized_hh_s3_key = "kwale/public-form/public-reconbhousehold/public-reconbhousehold.csv",
  hashmap_hh_s3_key = "kwale/hash-map/hash-map-reconbhousehold/hash-map-reconbhousehold.csv"
)

# get housedhold forms
hh <- get_household_forms() %>%
  dplyr::rowwise() %>%
  dplyr::mutate(hh_id_hash = digest(hh_id, algo = "sha256"))

#' hash map mapping
hash_map <- hh %>%
  dplyr::select(hh_id_hash, hh_id)

#' remove original id and use hash household id
anonymized_hh <- hh %>%
  dplyr::select(-hh_id, everything())

# household hash-map save to s3 bucket
filename <- tempfile(fileext = ".csv")
hash_map %>% fwrite(filename, row.names = FALSE)
save_to_s3_bucket(
  s3obj = s3obj,
  file_path = filename,
  bucket_name = OUTPUT_LIST$bucket,
  object_key = OUTPUT_LIST$hashmap_hh_s3_key)


# household pii mask save to s3 bucket
filename <- tempfile(fileext = ".csv")
anonymized_hh %>% fwrite(filename, row.names = FALSE)
save_to_s3_bucket(
  s3obj = s3obj,
  file_path = filename,
  bucket_name = OUTPUT_LIST$bucket,
  object_key = OUTPUT_LIST$anonymized_hh_s3_key)
