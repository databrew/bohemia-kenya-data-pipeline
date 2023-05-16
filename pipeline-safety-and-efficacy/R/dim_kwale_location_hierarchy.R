library(logger)
library(purrr)
library(dplyr)
library(cloudbrewr)
library(data.table)
library(sf)
library(lubridate)

# temp folder
temp_folder <- "/tmp"
output_filename <- 'kwale_location_hierarchy.csv'
bucket_name <- 'databrew.org'

# create log messages
logger::log_info('Extract Screening Form')

unlink(output_dir, recursive = TRUE)
dir.create(output_dir)

# variables / creds for ento
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
bucket <- 'databrew.org'
input_key <- list(
  household = 'clean-form/reconbhousehold/reconbhousehold.csv',
  cluster = 'shapefiles/clusters.zip',
  core = 'shapefiles/cores.zip',
  buffer = 'shapefiles/buffers.zip'
)


tryCatch({
  logger::log_info('Attempt AWS login')
  # login to AWS - this will be bypassed if executed in CI/CD environment
  cloudbrewr::aws_login(
    role_name = 'cloudbrewr-aws-role',
    profile_name =  'cloudbrewr-aws-role',
    pipeline_stage = env_pipeline_stage)

}, error = function(e){
  logger::log_error('AWS Login Failed')
  stop(e$message)
})


# Clean household data
household_data <- cloudbrewr::aws_s3_get_table(
  bucket = bucket,
  key = input_key$household)

# cluster object
cluster_obj <- cloudbrewr::aws_s3_get_object(
  bucket = bucket,
  key = input_key$cluster,
  output_dir = temp_folder
)

# core object
core_obj <- cloudbrewr::aws_s3_get_object(
  bucket = bucket,
  key = input_key$core,
  output_dir = temp_folder
)

# buffer object
buffer_obj <- cloudbrewr::aws_s3_get_object(
  bucket = bucket,
  key = input_key$buffer,
  output_dir = temp_folder
)



unzip(cluster_obj$file_path, exdir = temp_folder)
unzip(core_obj$file_path, exdir = temp_folder)
unzip(buffer_obj$file_path, exdir = temp_folder)

clusters <- sf::st_read(dsn = glue::glue("/{temp_folder}/clusters")) %>%
  st_transform(4326)
cores <- sf::st_read(dsn = glue::glue("/{temp_folder}/cores")) %>%
  st_transform(4326)
buffers <- sf::st_read(dsn = glue::glue("/{temp_folder}/buffers")) %>%
  st_transform(4326)

# create intersections
household_data %>%
  dplyr::select(Longitude, Latitude, ward, village) %>%
  tidyr::drop_na() %>%
  st_as_sf(., coords = c('Longitude', 'Latitude')) %>%
  st_set_crs(4326) %>%
  dplyr::mutate(
    core_number = st_intersects(.$geometry, cores$geometry),
    buffer_number = st_intersects(.$geometry, buffers$geometry),
    cluster_number = st_intersects(.$geometry, clusters$geometry)
  ) %>%
  tidyr::unnest(cluster_number, keep_empty = TRUE) %>%
  tidyr::unnest(core_number, keep_empty = TRUE) %>%
  tidyr::unnest(buffer_number, keep_empty = TRUE) %>%
  tidyr::unnest(cluster_number, keep_empty = TRUE) %>%
  tidyr::drop_na(cluster_number) %>%
  tibble::as_tibble() %>%
  dplyr::select(-geometry) %>%
  dplyr::distinct() %>%
  fwrite(output_filename)

# save dim table
cloudbrewr::aws_s3_store(
  bucket = bucket_name,
  key = 'clean-form/dim-kwale-location-hierarchy/dim-kwale-location-hierarchy.csv',
  filename = as.character(output_filename)
)

# create log messages
logger::log_success('Created dim table')
