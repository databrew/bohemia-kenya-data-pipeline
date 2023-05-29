library(logger)
library(purrr)
library(dplyr)
library(cloudbrewr)
library(data.table)
library(sf)
library(sp)
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
  recon = 'clean-form/reconbhousehold/reconbhousehold.csv',
  household = 'shapefiles/households.zip',
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


recon_raw <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = 'raw-form/reconbhousehold.csv')
recon <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = 'clean-form/reconbhousehold/reconbhousehold.csv') %>%
  dplyr::select(hh_id_clean = hh_id,
                ward,
                community_health_unit,
                village,
                roof_type,
                instanceID,
                todays_date) %>%
  dplyr::left_join(recon_raw %>% dplyr::select(instanceID, hh_id_raw = hh_id)) %>%
  dplyr::select(instanceID, todays_date, hh_id_clean, hh_id_raw, ward,
                community_health_unit, village, roof_type)

# household object
hh_obj <- cloudbrewr::aws_s3_get_object(
  bucket = bucket,
  key = input_key$household,
  output_dir = temp_folder
)


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


unzip(hh_obj$file_path, exdir = temp_folder)
unzip(cluster_obj$file_path, exdir = temp_folder)
unzip(core_obj$file_path, exdir = temp_folder)
unzip(buffer_obj$file_path, exdir = temp_folder)

hhsp <- rgdal::readOGR('/tmp/households/', 'households')
clusters <- rgdal::readOGR('/tmp/clusters/', 'clusters')
cores <- rgdal::readOGR('/tmp/cores/', 'cores')
buffers <- rgdal::readOGR('/tmp/buffers/', 'buffers')


# Determine which households are in which clusters, cores, buffers
o <- sp::over(hhsp,
              polygons(clusters))
hhsp@data$cluster_number <- clusters@data$cluster_nu[o]
o <- sp::over(hhsp, polygons(cores))
hhsp@data$core_number <- cores@data$cluster_nu[o]
o <- sp::over(hhsp, polygons(buffers))
hhsp@data$buffer_number <- buffers@data$cluster_nu[o]
hhsp@data$in_core <-
  !is.na(hhsp@data$core_number)

# Get the raw/uncorrected ID into the households data
hhsp@data <- left_join(hhsp@data %>% dplyr::select(-village,
                                                    -ward,
                                                    -community0),
                             recon %>%
                                dplyr::select(
                                  hh_id = hh_id_clean,
                                  hh_id_raw,
                                  village,
                                  ward,
                                  community_unit = community_health_unit
                             ))

# create location hierarchy
hhsp@data %>%
  dplyr::select(
    ward,
    village,
    cluster_number
  ) %>%
  tibble::as_tibble() %>%
  dplyr::mutate_all(as.character) %>%
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
