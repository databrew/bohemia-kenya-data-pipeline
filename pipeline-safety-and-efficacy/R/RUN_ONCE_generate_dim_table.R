# This script is used to be run once for generating all
# summary tables required for cluster goals, location hierarchy etc
# takes in data from current households and all the cluster, cores, and buffer information
# Author: atediarjo@gmail.com

library(paws)
library(dplyr)
library(readr)
library(ggplot2)
library(rgdal)
library(raster)
library(ggthemes)
library(sp)
library(rgeos)
library(maptools)
library(data.table)


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
bucket_spatial <- 'bohemia-spatial-assets'
input_key <- list(
  recon = 'kwale/clean-form/reconbhousehold/reconbhousehold.csv',
  registration = 'kwale/clean-form/reconaregistration/reconaregistration.csv',
  household = 'kwale/households.zip',
  cluster = 'kwale/clusters.zip',
  core = 'kwale/cores.zip',
  buffer = 'kwale/buffers.zip'
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
  bucket = bucket,
  key = 'kwale/raw-form/reconbhousehold/reconbhousehold.csv')
recon_clean <- cloudbrewr::aws_s3_get_table(
  bucket = bucket,
  key = 'kwale/clean-form/reconbhousehold/reconbhousehold.csv')

recon <-  recon_clean %>%
  dplyr::mutate(wid = as.character(coalesce(wid_manual, wid_qr))) %>%
  dplyr::select(hh_id_clean = hh_id,
                ward,
                community_health_unit,
                village,
                roof_type,
                instanceID,
                todays_date,
                wid,
                Longitude,
                Latitude) %>%
  dplyr::left_join(recon_raw %>%
                     dplyr::select(instanceID = KEY,
                                   hh_id_raw = `group_hh_id-hh_id`,
                                   contains('num_hh_members'))) %>%
  dplyr::select(instanceID,
                todays_date,
                hh_id_clean,
                hh_id_raw,
                ward,
                community_health_unit,
                village,
                roof_type,
                wid,
                num_hh_members = `group_hh_info-num_hh_members`,
                num_hh_members_lt_5 = `group_hh_info-num_hh_members_lt_5`,
                num_hh_members_bt_5_15 = `group_hh_info-num_hh_members_bt_5-15`,
                num_hh_members_gt_15 = `group_hh_info-num_hh_members_gt_15`,
                Longitude,
                Latitude)
cha_matcher <- cloudbrewr::aws_s3_get_table(
  bucket = bucket,
  key = input_key$registration) %>%
  filter(worker_type == 'CHV') %>%
  dplyr::mutate(wid_cha = as.character(coalesce(cha_wid_manual, cha_wid_qr)),
                wid = as.character(wid)) %>%
  dplyr::select(wid, wid_cha) %>%
  filter(!is.na(wid_cha)) %>%
  dplyr::distinct(wid, .keep_all = TRUE)



# get all spatial objects

# household object
# Convert household locations to spatial
ken3 <- raster::getData(name = 'GADM', download = TRUE, country = 'KEN', level = 3)
households <- recon_clean %>%
  filter(!is.na(Longitude)) %>%
  mutate(x = Longitude, y = Latitude) %>%
  dplyr::distinct(hh_id, .keep_all = TRUE)
hhsp <- households
coordinates(hhsp) <- ~x+y
proj4string(hhsp) <- proj4string(ken3)

# cluster object
cluster_obj <- cloudbrewr::aws_s3_get_object(
  bucket = bucket_spatial,
  key = input_key$cluster,
  output_dir = temp_folder
)

# core object
core_obj <- cloudbrewr::aws_s3_get_object(
  bucket = bucket_spatial,
  key = input_key$core,
  output_dir = temp_folder
)

# buffer object
buffer_obj <- cloudbrewr::aws_s3_get_object(
  bucket = bucket_spatial,
  key = input_key$buffer,
  output_dir = temp_folder
)


unzip(cluster_obj$file_path, exdir = temp_folder)
unzip(core_obj$file_path, exdir = temp_folder)
unzip(buffer_obj$file_path, exdir = temp_folder)

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
hh_with_cluster <- left_join(hhsp@data %>%
                               dplyr::select(-village,
                                             -ward,
                                             -community_health_unit),
                             recon %>%
                                dplyr::select(
                                  hh_id = hh_id_clean,
                                  hh_id_raw,
                                  village,
                                  ward,
                                  community_health_unit,
                                  everything()
                             ))


# Dim Table 1: Dim Cluster Goals
output_filename <- '/tmp/dim_kwale_cluster_goal.csv'
cluster_goal <- hh_with_cluster %>%
  left_join(cha_matcher) %>%
  # filter out those which are not in study
  filter(!is.na(cluster_number))%>%
  group_by(cluster_number) %>%
  summarise(households_core_and_buffer = n(),
            households_core = length(which(!is.na(core_number))),
            households_buffer = length(which(!is.na(buffer_number))),
            members_core_and_buffer = sum(num_hh_members),
            members_core = sum(num_hh_members[!is.na(core_number)]),
            members_buffer = sum(num_hh_members[!is.na(buffer_number)]),
            members_gt_5_core_and_buffer = sum(num_hh_members_bt_5_15 + num_hh_members_gt_15),
            members_bt_5_15_core = sum(num_hh_members_bt_5_15[!is.na(core_number)]),
            members_bt_5_15_buffer = sum(num_hh_members_bt_5_15[is.na(core_number)]),
            community_health_units = paste0(sort(unique(community_health_unit)), collapse = ';'),
            wards = paste0(sort(unique(ward)), collapse = ';'),
            villages = paste0(sort(unique(village)), collapse = ';'),
            chvs = paste0(sort(unique(wid)), collapse = ';'),
            chas = paste0(sort(unique(wid_cha)), collapse = ';')) %>%
  dplyr::mutate(cluster_number = as.integer(cluster_number)) %>%
  fwrite(output_filename)
# save dim table
cloudbrewr::aws_s3_store(
  bucket = 'bohemia-lake-db',
  key = 'bohemia_prod/dim_kwale_cluster_goal/dim_kwale_cluster_goal.csv',
  filename = as.character(output_filename)
)


# Dim Table 2: Dim village hierarchy
output_filename <- '/tmp/dim_kwale_location_hierarchy.csv'
hh_with_cluster %>%
  dplyr::select(
    ward,
    village,
    cluster_number
  ) %>%
  tibble::as_tibble() %>%
  dplyr::mutate(cluster_number = as.integer(cluster_number)) %>%
  dplyr::distinct() %>%
  tidyr::drop_na(cluster_number) %>%
  fwrite(output_filename)


# save dim table
cloudbrewr::aws_s3_store(
  bucket = 'bohemia-lake-db',
  key = 'bohemia_prod/dim_kwale_location_hierarchy/dim_kwale_location_hierarchy.csv',
  filename = as.character(output_filename)
)

# create log messages
logger::log_success('Created dim tables for SE')
