library(logger)
library(purrr)
library(dplyr)
library(cloudbrewr)
library(data.table)
library(sf)
library(lubridate)
library(rgdal)
library(geodata)
library(ggthemes)
library(sp)
library(raster)


# create log messages
logger::log_info('Extract Screening Form')

output_dir <-'report/clean_form'
bucket_name <- 'databrew.org'

unlink(output_dir, recursive = TRUE)
dir.create(output_dir)

# variables / creds for ento
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
bucket <- 'databrew.org'
input_key <- list(
  screening = 'kwale/clean-form/entoscreeningke/entoscreeningke.csv',
  mosquito = 'kwale/clean-form/entoltmorphid/entoltmorphid.csv',
  resting = 'kwale/clean-form/entorcmorphid/entorcmorphid.csv'
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

# unzip assets
unzip('assets/cores.zip')
unzip('assets/buffers.zip')
unzip('assets/clusters.zip')
cores <- rgdal::readOGR('cores/', 'cores')
buffers <- rgdal::readOGR('buffers/', 'buffers')
clusters <- rgdal::readOGR('clusters/', 'clusters')
ken3 <- geodata::gadm('KEN', level = 3, path = ".")

# Read from ento cluster asset (@joebrew)
ento_clusters <- fread('assets/ento_clusters.csv') %>%
  dplyr::mutate(cluster = sprintf("%02.0f", cluster_number))

# Form Monitoring 1 Recruitment and Withdrawals
data <- cloudbrewr::aws_s3_get_table(
  bucket = bucket,
  key = input_key$screening) %>%
  dplyr::filter(
    site != "Larval Habitat")
orig_les <- data %>%
  dplyr::filter(orig_le != "") %>% .$orig_le
orig_hhid <- data %>%
  dplyr::filter(orig_hhid != "") %>% .$orig_hhid
le_data <- data %>%
  dplyr::filter(leid != "") %>%
  dplyr::select(id = leid,
                orig_le,
                todays_date,
                site,
                village,
                Longitude,
                Latitude) %>%
  dplyr::mutate(
    todays_date = lubridate::date(todays_date),
    id_type = 'livestock_enclosure',
    id = as.character(id)
  )
hh_data <- data %>%
  dplyr::filter(hhid != "") %>%
  dplyr::select(id = hhid,
                orig_hhid,
                todays_date,
                site,
                village,
                Longitude,
                Latitude) %>%
  dplyr::mutate(
    todays_date = lubridate::date(todays_date),
    id_type = 'household',
    id = as.character(id)
    )

# unzip assets
unzip('assets/cores.zip')
unzip('assets/buffers.zip')
unzip('assets/clusters.zip')
cores <- rgdal::readOGR('cores/', 'cores')
buffers <-rgdal::readOGR('buffers/', 'buffers')
clusters <- rgdal::readOGR('clusters/', 'clusters')

# get master data
base_tbl <- dplyr::bind_rows(le_data, hh_data)
base_tbl <- base_tbl
coordinates(base_tbl) <- ~Longitude+Latitude
proj4string(base_tbl) <- proj4string(as(ken3, "Spatial"))


# Determine which households are in which clusters, cores, buffers
o <- sp::over(base_tbl, polygons(clusters))
base_tbl@data$cluster_number <- clusters@data$cluster_nu[o]
o <- sp::over(base_tbl, polygons(cores))
base_tbl@data$core_number <- cores@data$cluster_nu[o]
o <- sp::over(base_tbl, polygons(buffers))
base_tbl@data$buffer_number <- buffers@data$cluster_nu[o]
base_tbl@data$in_core <-
  !is.na(base_tbl@data$core_number)

base_tbl <- base_tbl@data



# Form Monitoring 1: Recruitment and Withdrawals
withdrawals <- base_tbl %>%
  dplyr::filter(orig_le != "" | orig_hhid != "") %>%
  dplyr::mutate(active_or_withdrawn = 'withdrawn',
                withdrawal_date = todays_date)

active <- base_tbl %>%
  dplyr::anti_join(withdrawals, by = 'orig_le') %>%
  dplyr::anti_join(withdrawals, by = 'orig_hhid') %>%
  dplyr::mutate(active_or_withdrawn = 'active',
                withdrawal_date = NA_Date_)

monitoring_tbl <- dplyr::bind_rows(active, withdrawals) %>%
  dplyr::mutate(id = as.character(id))

output_filename <- glue::glue('{output_dir}/summary_ento_recruitment_withdrawal.csv')
monitoring_tbl %>%
  dplyr::select(
    cluster_number,
    id,
    todays_date,
    site,
    active_or_withdrawn,
    withdrawal_date) %>%
  fwrite(output_filename)

cloudbrewr::aws_s3_store(
  bucket = 'bohemia-lake-db',
  key = 'bohemia_prod/summary_ento_recruitment_withdrawal/summary_ento_recruitment_withdrawal.csv',
  filename = as.character(output_filename)
)


# Form Monitoring 3: Light Trap Mosquito Collections
# data_light_trap <- cloudbrewr::aws_s3_get_object(
#   bucket = bucket,
#   key = input_key$mosquito) %>%
#   .$file_path %>%
#   data.table::fread()  %>%
#   subset(., select=which(!duplicated(names(.)))) %>%
#   dplyr::mutate(month_date = lubridate::ceiling_date(todays_date, unit = "months")) %>%
#   tibble::as_tibble()
#
# data_light_trap_hh <- data_light_trap %>%
#   dplyr::group_by(month_date, site) %>%
#   dplyr::summarise(num_hh_submitted_from_buffer = n_distinct(hhid),
#                    num_hh_submitted_from_core = n_distinct(hhid)) %>%
#   dplyr::ungroup() %>%
#   dplyr::mutate(cluster = NA,
#                 arm = NA) %>%
#   dplyr::select(cluster,
#                 month_date,
#                 arm,
#                 collection_site = site,
#                 num_hh_submitted_from_buffer,
#                 num_hh_submitted_from_core
#                 ) %>%
#   fwrite('report/clean_form/ento_monitoring_light_trap_households.csv')
#
#
# data_light_trap_dissected <- data_light_trap %>%
#   dplyr::group_by(month_date) %>%
#   dplyr::summarise(num_dissected_for_parity = sum(num_dissected_for_parity, na.rm = T)) %>%
#   dplyr::ungroup() %>%
#   dplyr::mutate(cluster = NA,
#                 arm = NA) %>%
#   dplyr::select(cluster,
#                 month_date,
#                 arm,
#                 num_dissected_for_parity
#   ) %>%
#   fwrite('report/clean_form/ento_monitoring_light_trap_dissected_for_parity.csv')
#
#
#
#
# # Form Monitoring 4: Resting Collections
# data_resting_collections <- cloudbrewr::aws_s3_get_object(
#   bucket = bucket,
#   key = input_key$resting) %>%
#   .$file_path %>%
#   data.table::fread()  %>%
#   subset(., select=which(!duplicated(names(.)))) %>%
#   dplyr::mutate(month_date = lubridate::ceiling_date(todays_date, unit = "months")) %>%
#   tibble::as_tibble() %>%
#   dplyr::mutate(cluster = NA,
#                 arm = NA)
#
# data_resting_collections_1 <- data_resting_collections %>%
#   group_by(month_date, hh_or_pit) %>%
#   dplyr::summarise(
#     num_hh_submitted_from_buffer = n_distinct(hhid),
#     num_hh_submitted_from_core = n_distinct(hhid)) %>%
#   dplyr::ungroup() %>%
#   dplyr::mutate(
#     cluster = NA,
#     arm = NA,
#   ) %>%
#   dplyr::select(cluster,
#                 month_date,
#                 arm,
#                 collection_site = hh_or_pit,
#                 num_hh_submitted_from_buffer,
#                 num_hh_submitted_from_core
#                 ) %>%
#   fwrite('report/clean_form/ento_monitoring_resting_collection_households.csv')
#
#
#
# data_resting_collections_2 <- data_resting_collections %>%
#   group_by(month_date) %>%
#   dplyr::summarise(
#     num_kept_for_survival_gonotrophic_cycle = sum(num_kept_for_survival_gonotrophic_cycle, na.rm = T)) %>%
#   dplyr::ungroup() %>%
#   dplyr::mutate(
#     cluster = NA,
#     arm = NA,
#   ) %>%
#   dplyr::select(cluster,
#                 month_date,
#                 arm,
#                 num_kept_for_survival_gonotrophic_cycle
#   ) %>%
#   fwrite('report/clean_form/ento_monitoring_resting_collection_survival.csv')

