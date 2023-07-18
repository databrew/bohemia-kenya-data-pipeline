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

# create directory
output_dir <-'report/clean_form'
unlink(output_dir, recursive = TRUE)
dir.create(output_dir)

# variables / creds for ento
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
input_bucket <- 'databrew.org'
output_bucket <- 'bohemia-lake-db'
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
  bucket = input_bucket,
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
    id = as.character(id),
    orig_le = as.character(orig_le),
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
    id = as.character(id),
    orig_hhid = as.character(orig_hhid),
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


##########################################
# create tables here
##########################################

# Form Monitoring 1: Recruitment and Withdrawals
withdrawals <- base_tbl %>%
  dplyr::filter(orig_le != "" | orig_hhid != "") %>%
  dplyr::mutate(active_or_withdrawn = 'withdrawn',
                date_of_withdrawal = todays_date)

active <- base_tbl %>%
  dplyr::anti_join(withdrawals, by = 'orig_le') %>%
  dplyr::anti_join(withdrawals, by = 'orig_hhid') %>%
  dplyr::mutate(active_or_withdrawn = 'active',
                date_of_withdrawal = NA_Date_)

monitoring_tbl_base <- dplyr::bind_rows(active, withdrawals) %>%
  dplyr::mutate(id = as.character(id))

output_filename <- glue::glue('{output_dir}/summary_ento_recruitment_withdrawal.csv')
monitoring_tbl <- monitoring_tbl_base %>%
  dplyr::select(
    cluster_number,
    id,
    date_of_consent = todays_date,
    collection_method = site,
    active_or_withdrawn,
    date_of_withdrawal) %>%
  distinct() %>%
  tidyr::drop_na(cluster_number)

monitoring_tbl %>%
  fwrite(output_filename)

cloudbrewr::aws_s3_store(
  bucket = output_bucket,
  key = 'bohemia_prod/summary_ento_recruitment_withdrawal/summary_ento_recruitment_withdrawal.csv',
  filename = as.character(output_filename)
)


# Form Monitoring 2: ICF List
s3_catalog <- cloudbrewr::aws_s3_get_catalog(bucket = output_bucket)
output_filename <- glue::glue('{output_dir}/summary_ento_icf_list.csv')
stg_key <- 'bohemia_stg/manual_adjustment_ento_icf_list/manual_adjustment_ento_icf_list.csv'
prod_key <- 'bohemia_prod/summary_ento_icf_list/summary_ento_icf_list.csv'

# check if there is manual upload in staging bucket
# if none, create a new table with all nulls
if(!stg_key %in% unique(s3_catalog$key)){
  monitoring_icf <- monitoring_tbl %>%
    dplyr::select(cluster_number,
                  id,
                  date_of_consent) %>%
    dplyr::mutate(
      name_of_person_receiving_in_field = NA_character_,
      date_of_person_receiving_in_field = NA_character_,
      name_of_person_receiving_in_office = NA_character_,
      date_of_person_receiving_in_office = NA_character_,
      name_of_archivist_receiving = NA_character_,
      date_of_archivist_receiving = NA_character_
    )
# if there is manual uploads in staging, create a join to table
}else{
  monitoring_ifc_from_upload <- cloudbrewr::aws_s3_get_table(
    bucket = output_bucket,
    key = stg_key) %>%
    dplyr::select(
      cluster_number = `Cluster`,
      id = `Household ID / Livestock Enclosure ID`,
      date_of_consent = `Date of consent on screening form`,
      name_of_person_receiving_in_field = `Name of Person Receiving in the Field`,
      date_of_person_receiving_in_field =`Date of Field Receival`,
      name_of_person_receiving_in_office = `Name of Person Receiving in the Office`,
      date_of_person_receiving_in_office = `Date of Office Receival`,
      name_of_archivist_receiving = `Name of Archivist Receiving` ,
      date_of_archivist_receiving = `Date of Archivist Receival`
    ) %>%
    dplyr::mutate(
      cluster_number = as.character(cluster_number),
      id = as.character(id),
      date_of_consent = lubridate::date(date_of_consent),
      date_of_person_receiving_in_field = lubridate::date(date_of_person_receiving_in_field),
      date_of_person_receiving_in_office = lubridate::date(date_of_person_receiving_in_office),
      date_of_archivist_receiving = lubridate::date(date_of_archivist_receiving)
    )

  monitoring_icf <- monitoring_tbl %>%
    dplyr::select(cluster_number, id, date_of_consent) %>%
    dplyr::inner_join(
      monitoring_ifc_from_upload,
      by = c('cluster_number','id', 'date_of_consent'))
}


monitoring_icf %>%
  fwrite(output_filename)
cloudbrewr::aws_s3_store(
  bucket = output_bucket,
  key = 'bohemia_prod/summary_ento_icf_list/summary_ento_icf_list.csv',
  filename = as.character(output_filename)
)
