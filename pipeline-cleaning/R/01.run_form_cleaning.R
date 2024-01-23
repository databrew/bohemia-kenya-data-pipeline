library(dplyr)
library(lubridate)
library(magrittr)
library(paws)
library(config)
library(cloudbrewr)
library(logger)
library(config)
library(tictoc)
library(data.table)
library(sf)
library(sp)
source('R/processing_utils.R')

# start timer
tic()

# create log message
logger::log_info('Starting Form Cleaning')

# variables / creds
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
Sys.setenv(R_CONFIG_ACTIVE=env_pipeline_stage)


# EXCLUDED FILEPATHS FOR PASS THROUGH
EXCLUDED_FILEPATHS <-
  c('projects/kwale/raw-form/reconaregistration/reconaregistration.csv',
    'projects/kwale/raw-form/reconbhousehold/reconbhousehold.csv')
EXCLUDED_FORM_ID <- c('reconaregistration','reconbhousehold')

# AWS related variables
S3_RESOLUTION_OBJECT_KEY <- 'anomalies/gsheets-fix/odk_form_anomalies - resolution.csv'
BUCKET_NAME <- 'databrew.org'
ROLE_NAME <- 'cloudbrewr-aws-role'

purrr::map(config::get('odk_projects'), function(project_name){
  unlink('./projects', force = TRUE, recursive = TRUE)
  logger::log_info(glue::glue('Starting Extraction on {project_name}'))
  prefix <- glue::glue('/{project_name}')

  # create connection to AWS
  tryCatch({
    # login to AWS - this will be bypassed if executed in CI/CD environment
    cloudbrewr::aws_login(
      role_name = ROLE_NAME,
      profile_name =  ROLE_NAME,
      pipeline_stage = env_pipeline_stage)

  }, error = function(e){
    logger::log_error('AWS Login Failed')
    stop(e$message)
  })


  # bulk retrieve all csv files
  tryCatch({
    logger::log_info('bulk retrieval from s3')
    dir.create('projects')
    file_map <- cloudbrewr::aws_s3_bulk_get(
      bucket = BUCKET_NAME,
      output_dir = 'projects',
      prefix = prefix
    )
    logger::log_success('successful bulk retrieval')
  }, error = function(e){
    logger::log_error(e$message)
    stop()
  })


  # Get Files mapping from AWS S3 sync
  files_orig <- tibble::tibble(file_path =
                                 list.files('projects',
                                            full.names = TRUE,
                                            recursive = TRUE,
                                            pattern = ".csv"))  %>%
    dplyr::mutate(clean_file_path = stringr::str_replace(file_path,
                                                         'raw-form',
                                                         'clean-form'),
                  form_id = stringr::str_split(file_path, '/')) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(form_id = form_id[3]) %>%
    dplyr::ungroup() %>%
    dplyr::filter(!file_path %in% EXCLUDED_FILEPATHS,
                  !form_id %in% EXCLUDED_FORM_ID,
                  !stringr::str_detect(file_path, 'clean-form'),
                  !stringr::str_detect(file_path, 'sanitized-form'))

  # Read Resolution file from S3
  resolution_file <- cloudbrewr::aws_s3_get_table(
    bucket = BUCKET_NAME,
    key = S3_RESOLUTION_OBJECT_KEY) %>%
    dplyr::filter(!Form %in% EXCLUDED_FORM_ID) %>%
    dplyr::filter(!(`Column` == "" & `Operation` =='SET')) %>%
    expand_resolution_file_with_connected_cols()

  # Read local file mapping and create tibble-list inside the dataframe for
  # easier indexing
  tbl_nest <- files_orig %>%
    dplyr::mutate(raw = purrr::map(file_path, function(cf){
      fread(cf) %>%
        clean_column_names() %>%
        tibble::as_tibble(.name_repair = 'unique')
    })) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(resolution = list(resolution_file %>% dplyr::filter(Form == form_id)),
                  repeat_name = stringr::str_split(tools::file_path_sans_ext(basename(file_path)), pattern = '-')[[1]][2]) %>%
    dplyr::select(file_path,
                  clean_file_path,
                  form_id,
                  repeat_name,
                  raw,
                  resolution)

  # instantiate geo objects to /tmp files
  tbl_final_mapping <- purrr::pmap_dfr(tbl_nest,
                                       function(file_path = ..1,
                                                clean_file_path = ..2,
                                                form_id = ..3,
                                                repeat_name = ..4,
                                                raw = ..5,
                                                resolution = ..6){

                                         # google sheets resolution
                                         tryCatch({
                                           if(project_name == 'kwale' &
                                              nrow(raw) > 0){

                                             raw <- raw %>%
                                               standardize_col_dobs('dob') %>%
                                               standardize_col_dobs('dob_string')
                                             clean <- google_sheets_fix(
                                                data = raw,
                                                form_id = form_id,
                                                repeat_name = repeat_name,
                                                resolution = resolution) %>%
                                               add_cluster_geo_num(
                                                 data = .,
                                                 form_id = form_id,
                                                 repeat_name = repeat_name) %>%
                                               standardize_col_value_case(data = ., col_names = 'village') %>%
                                               standardize_col_value_case(data = ., col_names = 'village_select') %>%
                                               standardize_col_value_case(data = ., col_names = 'village_specify') %>%
                                               standardize_col_value_case(data = ., col_names = 'firstname') %>%
                                               standardize_col_value_case(data = ., col_names = 'lastname') %>%
                                               standardize_village() %>%
                                               get_corrected_age()
                                           }else{
                                             clean <- raw
                                           }

                                           data <- tibble(
                                             file_path = file_path,
                                             clean_file_path = clean_file_path,
                                             form_id = form_id,
                                             repeat_name = repeat_name,
                                             raw = list(raw),
                                             resolution = list(resolution),
                                             clean = list(clean))
                                           dir.create(glue::glue('projects/clean-form/{form_id}'),
                                                      recursive = TRUE,
                                                      showWarnings = FALSE)
                                           clean %>% fwrite(clean_file_path)
                                           return(data)
                                         }, error = function(e){
                                           logger::log_error(glue::glue('{form_id}={repeat_name} with error {e$message}'))
                                           stop()
                                         })
                                       })

  # create zip file
  if(project_name == 'kwale'){
    tryCatch({
      folders <- tbl_final_mapping %>%
        dplyr::mutate(target_folder = dirname(clean_file_path)) %>%
        distinct(target_folder) %>% .$target_folder

      purrr::map(folders, function(x){
        zip_name = glue::glue('{x}/{basename(x)}.zip')
        zip(zipfile = zip_name, files = dir(x, full.names = TRUE))
      })
    })
  }


  # save object to s3
  tryCatch({
    # do bulk store for speed
    logger::log_info('Bulk store to AWS S3')
    cloudbrewr::aws_s3_bulk_store(bucket = config::get('bucket'),
                                  target = './projects',
                                  prefix = prefix)

    # remove directory once done
    logger::log_success('Bulk upload completed')

  }, error = function(e){
    logger::log_error('Error storing to AWS S3')
    stop(e$message)
  })


})

# stop timer
toc()







