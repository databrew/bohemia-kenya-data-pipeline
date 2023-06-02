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

# start timer
tic()

# create log message
logger::log_info('Starting Form Cleaning')

# variables / creds
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
Sys.setenv(R_CONFIG_ACTIVE=env_pipeline_stage)

BUCKET_NAME <- 'databrew.org'
ROLE_NAME <- 'cloudbrewr-aws-role'

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


dir.create('projects/clean-form')
dir.create('projects/sanitized-form')

# bulk retrieve all csv files
tryCatch({
  logger::log_info('bulk retrieval from s3')
  dir.create('projects')
  file_map <- cloudbrewr::aws_s3_bulk_get(
    bucket = BUCKET_NAME,
    output_dir = 'projects/clean-form',
    prefix = '/kwale/clean-form'
  )
  logger::log_success('successful bulk retrieval')
}, error = function(e){
  logger::log_error(e$message)
  stop()
})



# Get Files mapping from AWS S3 sync
files_orig <- tibble::tibble(file_path =
                               list.files('projects/clean-form',
                                          full.names = TRUE,
                                          recursive = TRUE,
                                          pattern = ".csv"))  %>%
  dplyr::mutate(form_id = stringr::str_split(file_path, '/')) %>%
  dplyr::rowwise() %>%
  dplyr::mutate(form_id = form_id[3]) %>%
  dplyr::ungroup()


# Read local file mapping and create tibble-list inside the dataframe for
# easier indexing
tbl_nest <- files_orig %>%
  dplyr::mutate(clean = purrr::map(file_path, function(cf){
    fread(cf) %>%
      tibble::as_tibble(.name_repair = 'unique')})) %>%
  dplyr::rowwise() %>%
  dplyr::mutate(sanitized_file_path = file.path('projects/sanitized-form',form_id, basename(file_path))) %>%
  dplyr::select(file_path, sanitized_file_path, form_id, clean)

# create data sanitation folder
dir.create('projects/sanitized-form')
tbl_final_mapping <- purrr::pmap_dfr(tbl_nest,
                                     function(file_path = ..1,
                                              sanitized_file_path = ..2,
                                              form_id = ..3,
                                              clean = ..4){
                                       sanitized <- clean
                                       data <- tibble(
                                         file_path = file_path,
                                         sanitized_file_path = sanitized_file_path,
                                         form_id = form_id,
                                         raw = list(clean),
                                         sanitized = list(sanitized))
                                       dir.create(glue::glue('projects/sanitized-form/{form_id}'),
                                                  recursive = TRUE,
                                                  showWarnings = FALSE)
                                       sanitized %>% fwrite(sanitized_file_path)
                                       return(data)
                                     })


# save object to s3
tryCatch({
  # do bulk store for speed
  logger::log_info('Bulk store to AWS S3')
  cloudbrewr::aws_s3_bulk_store(bucket = config::get('bucket'),
                                target = './projects/sanitized-form',
                                prefix = '/kwale/sanitized-form')

  logger::log_success('Bulk upload completed')

}, error = function(e){
  logger::log_error('Error storing to AWS S3')
  stop(e$message)
})

# remove directory once done
unlink('./projects', force = TRUE)

# stop timer
toc()

