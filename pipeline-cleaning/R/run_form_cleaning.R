library(dplyr)
library(lubridate)
library(magrittr)
library(paws)
library(config)
library(cloudbrewr)
library(logger)
library(config)
library(tictoc)

# start timer
tic()

# create log message
logger::log_info('Starting Anomaly Detection')

# variables / creds
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
Sys.setenv(R_CONFIG_ACTIVE=env_pipeline_stage)

# create connection to AWS
tryCatch({
  # login to AWS - this will be bypassed if executed in CI/CD environment
  cloudbrewr::aws_login(
    role_name = 'cloudbrewr-aws-role',
    profile_name =  'cloudbrewr-aws-role',
    pipeline_stage = env_pipeline_stage)

}, error = function(e){
  logger::log_error('AWS Login Failed')
  stop(e$message)
})


# bulk retrieve all csv files
dir.create('projects')
file_map <- cloudbrewr::aws_s3_bulk_get(
  bucket = 'databrew.org',
  output_dir = 'projects'
)


# get files mapping
files_orig <- tibble::tibble(file_path =
                          list.files('projects',
                                     full.names = TRUE,
                                     recursive = TRUE,
                                     pattern = ".csv")) %>%
  dplyr::mutate(clean_file_path = stringr::str_replace(file_path,
                                                       'raw-form',
                                                       'clean-form'))


# choose data that goes through cleaning,
# if cleaning is required, then upload separately and
# remove from pass-through
excluded_filepaths <-
  c('projects/kwale/raw-form/reconaregistration/reconaregistration.csv',
    'projects/kwale/raw-form/reconbhousehold/reconbhousehold.csv')





# TODO: To fill here








# bulk upload everything that is unedited to clean-form
files_staging <- files_orig %>%
  dplyr::filter(!file_path %in% excluded_filepaths)

# create skeleton folder
purrr::map(files_staging$clean_file_path %>% dirname(), function(d){
  dir.create(d, recursive = TRUE)
})

# move files passthrough
file.rename(files_staging$file_path, files_staging$clean_file_path)

# cleanup any previous files
purrr::map(files_orig$file_path %>% dirname(), function(d){
  unlink(d, recursive = TRUE)
})

# save object to s3
tryCatch({
  # do bulk store for speed
  logger::log_info('Bulk store to AWS S3')
  cloudbrewr::aws_s3_bulk_store(bucket = config::get('bucket'),
                                target = './projects')

  # remove directory once done
  unlink('./projects', force = TRUE)
  logger::log_success('Bulk upload completed')

}, error = function(e){
  logger::log_error('Error storing to AWS S3')
  stop(e$message)
})

# stop timer
toc()
unlink('projects', recursive = TRUE)






