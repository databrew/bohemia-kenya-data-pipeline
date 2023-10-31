# This script is used for creating empty objects before running `generate_metadata.R` - this is
# done to create temporary placeholder from testing projects and make that variable available for
# deployed form
# Author: atediarjo@gmail.com
library(logger)
library(purrr)
library(dplyr)
library(cloudbrewr)
library(data.table)
library(lubridate)

TARGET_OBJS <- c(
  'test_of_test',
  'health_economics_testing',
  'kwale_testing'
)
TEMP_DIR <- 'temp_test_objects'
TARGET_DIR <- 'empty_objects'
ENV_PIPELINE_STAGE <- Sys.getenv("PIPELINE_STAGE")

rr <- function(x){
  message('removing ', nrow(x), ' rows')
  return(head(x, 0))
}


tryCatch({
  logger::log_info('Attempt AWS login')
  # login to AWS - this will be bypassed if executed in CI/CD environment
  cloudbrewr::aws_login(
    role_name = 'cloudbrewr-aws-role',
    profile_name =  'cloudbrewr-aws-role',
    pipeline_stage = ENV_PIPELINE_STAGE)

}, error = function(e){
  logger::log_error('AWS Login Failed')
  stop(e$message)
})

# fetch files
dir.create(TEMP_DIR)
purrr::map(TARGET_OBJS, function(obj){
  cloudbrewr::aws_s3_bulk_get(
    bucket ='databrew.org',
    prefix = glue::glue('/{obj}/clean-form'),
    output_dir = TEMP_DIR
  )
})

# create file mapping
df_list <- list.files(TEMP_DIR, recursive = TRUE) %>%
  tibble::as_tibble() %>%
  tidyr::separate(value, into = c('folder', 'form'), sep = '/') %>%
  dplyr::mutate(folder_prx = folder,
                var_name = stringr::str_replace(tools::file_path_sans_ext(form), '-','_')) %>%
  dplyr::mutate(
    data = purrr::map(glue::glue('{TEMP_DIR}/{folder_prx}/{form}'),
                      function(x){fread(x) %>% rr()}))

# assign mapping to environment variables
norm <- df_list %>%
  dplyr::select(var_name, data) %>%
  tibble::deframe() %>%
  list2env(., envir = .GlobalEnv)

unlink(TEMP_DIR, recursive = TRUE)

# save respective objects (main form, repeats) to the parent folder as Rdata to match with Joe's usage
dir.create(glue::glue('{TARGET_DIR}'))
df_list %>%
  dplyr::select(folder, var_name) %>%
  split(.$folder) %>%
  purrr::map(function(c){
    save(list = c$var_name, file = glue::glue('empty_objects/{c$folder[[1]]}.RData'))
  })
