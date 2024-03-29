library(logger)
library(purrr)
library(dplyr)
library(cloudbrewr)
library(data.table)
library(here)

# create cloudfront object
cf <- paws::cloudfront()

# create log message
logger::log_info('Running Bulk Storing Reports')

# variables / creds and bucket target
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
distribution_id <- Sys.getenv("CF_DISTRIBUTION_ID")
report_bucket_name <- 'bohemia-reporting'
data_bucket_name <- 'databrew.org'
target_dir <- glue::glue('{here::here()}/report/html_report')

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


purrr::map(config::get('report_index'), function(index){
  entry <- as.character(glue::glue('R/{index}/html_report'))
  prefix <- as.character(glue::glue('/{index}'))
  cloudbrewr::aws_s3_bulk_store(
    bucket = report_bucket_name,
    target_dir = entry,
    prefix = prefix
  )
})

index <- 'consolidate'
entry <- as.character(glue::glue('R/{index}/html_report'))
cloudbrewr::aws_s3_bulk_store(
  bucket = report_bucket_name,
  target_dir = entry
)
