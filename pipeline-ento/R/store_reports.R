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

# variables / creds
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
distribution_id <- Sys.getenv("CF_DISTRIBUTION_ID")
report_bucket_name <- 'kenya-reporting-bucket'
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


# create log message
logger::log_info('Uploading reports')
cloudbrewr::aws_s3_bulk_store(
  bucket = report_bucket_name,
  target_dir = 'report/html_report',
  prefix = '/ento'
)

# invalidate cloudfront cache
tryCatch({
  logger::log_info('Invalidating html report in cloudfront cache')
  cf$create_invalidation(
    DistributionId = distribution_id,
    InvalidationBatch = list(
      Paths = list(
        Quantity = 1,
        Items = list(
          "/*"
        )
      ),
      CallerReference = format(lubridate::now(), "%Y%m%d%H%M%s")
    )
  )
  logger::log_success('Success invalidating report in cache')
}, error = function(e){
  logger::log_error(e$message)
  stop()
})


