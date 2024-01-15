library(logger)
library(purrr)
library(dplyr)
library(cloudbrewr)
library(data.table)
library(here)
library(config)


ENV_PIPELINE_STAGE <- Sys.getenv("PIPELINE_STAGE")

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


purrr::map(config::get('monitoring'), function(l){
  cloudbrewr::aws_s3_bulk_store(
    bucket = l$bucket,
    prefix = '/report',
    target_dir = l$fp
  )
  cloudbrewr::aws_s3_bulk_store(
    bucket = l$bucket,
    target_dir = l$index
  )

  cloudbrewr::aws_s3_bulk_store(
    bucket = config::get('cra')$bucket,
    prefix = '/report',
    target_dir = l$fp
  )
})


cloudbrewr::aws_s3_bulk_store(
  bucket = config::get('cra')$bucket,
  target_dir = config::get('cra')$fp
)
