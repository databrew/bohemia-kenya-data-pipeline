library(pagedown)
library(qpdf)
library(logger)
library(glue)
library(lubridate)
library(dplyr)

output_dir <-'report/html_report'
unlink(output_dir, recursive = TRUE)
dir.create(output_dir)

env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
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

tryCatch({
  logger::log_info('Knitting metadata reports')
  output_file <- "html_report/index.html"
  markdown_loc <- 'report/generate_report_mapping.Rmd'
  rmarkdown::render(
    markdown_loc,
    output_file = output_file)
}, error = function(e){
  logger::log_error(e$message)
  stop()
})
