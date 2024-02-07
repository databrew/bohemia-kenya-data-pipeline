library(lubridate)
library(cloudbrewr)
library(tidyverse)
library(janitor)

# log into AWS and load data
# ENV_PIPELINE_STAGE <- Sys.getenv("PIPELINE_STAGE")
ENV_PIPELINE_STAGE <- 'production'
DATA_STAGING_BUCKET_NAME <- 'databrew.org'
DATA_LAKE_BUCKET_NAME <- 'bohemia-lake-db'
PROJECT_SOURCE <- 'kwale'
SE_FOLDER_TARGET <- glue::glue('{PROJECT_SOURCE}/clean-form')

pad_hhid <- function(data){
  if('hhid' %in% names(data)){
    data %>% dplyr::mutate(hhid = stringr::str_pad(hhid, 5, pad = "0"))
  } else{
    data
  }
}

tryCatch({
  logger::log_info('Attempt AWS login')
  # login to AWS - this will be bypassed if executed in CI/CD environment
  cloudbrewr::aws_login(
    role_name = 'cloudbrewr-aws-role',
    profile_name =  'cloudbrewr-aws-role',
    pipeline_stage = ENV_PIPELINE_STAGE )
  
}, error = function(e){
  logger::log_error('AWS Login Failed')
  stop(e$message)
})

# load the data
# to get this data go to S3 bucket in AWS, databrew.org, kwale, v0demography or efficacy
# v0repeat <- cloudbrewr::aws_s3_get_table(
#   bucket = DATA_STAGING_BUCKET_NAME,
#   key = glue::glue('{PROJECT_SOURCE}/sanitized-form/v0demography/v0demography-repeat_individual.csv')) %>%
#   pad_hhid()

# v0demography <- cloudbrewr::aws_s3_get_table(
#   bucket = DATA_STAGING_BUCKET_NAME,
#   key = glue::glue('{PROJECT_SOURCE}/sanitized-form/v0demography/v0demography.csv')) %>%
#   pad_hhid()

ntd <- cloudbrewr::aws_s3_get_table(
  bucket = DATA_STAGING_BUCKET_NAME,
  key = glue::glue('{PROJECT_SOURCE}/sanitized-form/ntdbaseline/ntdbaseline.csv')) %>%
  pad_hhid()

# the associated paper form for these documents: https://docs.google.com/document/d/1oZDP1F58pfRyUxfH24VZSid6z4lOoGjS/edit
# the associated url: https://docs.google.com/spreadsheets/d/1BIn766umIBrjNQ0xpToMD7f8jAE50PZx5pXyHW1WXNk/edit#gid=0
# please note that all the variables and their meanings can be found in the url

healtheconmonthly <- cloudbrewr::aws_s3_get_table(
  bucket = DATA_STAGING_BUCKET_NAME,
  key = glue::glue('{PROJECT_SOURCE}/sanitized-form/healtheconmonthly/healtheconmonthly.csv')) %>%
  pad_hhid()

he_individual <- cloudbrewr::aws_s3_get_table(
  bucket = DATA_STAGING_BUCKET_NAME,
  key = glue::glue('{PROJECT_SOURCE}/sanitized-form/healtheconmonthly/healtheconmonthly-repeat_individual.csv')) %>%
  pad_hhid()

he_disease <- cloudbrewr::aws_s3_get_table(
  bucket = DATA_STAGING_BUCKET_NAME,
  key = glue::glue('{PROJECT_SOURCE}/sanitized-form/healtheconmonthly/healtheconmonthly-repeat_disease.csv')) %>%
  pad_hhid()

# assignment list received from Nika / Joe
# assignment <- read.csv('data/assignments.csv') %>%
#   rename(cluster = cluster_number)

eligible <- he_individual %>% 
  filter( v1_safety_status == "in" )

# prevalence of tunga among those who answered the tunga question :
eligible %>%
  filter( tunga %in% c("yes", "no") ) %>% 
  tabyl( tunga ) %>% 
  adorn_totals() 

eligible %>%
  filter( ! feet_inspection %in% c("", "none" ) ) %>% 
  group_by( feet_inspection ) %>% 
  tally() %>%
  adorn_totals()

eligible %>%
  filter( tunga %in% c("yes", "no") ) %>% 
  tabyl( tung_past_month ) %>% 
  adorn_totals() 
