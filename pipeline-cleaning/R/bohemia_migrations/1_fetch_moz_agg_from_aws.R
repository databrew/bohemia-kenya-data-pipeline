# This code is used to fetch moz data from Joe's AWS account manually (faster)
# into databrew account. Each RData is fetched and stored as csv (and zip files) into AWS

library(dplyr)
library(data.table)

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


# https://eu-west-3.console.aws.amazon.com/s3/buckets/bohemia2022?region=eu-west-3&bucketType=general&prefix=bohemia.systems/Aggregate/2022-04-28+15%3A44%3A20/&showversions=false
load('R/bohemia_migrations/mopeia.com.data_list_aggregate.RData')
mopeia.com <- robject

nested_df <- mopeia.com %>%
  tibble::enframe()

purrr::map2(nested_df$name, nested_df$value, function(form_id, objects){
  output_dir <- glue::glue('R/bohemia_migrations/output/mopeia.com/clean-form/{form_id}')
  purrr::map(names(objects), function(key){
    output_filename <- glue::glue('{output_dir}/{key}.csv')
    dir.create(dirname(output_filename), recursive =  TRUE, showWarnings = FALSE)
    objects[key] %>% fwrite(output_filename)
  })
})


cloudbrewr::aws_s3_bulk_store(
  bucket = 'bohemia-mozambique-data-repository',
  prefix = '/mopeia.com',
  target_dir = 'R/bohemia_migrations/output/mopeia.com/'
)



# https://eu-west-3.console.aws.amazon.com/s3/buckets/bohemia2022?region=eu-west-3&bucketType=general&prefix=mopeia.com/Aggregate/2023-09-11+09%3A04%3A53/&showversions=false
load('R/bohemia_migrations/bohemia.systems.data_list_aggregate.RData')
bohemia.systems <- robject


nested_df <- bohemia.systems %>%
  tibble::enframe()

purrr::map2(nested_df$name, nested_df$value, function(form_id, objects){
  output_dir <- glue::glue('R/bohemia_migrations/output/bohemia.systems/clean-form/{form_id}')
  purrr::map(names(objects), function(key){
    output_filename <- glue::glue('{output_dir}/{key}.csv')
    dir.create(dirname(output_filename), recursive =  TRUE, showWarnings = FALSE)
    objects[key] %>% fwrite(output_filename)
  })
})


cloudbrewr::aws_s3_bulk_store(
  bucket = 'bohemia-mozambique-data-repository',
  prefix = '/bohemia.systems',
  target_dir = 'R/bohemia_migrations/output/bohemia.systems/'
)
