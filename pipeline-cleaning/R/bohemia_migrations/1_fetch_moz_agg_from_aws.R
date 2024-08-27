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



# manual and local download to avoid AWS cross-account transfer
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
