library(dplyr)
library(data.table)

# variables / creds
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
Sys.setenv(R_CONFIG_ACTIVE=env_pipeline_stage)
BUCKET_NAME <- 'bohemia-mozambique-data-repository'
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

for (fp in list.files('R/bohemia_migrations/output/tmp',
                        recursive = TRUE,
                        full.names = TRUE)){
  tryCatch({
    logger::log_info(glue::glue('Parsing {fp}'))
    expand = stringr::str_split(fp, '/') %>% unlist()
    form_id = expand[5]
    clean_or_raw = expand[6]
    filename = expand[7]

    load(fp)
    key_output <- dirname(fp)

    if(any(class(robject) == 'data.frame')){
      if (clean_or_raw == 'clean'){
        clean_or_raw_path = 'clean-form'
      }else {
        clean_or_raw_path = 'raw-form'
      }

      output_path = glue::glue('R/bohemia_migrations/output/mopeia.org/{clean_or_raw_path}/{form_id}/{form_id}.csv')
      unlink(output_path, recursive = TRUE, force = TRUE)
      dir.create(dirname(output_path), recursive = TRUE)

      robject %>% fwrite(output_path)

    }else{
      for (key in names(robject)){
        if (clean_or_raw == 'clean'){
          clean_or_raw_path = 'clean-form'
        }else {
          clean_or_raw_path = 'raw-form'
        }

        output_path = glue::glue('R/bohemia_migrations/output/mopeia.org/{clean_or_raw_path}/{form_id}/{key}.csv')
        unlink(output_path, recursive = TRUE, force = TRUE)
        dir.create(dirname(output_path), recursive = TRUE)

        robject[[key]] %>% fwrite(output_path)
      }
    }

  }, error = function(e){
    logger::log_error(glue::glue('Error in {e} in this filepath {fp}'))
  })
}


cloudbrewr::aws_s3_bulk_store(
  bucket = 'bohemia-mozambique-data-repository',
  prefix = '/mopeia.org',
  target_dir = 'R/bohemia_migrations/output/mopeia.org/'
)
