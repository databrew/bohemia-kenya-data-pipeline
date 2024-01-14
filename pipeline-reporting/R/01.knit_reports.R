library(pagedown)
library(qpdf)
library(logger)
library(glue)
library(lubridate)
library(dplyr)
library(tools)
library(logger)
source('R/utils.R')

unlink('log',recursive=TRUE,force=TRUE)
dir.create('log', recursive=TRUE)

env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")

log_name <- as.character(lubridate::now() %>% as.integer())
log_output_file = glue::glue("log/{log_name}.log")
log_layout(layout_json())
log_appender(appender_file(log_output_file))

log_threshold(ERROR)

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

# clean logs before proceeding
unlink("logs",force=TRUE, recursive = TRUE)

# parse through all report index
a <- purrr::map(config::get('report_index'), function(index){
  entry <- glue::glue('R/{index}')
  purrr::map(list.files(entry, pattern = '*.Rmd', full.names = TRUE), function(rmd){
    tryCatch({
      if(index != 'monitoring'){
        dir.create(
          glue::glue('{entry}/html_report'),
          recursive = TRUE,
          showWarnings = FALSE)
        logger::log_info(glue::glue("Knitting {rmd}"))
        output_file <- glue::glue("html_report/{file_path_sans_ext(basename(rmd))}.html")
        markdown_loc <- glue::glue("{rmd}")
        rmarkdown::render(
          markdown_loc,
          output_file = output_file)
      }
    }, error = function(e){
      err_msg <- glue::glue('Report: {rmd} is throwing an error: {e$message}')
      log_error(err_msg)
    })
  })
})

index <- 'monitoring'
entry <- glue::glue('R/{index}')
version <- format(floor_date(lubridate::today(), 'week'), "%Y%m%d")
purrr::map(list.files(entry, pattern = '*.Rmd'), function(rmd){
  tryCatch({
    logger::log_info(glue::glue("Knitting {rmd}"))
    basename <- glue::glue('{file_path_sans_ext(basename(rmd))}')
    dir.create(
      glue::glue('{entry}/html_report/{basename}'),
      recursive = TRUE,
      showWarnings = FALSE)
    output_file <- glue::glue("html_report/{basename}/{toupper(basename)}-{version}.html")
    markdown_loc <- glue::glue("{entry}/{rmd}")
    rmarkdown::render(
      markdown_loc,
      output_file = output_file)
  }, error = function(e){
    err_msg <- glue::glue('Report: {rmd} is throwing an error: {e$message}')
    log_error(err_msg)
  })
})

index <- 'monitoring-issues-ui'
entry <- glue::glue('R/{index}')
endpoint_list <- config::get('monitoring') %>% purrr::map(function(x){x$endpoint}) %>% unlist()
purrr::map(endpoint_list, function(rmd){
  tryCatch({
    dir.create(
      glue::glue('{entry}/html_report/{rmd}'),
      recursive = TRUE,
      showWarnings = FALSE)
    logger::log_info(glue::glue("Knitting {rmd}"))
    output_file <- glue::glue("html_report/{rmd}/index.html")
    markdown_loc <- glue::glue("{entry}/{rmd}_site_ui.rmd")
    rmarkdown::render(
      markdown_loc,
      output_file = output_file)
  }, error = function(e){
    logger::log_error(e$message)
    stop("")
  })
})


index <- 'consolidate'
entry <- glue::glue('R/{index}')
purrr::map(list.files(entry, pattern = '*.Rmd'), function(rmd){
  tryCatch({
    dir.create(
      glue::glue('{entry}/html_report'),
      recursive = TRUE,
      showWarnings = FALSE)
    logger::log_info(glue::glue("Knitting {rmd}"))
    output_file <- glue::glue("html_report/{file_path_sans_ext(basename(rmd))}.html")
    markdown_loc <- glue::glue("{entry}/{rmd}")
    rmarkdown::render(
      markdown_loc,
      output_file = output_file)
  }, error = function(e){
    logger::log_error(e$message)
    stop("")
  })
})



if(length(list.files('log')) > 0){
  cloudbrewr::aws_s3_bulk_store(
    bucket = 'bohemia-lake-db',
    prefix = '/bohemia_prod/log/knit_report_error/',
    target_dir = 'log/'
  )
}


