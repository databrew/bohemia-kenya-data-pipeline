library(pagedown)
library(qpdf)
library(logger)
library(glue)
library(lubridate)
library(dplyr)
library(tools)

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

# parse through all report index
purrr::map(config::get('report_index'), function(index){
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
      stop()
    })
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
    stop()
  })
})


