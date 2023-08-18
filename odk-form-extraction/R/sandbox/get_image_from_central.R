library(dplyr)
library(data.table)
library(purrr)
library(cloudbrewr)
source("R/utils.R")

# login to AWS
# to test Sys.setenv(PIPELINE_STAGE = 'develop')
cloudbrewr::aws_login(pipeline_stage = Sys.getenv("PIPELINE_STAGE"))

# parse ruODK credentials
CREDENTIALS_FILE_PATH <- "~/.bohemia_credentials"
credentials_check(CREDENTIALS_FILE_PATH)
creds <- yaml::yaml.load_file(Sys.getenv('bohemia_credentials'))

# ODK input
INPUT <- list(
  pid = 5,
  fid = 'Image field test'
)

# output folders
OUTPUT <- list(
  output_folder = 'export',
  media_folder = 'export/media',
  zip_folder = 'export/zip',
  bucket = 'databrew.org'
)

# odk setup
# Set up some parameters
ruODK::ru_setup(fid = NULL,
                url = creds$url,
                un = creds$un,
                pw = creds$pw,
                tz = 'UTC')

# create folders
purrr::map(OUTPUT, function(folder){
  dir.create(folder,
             recursive = TRUE,
             showWarnings = FALSE)
})

# get form project name
project_name <- ruODK::project_list() %>%
  dplyr::filter(id == INPUT$pid) %>%
  .$name %>%
  tolower() %>%
  stringr::str_squish(.) %>%
  gsub(" ", "", .)

# get form metadata
form_metadata <- ruODK::form_list(pid = INPUT$pid) %>%
  dplyr::filter(xml_form_id == INPUT$fid)

# get form name w/ fid
form_name <- form_metadata %>%
  .$name %>%
  tolower() %>%
  stringr::str_squish(.) %>%
  gsub(" ", "", .)


# create log message
message(glue::glue("[{lubridate::now()} BOHEMIA_PIPELINE_LOGS]: ", 'Exporting ODK Images'))

# export ODK submission
ruODK::submission_export(
  pid = INPUT$pid,
  fid = INPUT$fid,
  overwrite = TRUE,
  local_dir = OUTPUT$zip_folder)

# get exported file from ODK
exported_file <- list.files(OUTPUT$zip_folder)[1]

# unzip media objects
unzip(glue::glue(OUTPUT$zip_folder, '/',exported_file),
      exdir = OUTPUT$output_folder)

# create submissions
path <- glue::glue("{OUTPUT$output_folder}/{form_metadata$xml_form_id}.csv")
submission_metadata <- fread(path) %>%
  clean_column_names() %>%
  dplyr::mutate(
    file_path = as.character(glue::glue("{OUTPUT$output_folder}/media/{img}")),
    key = as.character(glue::glue('{project_name}/{form_name}/{img}')),
    bucket = 'databrew.org')

# create log message
message(glue::glue("[{lubridate::now()} BOHEMIA_PIPELINE_LOGS]: ", 'Uploading ODK Images to S3'))

# save images to s3 folder
submission_metadata %>%
  dplyr::select(
    bucket,
    key,
    file_path,
    instanceID,
    hh_id = hhid,
    wid) %>%
  purrr::pmap(
    ~cloudbrewr::aws_s3_store(
      bucket = ..1,
      key = ..2,
      filename = ..3,
      Metadata = list(
        instanceID = ..4,
        hh_id = ..5,
        wid = ..6
      )
    )
  )


# indicate finished data extraction
message(glue::glue("[{lubridate::now()} BOHEMIA_PIPELINE_LOGS]: ", 'Cleaning Export Folder'))
unlink('export', recursive = T)

# Finish code pipeline
message(glue::glue("[{lubridate::now()} BOHEMIA_PIPELINE_LOGS]: ", 'ODK Images Extracted to S3'))

