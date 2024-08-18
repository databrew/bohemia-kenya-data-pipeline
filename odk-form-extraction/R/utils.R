#' @description: Function to create S3 bucket with versioning enabled
#'
#' @param s3obj instantiation of s3 object
#' @param bucket_name name of the bucket
create_s3_bucket <- function(bucket_name){
  s3obj <- paws::s3()
  tryCatch({
    message(glue::glue("[{lubridate::now()} BOHEMIA_PIPELINE_LOGS]: checking {bucket_name}"))
    bucket_list <- s3obj$list_buckets() %>%
      .$Buckets %>%
      purrr::map_dfr(function(b){b})
    if(!bucket_name %in% bucket_list$Name){
      s3obj$create_bucket(Bucket = bucket_name)
      message(glue::glue("[{lubridate::now()} BOHEMIA_PIPELINE_LOGS]: {bucket_name} created"))
      s3obj$put_bucket_versioning(
        Bucket = bucket_name,
        VersioningConfiguration = list(Status = 'Enabled'))
      message(glue::glue("[{lubridate::now()} BOHEMIA_PIPELINE_LOGS]: {bucket_name} versioning enabled"))
    }else{
      message(glue::glue("[{lubridate::now()} BOHEMIA_PIPELINE_LOGS]: {bucket_name} is available"))
    }
  }, error = function(e){
    message(glue::glue("[{lubridate::now()} BOHEMIA_PIPELINE_LOGS]: ", e$message))
  })
}


odk_submission_export_wrapper <- function(pid, fid) {
  tryCatch({
    msg <- as.character(glue::glue('fetching fid:{fid} from pid:{pid}'))
    logger::log_info(msg)
    ruODK::submission_export(pid = pid,
                             fid = fid,
                             local_dir = '/tmp',
                             overwrite = TRUE,
                             media = FALSE
                            )
  }, error = function(e){
    msg <- as.character(glue::glue('{fid} is throwing an error: {e$message}'))
    logger::log_error(msg)
  })
}


#' @description: Function to save files to s3 bucket
#'
#' @param s3obj instantiation of s3 object
#' @param server name of the hosted server for ODK
#' @param projects name of the projects
create_s3_upload_manifest <- function(bucket_name = 'databrew.org',
                                      server,
                                      project){
  manifest <- tryCatch({
    logger::log_info(glue::glue('Extracting {project} forms from {server}'))

    # get all project_id
    project_id <- ruODK::project_list() %>%
      dplyr::filter(!archived) %>%
      dplyr::filter(name == project) %>%
      .$id

    # generate file mapping as a manifest
    manifest <- ruODK::form_list(pid = project_id) %>%
      dplyr::rowwise() %>%
      dplyr::mutate(
        project_name = project,
        zip_path = odk_submission_export_wrapper(pid = project_id, fid = fid))

    # unload manifest files into zip files
    manifest$zip_path %>%
      purrr::map_dfr(function(z){
        print(z)
        form_id = tools::file_path_sans_ext(basename(z))
        project_name <- stringr::str_replace_all(project, " ", "_") %>% tolower()
        dir <- as.character(glue::glue("{output_dir}/{project_name}/raw-form/{form_id}"))
        unzip(z, exdir = dir)
        dt <- unzip(z, exdir = dir, list = TRUE) %>%
          dplyr::mutate(root_zipfile = z,
                        form_id = form_id)
        file.rename(z, glue::glue('{dir}/{base_zip}', base_zip = basename(z)))
        return(dt)
      })

  }, error = function(e){
    logger::log_error(e$message)
    stop()
  }, finally = function(f){
    logger::log_success('Created S3 manifest')
  })
}




#' Credentials check
#'
#' Check on or create credentials. If a credentials_path argument is supplied, this function will set the bohemia_credentials environment variable to that path; if it is not supplied, this function will do the same, but will first prompt a few fields so as to create the credentials file.
#' @param credentials_path Path to a bohemia_credentials.yaml file
#' @return A credentials file will be created and a bohemia_credentials environment variable will be set
#' @export
#' @import yaml
credentials_check <- function(credentials_path = NULL){

  # If a path was given, confirm that it's the path they want
  if(is.null(credentials_path)){
    out <- menu(choices = c('Yes', 'No, stop.'),
                title = 'You did not supply a path to a credentials file. Would you like to make one now?')

    if(out == 1){
      url <- ''
      while(nchar(url) < 1){
        url <- readline(prompt="ODK Central server (include https://): ")
      }

      project_name <- ''
      while(nchar(project_name) < 1){
        project_name <- readline(prompt="ODK Central project_name: ")
      }

      un <- ''
      while(nchar(un) < 1){
        un <- readline(prompt="ODK Central user email: ")
      }

      pw <- ''
      while(nchar(pw) < 1){
        pw <- readline(prompt="ODK Central user password: ")
      }

      backup_pw <- readline(prompt="ODK Central password for backups: ")


      agg_url <- ''
      while(nchar(agg_url) < 1){
        agg_url <- readline(prompt="ODK Aggregate server (include https://): ")
      }

      agg_un <- ''
      while(nchar(agg_un) < 1){
        agg_un <- readline(prompt="ODK Aggregate user name: ")
      }

      agg_pw <- ''
      while(nchar(agg_pw) < 1){
        agg_pw <- readline(prompt="ODK Aggregate user password: ")
      }

      briefcase_directory <- ''
      while(nchar(briefcase_directory) < 1){
        briefcase_directory <- readline(prompt="ODK Aggregate briefcase directory: ")
      }

      aws_access_key_id <- ''
      while(nchar(aws_access_key_id) < 1){
        aws_access_key_id <- readline(prompt="AWS S3 Access key ID: ")
      }

      aws_secret_access_key <- ''
      while(nchar(aws_secret_access_key) < 1){
        aws_secret_access_key <- readline(prompt="AWS S3 Secret access key: ")
      }

      message('Great! You have set up the following credentials.')
      message('---ODK Central server: ', url)
      message('---ODK Central project name: ', project_name)
      message('---ODK Central user: ', un)
      message('---ODK Central pass: ', pw)
      message('---ODK Central backup pass: ', backup_pw)
      message('---ODK Aggregate server : ', agg_url)
      message('---ODK Aggregate user : ', agg_un)
      message('---ODK Aggregate password : ', agg_pw)
      message('---ODK Aggregate briefcase directory: ', briefcase_directory)
      message('---AWS S3 Access key ID: ', aws_access_key_id)
      message('---AWS S3 Secrety access key: ', aws_secret_access_key)

      is_ok <- FALSE
      while(!is_ok){
        write_to <- readline(prompt = 'In which folder would you like to write your bohemia_credentials.yaml file (type the folder path)? ')
        is_ok <- dir.exists(write_to)
      }
      yaml_lines <-
        c(paste0('url: ', url),
          paste0('project_name: ', project_name),
          paste0('un: ', un),
          paste0('pw: ', pw),
          paste0('backup_pw: ', backup_pw),
          paste0('agg_url: ', agg_url),
          paste0('agg_un: ', agg_un),
          paste0('agg_pw: ', agg_pw),
          paste0('briefcase_directory: ', briefcase_directory),
          paste0('aws_access_key_id: ', aws_access_key_id),
          paste0('aws_secret_access_key: ', aws_secret_access_key),
          paste0('aws_default_region_name: "eu-west-3"'),
          c(''))
      out_file <- file.path(write_to, 'bohemia_credentials.yaml')
      conn <- file(out_file)
      writeLines(text = yaml_lines,
                 con = conn)
      close(conn)
      message('Successfully wrote a file to ', out_file)
      credentials_path <- out_file
    } else {
      stop('Okay, stopping.')
    }
  }

  message('Going to set the bohemia_credentials environment variable to ', credentials_path)
  Sys.setenv('bohemia_credentials'=credentials_path)
  message('(done)')
}



