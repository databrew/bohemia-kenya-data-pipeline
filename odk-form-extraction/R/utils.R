#' @description clean column names
#' @param data dataset to clean
clean_column_names <- function(data){
  names(data) <- unlist(lapply(strsplit(names(data), '-'), function(a){a[length(a)]}))
  return(data)
}


#' @description: Function to create S3 bucket with versioning enabled
#'
#' @param s3obj instantiation of s3 object
#' @param bucket_name name of the bucket
create_s3_bucket <- function(s3obj = NULL, bucket_name){
  tryCatch({
    message(glue::glue("log_message: checking {bucket_name}"))
    bucket_list <- s3obj$list_buckets() %>%
      .$Buckets %>%
      purrr::map_dfr(function(b){b})
    if(!bucket_name %in% bucket_list$Name){
      s3obj$create_bucket(Bucket = bucket_name)
      message(glue::glue("log_message: {bucket_name} created"))
      s3obj$put_bucket_versioning(
        Bucket = bucket_name,
        VersioningConfiguration = list(Status = 'Enabled'))
      message(glue::glue("log_message: {bucket_name} versioning enabled"))
    }else{
      message(glue::glue("log_message: {bucket_name} is available"))
    }
  }, error = function(e){
    message(glue::glue("error_message: ", e$message))
  })
}

#' @description: Function to save files to s3 bucket
#'
#' @param s3obj instantiation of s3 object
#' @param project_name name of the project
#' @param fid survey form id
#' @param file_path filepath
#' @param bucket_name name of the bucket
#' @param object_key S3 object key (S3 URI)
save_to_s3_bucket <- function(s3obj, project_name, fid, file_path, bucket_name, object_key){
  # instantiate s3 object
  tryCatch({
    # get basepath
    basepath <- basename(file_path)
    # Upload to bucket
    s3obj$put_object(Bucket = bucket_name, Body = file_path, Key = object_key)
    message(glue::glue("log_message: {project_name} : {fid} is uploaded with Bucket URI:{object_key}"))
  }, error = function(e){
    message(glue::glue("error_message: ", e$message))
  })
}

#' @description: Function to save files to s3 bucket
#'
#' @param s3obj instantiation of s3 object
#' @param server name of the hosted server for ODK
#' @param projects name of the projects
create_s3_upload_manifest <- function(s3obj = NULL, server, projects){
  purrr::map_dfr(projects, function(project){
    # create dir
    # create save location
    tryCatch({
      t <- glue::glue('~/.odk_cache/{project}')
      dir.create(t, showWarnings = FALSE, recursive = TRUE)

      # name bucket
      bucket_name <- glue::glue(Sys.getenv('BUCKET_PREFIX'),
                                gsub('https://', '', server, fixed = TRUE))

      # change project_name
      project_name <- tolower(gsub(' ', '', project, fixed = TRUE))

      # Establish a prefix (folder on AWS for saving everything)
      prefix <- glue::glue("{project_name}/raw-form/")

      # get all project_id
      project_id <- ruODK::project_list() %>%
        dplyr::filter(!archived) %>%
        dplyr::filter(name == project) %>%
        .$id

      # generate file mapping as a manifest
      manifest <- ruODK::form_list(pid = project_id) %>%
        dplyr::rowwise() %>%
        dplyr::mutate(
          server_name = server,
          project_name = project_name,
          zip_path = ruODK::submission_export(pid = project_id,
                                              fid = fid,
                                              local_dir = t,
                                              overwrite = TRUE),
          file_path = unzip(zip_path, exdir = t),
          bucket_name = bucket_name,
          object_key = glue::glue("{prefix}{fid}/{fid}.csv")) %>%
        dplyr::select(server_name,
                      project_name,
                      fid,
                      file_path,
                      bucket_name,
                      object_key)
      # # clean extraneous column names
      manifest$file_path %>%
        purrr::map(function(file_path){
          data <- data.table::fread(file_path) %>%
            clean_column_names() %>%
            data.table::fwrite(file_path)
        })
    }, error = function(e){
      message(glue::glue("error_message: Bug is coming from ", project, " project"))
      message(glue::glue("error_message: ", e$message))
    })
    return(manifest)
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



