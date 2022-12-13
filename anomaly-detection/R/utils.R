#' @description Function for retrieving files from S3
#'
#' @param s3obj instantiate s3 object
#' @param bucket bucket name
#' @param object_key object key
#' @param filename output filename
#' @return saved local filename
get_s3_data <- function(s3obj = NULL, bucket, object_key, filename){
  s3obj$download_file(
    Bucket= bucket,
    Key = object_key,
    Filename = filename)
  return(filename)
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
save_to_s3_bucket <- function(s3obj, file_path, bucket_name, object_key, ..){
  # instantiate s3 object
  tryCatch({
    # get basepath
    basepath <- basename(file_path)
    # Upload to bucket
    s3obj$put_object(Bucket = bucket_name, Body = file_path, Key = object_key)
    message(glue::glue("log_message: File is uploaded with Bucket URI:{object_key}"))
  }, error = function(e){
    message(glue::glue("error_message: ", e$message))
  })
}

