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

#' @description function to get household data from s3
get_household_forms <- function(){
  s3obj = paws::s3()
  filename <- tempfile(fileext = '.csv')
  s3obj$download_file(
    Bucket= glue::glue(Sys.getenv("BUCKET_PREFIX"), "databrew.org"),
    Key = "kwale/clean-form/reconbhousehold/reconbhousehold.csv",
    Filename = filename)
  return(fread(filename))
}

#' @description function to get household data from s3
get_registration_forms <- function(){
  s3obj = paws::s3()
  filename <- tempfile(fileext = '.csv')
  s3obj$download_file(
    Bucket= glue::glue(Sys.getenv("BUCKET_PREFIX"), "databrew.org"),
    Key = "kwale/clean-form/reconaregistration/reconaregistration.csv",
    Filename = filename)
  return(fread(filename))
}
