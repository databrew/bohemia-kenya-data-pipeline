
instantiate_prod_dev_bucket_namespace <- function(){
  sts <- paws::sts()
  if(sts$get_caller_identity()$Account == "381386504386"){
    Sys.setenv(BUCKET_PREFIX = "databrew-testing")
  }else{
    Sys.setenv(BUCKET_PREFIX = "")
  }
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
