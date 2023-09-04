# this is utility script for metadata generation
rr <- function(x){
  message('removing ', nrow(x), ' rows')
  return(head(x, 0))
}

# Make household ID 5 characters
add_zero <- function (x, n) {
  if(length(x) > 0){
    x <- as.character(x)
    adders <- n - nchar(x)
    adders <- ifelse(adders < 0, 0, adders)
    for (i in 1:length(x)) {
      if (!is.na(x[i])) {
        x[i] <- paste0(paste0(rep("0", adders[i]), collapse = ""),
                       x[i], collapse = "")
      }
    }
  }
  return(as.character(x))
}

cleanup <- function(data) {
  start_from <- as.Date('1900-01-01')

  if('hhid' %in% names(data)){
    data <- data %>%
      mutate(hhid = add_zero(hhid, n = 5))
  }

  if('todays_date' %in% names(data)){
    data <- data %>%
      filter(todays_date >= start_from)
  }

  return(data)
}

try_read_to_s3 <- function(bucket, key) {
  tryCatch({
    logger::log_info(glue::glue('Trying to fetch {key} from S3'))
    data <- cloudbrewr::aws_s3_get_table(bucket = bucket, key = key)
    logger::log_info('Successful')
    return(data)
  }, error = function(e){
    logger::log_error(e$message)
    logger::log_info('Returning NULLs')
    return(NULL)
  })
}

test_df <-  function(df){

  if(dim(df)[1]>0){
    test <- TRUE
  }
  else{
    test<- FALSE
  }

  return(test)
}


clean_column_names <- function(data){
  names(data) <- unlist(lapply(strsplit(names(data), '-'), function(a){a[length(a)]}))
  return(data)
}
