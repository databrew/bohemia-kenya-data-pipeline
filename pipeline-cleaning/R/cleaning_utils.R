#' @description clean column names
#' @param data dataset to clean
clean_column_names <- function(data){
  names(data) <- unlist(lapply(strsplit(names(data), '-'), function(a){a[length(a)]}))
  return(data)
}

#' @description  clean pii column
clean_pii_columns <- function(data){
  pii_columns <- c('firstname', 'lastname')
  data %>% dplyr::select(-any_of(pii_columns))
}


# data type conversions
convert_datatype <- function(series){
  if(class(series) == 'integer') {
    change_data_type_funs = as.integer
  }else if (class(series) == 'character'){
    change_data_type_funs = as.character
  }else if (class(series) == 'logical'){
    change_data_type_funs = as.logical
  }else if (class(series) == 'numeric'){
    change_data_type_funs = as.numeric
  }else if (class(series) == 'complex'){
    change_data_type_funs = as.complex
  }else {
    change_data_type_funs = as.raw
  }
  return(change_data_type_funs)
}


# function to do batch set based on ID
batch_set <- function(data, resolution){
  tryCatch({
    logger::log_info('Batch set based on ID')

    resolution <- resolution %>%
      dplyr::filter(Operation == 'SET') %>%
      distinct(instanceID, Column, .keep_all = TRUE)

    # unique columns
    cols <- unique(resolution$Column)

    # pivot resolution file
    pvt <- resolution %>%
      distinct(.) %>%
      tidyr::pivot_wider(names_from = Column,
                         values_from = `Set To`,
                         id_cols = instanceID)

    # joined with pivot table
    jtbl <- data %>%
      dplyr::left_join(pvt, by = 'instanceID')


    # loop through all changes
    purrr::map(cols, function(col){
      left <- as.character(glue::glue('{col}.x'))
      right <- as.character(glue::glue('{col}.y'))
      datatype <- convert_datatype(series = jtbl[[left]])
      jtbl <<- jtbl %>%
        dplyr::mutate_at(c(left, right), datatype) %>%
        dplyr::mutate(!!sym(col) := coalesce(!!sym(right),
                                             !!sym(left))) %>%
        dplyr::select(-all_of(c(left,right)))
    })
    logger::log_success('Batch set successful')
    return(jtbl)

  }, error = function(e){
    logger::log_error(e$message)
    stop()
  })
}

# Function to do batch delete
batch_delete <- function(data, resolution) {
  tryCatch({
    logger::log_info('Batch delete based on ID')
    to_delete <- resolution %>%
      dplyr::filter(Operation == 'DELETE') %$%
      instanceID %>%
      unique()
    logger::log_success('Batch delete successful')
    data <- data %>%
      dplyr::filter(!instanceID %in% to_delete)
    return(data)
  }, error = function(e){
    logger::log_error(e$message)
  })

}


# Entry point for google sheets fixes
google_sheets_fix <- function(data, resolution){
  if(nrow(resolution) > 0){
    data <- batch_delete(data = data, resolution = resolution) %>%
      batch_set(data = ., resolution = resolution)
    return(data)
  }else{
    return(data)
  }
}
