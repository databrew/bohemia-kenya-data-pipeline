#' @description clean column names
#' @param data dataset to clean
clean_column_names <- function(data){
  names(data) <- unlist(lapply(strsplit(names(data), '-'), function(a){a[length(a)]}))
  return(data)
}

#' @description  clean pii column
#' @param data data to sanitize
clean_pii_columns <- function(data){
  pii_columns <- c('firstname', 'lastname')
  data %>% dplyr::select(-any_of(pii_columns))
}


#' @description data type conversions
#' @param series data series to convert
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


#' @description function to do batch set based on ID
#' @param data data
#' @param form_id form_id to parse
#' @param repeat_name name of repeat
#' @param resolution resolution data
batch_set <- function(data, form_id, repeat_name, resolution){
  tryCatch({
    # get resolution file, if there is duplicate SETs take most recent one
    resolution <- resolution %>%
      dplyr::filter(Operation == 'SET') %>%
      group_by(instanceID,
               Column,
               RepeatName,
               RepeatKey) %>%
      slice_tail() %>%
      ungroup()

    # unique columns for resolution
    cols <- unique(resolution$Column)
    target_cols <- names(data)[names(data) %in% cols]

    # pivot resolution file
    pvt <- resolution %>%
      distinct(.) %>%
      tidyr::pivot_wider(
        names_from = Column,
        values_from = `Set To`,
        id_cols = c('instanceID', 'RepeatName', 'RepeatKey')) %>%
      dplyr::select(instanceID,
                    repeat_name = RepeatName,
                    repeat_key = RepeatKey,
                    any_of(target_cols))

    # joined with pivot table
    if(!is.na(repeat_name)){
      logger::log_info(glue::glue('Batch set on {form_id}-{repeat_name}'))
      staging <- data %>%
        dplyr::rowwise() %>%
        dplyr::mutate(
          repeat_parser = stringr::str_split(stringr::str_replace_all(basename(KEY), "\\[|\\]", ";"), ";"),
          repeat_key = as.integer(unlist(repeat_parser)[2]),
          repeat_name = unlist(repeat_parser)[1]
        ) %>%
        dplyr::select(-repeat_parser) %>%
        dplyr::select(PARENT_KEY,
                      repeat_key,
                      repeat_name,
                      everything())

      jtbl <- staging %>%
        dplyr::left_join(
          pvt %>%
            dplyr::filter(repeat_name != ""),
          by = c('PARENT_KEY' = 'instanceID',
                 'repeat_name',
                 'repeat_key'))

    # join with instance ID on main table
    }else{
      logger::log_info(glue::glue('Batch set on {form_id}'))
      jtbl <- data %>%
        dplyr::left_join(pvt %>%
                           dplyr::filter(repeat_name == ""),
                         by = 'instanceID')
    }

    # loop through all changes for target columns
    purrr::map(target_cols, function(col){
      left <- as.character(glue::glue('{col}.x'))
      right <- as.character(glue::glue('{col}.y'))

      # convert datatype based on known datatypes
      # if already available, use left-side dtypes
      # if not available, use inputted values from data custodian
      if(all(is.na(jtbl[[left]]))){
        datatype <- convert_datatype(series = jtbl[[right]])
      }else{
        datatype <- convert_datatype(series = jtbl[[left]])
      }

      jtbl <<- jtbl %>%
        dplyr::mutate_at(c(left, right), datatype) %>%
        dplyr::mutate(!!sym(col) := coalesce(!!sym(right),
                                             !!sym(left))) %>%
        dplyr::select(-all_of(c(left,right)))
      return(NULL)
    })

    logger::log_success(glue::glue('Batch set successful on {form_id}-{repeat_name}'))
    return(jtbl)

  }, error = function(e){
    logger::log_error(e$message)
    stop()
  })
}

# Function to do batch delete
batch_delete <- function(data,
                         form_id,
                         resolution,
                         repeat_name) {
  tryCatch({
    # joined with pivot table
    if(!is.na(repeat_name)){
      logger::log_info(glue::glue('Batch delete on {form_id}-{repeat_name}'))
      # files to delete
      to_delete <- resolution %>%
        dplyr::filter(Operation == 'DELETE') %>%
        dplyr::select(form_id = Form,
                      repeat_name = RepeatName,
                      repeat_key = RepeatKey,
                      PARENT_KEY = instanceID)

      # stage table
      staging <- data %>%
        dplyr::rowwise() %>%
        dplyr::mutate(
          repeat_parser = stringr::str_split(stringr::str_replace_all(basename(KEY), "\\[|\\]", ";"), ";"),
          repeat_key = as.integer(unlist(repeat_parser)[2]),
          repeat_name = unlist(repeat_parser)[1]
        ) %>%
        dplyr::mutate(form_id = form_id) %>%
        dplyr::ungroup() %>%
        dplyr::anti_join(to_delete,
                         by = c('form_id',
                                'repeat_name',
                                'repeat_key',
                                'PARENT_KEY')) %>%
        dplyr::select(
          PARENT_KEY,
          repeat_key,
          repeat_name,
          everything())
    }else{
      logger::log_info(glue::glue('Batch delete on {form_id}'))
      # files to delete
      to_delete <- resolution %>%
        dplyr::filter(Operation == 'DELETE',
                      (RepeatName == "" | is.na(RepeatName))) %>%
        dplyr::select(instanceID)

      staging <- data %>%
        dplyr::anti_join(to_delete, by = 'instanceID')
    }

    data <- staging

    logger::log_success(glue::glue('Batch delete successful on {form_id}-{repeat_name}'))

    return(data)
  }, error = function(e){
    logger::log_error(e$message)
  })
}


# Entry point for google sheets fixes
# In data cleaning process DELETE will supersedes SET
# Cleaning will prioritize deletion then do set on columns
google_sheets_fix <- function(data,
                              form_id,
                              repeat_name,
                              resolution){
  if(nrow(resolution) > 0){
    data <- batch_delete(data = data,
                         form_id = form_id,
                         repeat_name = repeat_name,
                         resolution = resolution) %>%
      batch_set(data = .,
                form_id = form_id,
                repeat_name = repeat_name,
                resolution = resolution) %>%
      dplyr::ungroup() %>%
      dplyr::select(-any_of(c(
        'repeat_parser',
        'repeat_name',
        'repeat_key',
        'form_id')))
    return(data)
  }else{
    return(data)
  }
}
