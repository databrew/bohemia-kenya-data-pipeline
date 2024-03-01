# function pad hhid with zeroes
pad_hhid <- function(data){
  if('hhid' %in% names(data)){
    data %>%
      dplyr::mutate(hhid = stringr::str_pad(hhid, 5, pad = "0"))
  }else{
    data
  }
}

# function to create data into parquet
create_parquet <- function(data){
  if('hhid' %in% names(data)){
    data  %>%
      arrow_table() %>%
      mutate(hhid = cast(hhid, string()))
  }else{
    data
  }
}

#' @description get user age based on available dob after fixes
get_corrected_age <- function(data) {
  tryCatch({
    if(('age' %in% names(data)) &
       ('dob' %in% names(data)) &
       (nrow(data) > 0)) {
      output <- data %>%
        dplyr::mutate(corrected_age = as.numeric((lubridate::today() - lubridate::date(dob))/365.25))
    }else{
      output <- data
    }
    return(output)
  }, error = function(e){
    logger::log_error(e$message)
    return(data)
  })
}

#' @description clean column names
#' @param data dataset to clean
clean_column_names <- function(data){
  names(data) <- unlist(lapply(strsplit(names(data), '-'), function(a){a[length(a)]}))
  return(data)
}

#' @description  clean pii column
#' @param data data to sanitize
clean_pii_columns <- function(data){
  pii_columns <- c(
     'archivist_select',
     'dob_pulled',
     'ento_le',
     'firstname',
     'fullname',
     'hecon_members',
     'herd_id',
     'hh_head',
     'hh_head_string',
     'hh_head_sub',
     'hh_head_sub_string',
     'hhid_barcode',
     'hhid_calculate',
     'hhid_manual',
     'hhid_print',
     'hhid_select',
     'household_members',
     'instance_label',
     'instancename',
     'lastname',
     'member_select',
     'participant_name',
     'person_string',
     'taken'
  )
  data %>% dplyr::select(-any_of(pii_columns))
}

hash_columns <- function(data) {

  encrypt_list <- c(
    'archivist_id',
    'cl_id',
    'cl_wid_sending',
    'extid',
    'fa_id',
    'fa_wid_receiving',
    'fa_wid_sending',
    'hhid',
    'id_username',
    'pharma_wid_receiving',
    'pharma_wid_sending',
    'pkstaff_issuing',
    'pkstaff_receiving',
    'sfa_wid_receiving',
    'sfa_wid_sending'
  )

  data %>%
    dplyr::rowwise() %>%
    dplyr::mutate(across(any_of(encrypt_list), function(x){  digest(x, algo = 'sha1') }))
}


clean_artifacts <- function(data) {
  data %>%
    janitor::remove_empty(which = "cols")
}





jitter_location <- function(data) {

  coord_cols <- c('Longitude', 'Latitude')
  set.seed(1001)
  if(coord_cols %in% names(data) %>% all()){
    return(data %>%
      dplyr::group_by(hhid) %>%
      dplyr::mutate(
        longitude_new = `Longitude` + (0.01 * sin(runif(1) * 2 * base::pi)),
        latitude_new = `Latitude` + (0.01 * cos(runif(1) * 2 * base::pi)),
      ))
  }else{
    return(data)
  }
}

standardize_col_value_case <- function(data, col_names){
  tryCatch({
    data %>%
      dplyr::mutate(!!sym(col_names) := toupper(stringr::str_squish(stringr::str_to_title(!!sym(col_names)))))
  }, error = function(e){
    return(data)
  })
}

standardize_col_dobs <- function(data, col_names){
  tryCatch({
    data %>%
      dplyr::mutate(!!sym(col_names) := lubridate::date(!!sym(col_names)))
  }, error = function(e){
    return(data)
  })
}

standardize_village <- function(data) {
  tryCatch({
    data %>%
      dplyr::mutate(ward = toupper(ward)) %>%
      dplyr::mutate(village_specify = toupper(stringr::str_replace_all(village_specify, 'NGUZ0', 'NGUZO'))) %>%
      dplyr::mutate(village = toupper(stringr::str_replace_all(village, 'NGUZ0', 'NGUZO'))) %>%
      dplyr::mutate(village_select = toupper(stringr::str_replace_all(village_select, 'NGUZ0', 'NGUZO')))
  }, error = function(e){
    return(data)
  })

}

#' @description data type conversions
#' @param series data series to convert
convert_datatype <- function(series){
  if(inherits(series,  'numeric')) {
    change_data_type_funs = as.numeric
  } else if (inherits(series, 'integer')) {
    change_data_type_funs = as.integer
  } else if (inherits(series, 'character')){
    change_data_type_funs = as.character
  } else if (inherits(series, "Date")) {
    change_data_type_funs = lubridate::date
  } else if (inherits(series, "logical")) {
    change_data_type_funs = as.logical
  }else{
    logger::log_error('Data type unrecognizable')
    stop()
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

    if(length(target_cols) != 0){
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
                             dplyr::filter(repeat_name == "" | is.na(repeat_name)),
                           by = 'instanceID')
      }

      # loop through all changes for target columns
      purrr::map(target_cols, function(col){
        logger::log_info(glue::glue('Batch set loop on {form_id} col:{col}'))
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
    }else{
      logger::log_success(glue::glue('Nothing to change on {form_id}-{repeat_name}'))
      return(data)
    }
  }, error = function(e){
    logger::log_error(e$message)
    stop(e$message)
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

      # files to delete in repeats
      to_delete <- resolution %>%
        dplyr::filter(Operation == 'DELETE',
                      !(RepeatName == "" | is.na(RepeatName))) %>%
        dplyr::select(form_id = Form,
                      repeat_name = RepeatName,
                      repeat_key = RepeatKey,
                      PARENT_KEY = instanceID)
      # files to delete specifically from parent
      to_delete_from_parent <- resolution %>%
        dplyr::filter(Operation == 'DELETE',
                      (RepeatName == "" | is.na(RepeatName))
        ) %>%
        dplyr::select(PARENT_KEY = instanceID)

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
          everything()) %>%
        dplyr::filter(!PARENT_KEY %in% unique(to_delete_from_parent$PARENT_KEY))
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
  if(nrow(resolution) > 0 & nrow(data) > 0){
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

#' @description Add cluster geo num
#' If longitude or latitude exists, add cluster geo num accross forms
#' THIS USES NEW CLUSTER LISTED HERE IN BK: https://bohemiakenya.slack.com/archives/C042KSRLYUA/p1690895077884209
add_cluster_geo_num <- function(data,
                                form_id,
                                repeat_name,
                                buffer_size = 50){
  logger::log_info(glue::glue('Reassigning cluster / core number to {form_id}-{repeat_name}'))
  target_cols <- c('instanceID', 'Longitude', 'Latitude')


  # first pass, check if instance id and target cols exist and have numeric datatypes
  if(names(data) %in% target_cols %>% sum() == length(target_cols)){
    data_proj <- data %>%
      dplyr::select(instanceID, Longitude, Latitude) %>%
      dplyr::filter(inherits(Latitude, 'numeric'),
                    inherits(Longitude, "numeric")) %>%
      dplyr::filter(!is.na(Longitude)) %>%
      dplyr::distinct()
  }else{
    logger::log_success(glue::glue('Skip Reassigning cluster / core number to {form_id}-{repeat_name}'))
    return(data)
  }

  # process data if it has more than 0 rows
  if(nrow(data_proj) > 0){
    tryCatch({
      p4s <- "+proj=utm +zone=37 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
      crs <- CRS(p4s)
      llcrs <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0")

      # data projection
      coordinates(data_proj) <- ~Longitude+Latitude
      proj4string(data_proj) <- llcrs
      data_proj <- spTransform(data_proj, crs)

      load('assets/clusters.RData')
      old_clusters <- clusters

      # clusters projection
      old_clusters_projected <- spTransform(
        old_clusters,
        crs)
      # see which households are in which clusters with NO buffering first
      old_clusters_projected <- spTransform(old_clusters, crs)
      o <- sp::over(data_proj, polygons(old_clusters_projected))
      data_proj@data$cluster_geo <- old_clusters_projected$cluster_number[o]
      # for households which are not strictly in the cluster boundaries, associate them
      # with a cluster if within 50 meters
      old_clusters_projected_buffered <- rgeos::gBuffer(spgeom = old_clusters_projected,
                                                        byid = TRUE, width = 50)
      o <- sp::over(data_proj, polygons(old_clusters_projected_buffered))
      data_proj@data$not_in_old_cluster <- is.na(o)
      data_proj@data$cluster_with_buffer <- old_clusters_projected_buffered@data$cluster_number[o]
      data_proj@data$old_cluster_correct <-
        ifelse(is.na(data_proj@data$cluster_geo),
               data_proj@data$cluster_with_buffer,
               data_proj@data$cluster_geo)

      data_final <- left_join(data,
                 data_proj@data %>%
                   dplyr::select(instanceID,
                                 geo_cluster_num = old_cluster_correct,
                                 geo_not_in_cluster = not_in_old_cluster),
                 by = 'instanceID')

      logger::log_success(glue::glue('Success Reassigning cluster / core number to {form_id}-{repeat_name}'))
      return(data_final)

    }, error = function(e){
      logger::log_error(glue::glue('{form_id}-{repeat_name} is throwing error:{e$message}'))
    })

  }else{
    logger::log_success(glue::glue('Skip Reassigning cluster / core number to {form_id}-{repeat_name}'))
    return(data)
  }
}


# function to expand resolution file with connected cols
expand_resolution_file_with_connected_cols <- function(resolution_file) {

  # connected cols based on the same field
  mapping <-   tribble(
    ~source, ~cascade_to,
    "dob",   "dob_select",
    "dob",   "dob_string",
    "extid", "extid_calculate",
    "hhid",  "hhid_calculate",
    "hhid",  "hh_qr",
    "person_absent_reason", "person_absent",
    "person_absent_reason", "person_unenrolled_migrated",
    "person_absent_reason", "person_out_absent",
    "person_absent_reason", "out",
    "person_absent_reason", "migrated_status"
  )


  expanded_resolution_file <- resolution_file %>%
    dplyr::filter(`Operation` == 'SET') %>%
    dplyr::inner_join(mapping, by = c(`Column` = 'source')) %>%
    dplyr::mutate(`Column` = cascade_to) %>%
    dplyr::select(all_of(names(resolution_file)))


  # Expand based on mapping mentioned in this thread for efficacy absences
  expanded_resolution_file <- expanded_resolution_file %>%
    dplyr::mutate(`Set To` = case_when(`Set To` == 'Absent' & `Column` == 'person_absent' & `Form` == 'efficacy' ~ "1",
                                       `Set To` == 'Absent' & `Column` == 'person_unenrolled_migrated' & `Form` == 'efficacy' ~ "0",
                                       `Set To` == 'Absent' & `Column` == 'person_out_absent' & `Form` == 'efficacy' ~ "1",
                                       `Set To` == 'Absent' & `Column` == 'out' & `Form` == 'efficacy' ~ "1",
                                       `Set To` == 'Absent' & `Column` == 'migrated_status' & `Form` == 'efficacy' ~ "0",
                                       TRUE ~ `Set To`
                                       ))

  output <- dplyr::bind_rows(resolution_file, expanded_resolution_file)


  return(output)

}
