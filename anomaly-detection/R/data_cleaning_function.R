#' Helper function to set row values by uuid
#' @param data data input (tibble)
#' @param uuid the uuid of data (character)
#' @param mapping list of change in the row (list)
#' return tibble with changed row values
set_row_values <- function(data, uuid, mapping){
  map_value <- tribble(
    ~instanceID,
    uuid
  ) %>% bind_cols(mapping %>% as_tibble())
  purrr::map()
}

#' Helper function to set column values in batches
#' @param data data input (tibble)
#' @param mapping data mapping
#' @param col column to set values in
#' @param id row id
#' return tibble with changed row values
batch_set_row_values <- function(data,
                                 mapping,
                                 col,
                                 id = "instanceID",
                                 change_data_type_funs = NULL){

  mapping <- mapping %>%
    dplyr::filter(Column == col)

  to_drop <- mapping %>%
    dplyr::group_by(instanceID, Column) %>%
    dplyr::summarise(n = dplyr::n(), .groups = "drop") %>%
    dplyr::filter(n > 1L)

  mapping <- mapping %>%
    distinct(.) %>%
    dplyr::anti_join(to_drop, by=c("instanceID", "Column")) %>%
    tidyr::pivot_wider(names_from = Column,
                       values_from = `Set To`,
                       id_cols = instanceID) %>%
    dplyr::mutate_at(c(col),change_data_type_funs)

  # anti join to get unaffected rows
  unaffected_rows <- data %>%
    dplyr::anti_join(mapping, by  = id)

  # get col_new and col_old
  # when joining a column will have a suffix .x (left), .y (right)
  col_new <- glue::glue("{col}.x")
  col_old <- glue::glue("{col}.y")

  # do left join and set column with new column,
  # remove any .x, .y suffix columns
  # bind with unaffected rows
  mapping %>%
    dplyr::left_join(data, by = id) %>%
    dplyr::mutate(!!sym(col) := !!sym(col_new)) %>%
    dplyr::select(-all_of(c(col_new, col_old))) %>%
    dplyr::bind_rows(unaffected_rows)
}

#' Helper function to set row values by uuid
#' @param data data input (tibble)
#' @param uuid list of uuid
#' return deleted rows
delete_row_values <- function(data, instanceIDs = NULL){
  data %>%
    dplyr::anti_join(instanceIDs, by = "instanceID")
}


#' This script is purposed to be cleaning function for data from s3 and saved to cleaned table
#' @param data
#' @return cleaned household data
clean_household_data <- function(data, resolution_file){

  # read local resolution file
  resolution_file <- resolution_file %>%
    dplyr::filter(Form == 'household') %>%
    tibble::as_tibble()

  # create mapping
  set_rows_mapping <- resolution_file %>%
    dplyr::filter(Operation == 'SET') %>%
    dplyr::select(instanceID, Operation, Column, `Set To`)
  delete_rows <- resolution_file %>%
    dplyr::filter(Operation == 'DELETE') %>%
    dplyr::select(instanceID)

  # consolidate here
  data <- data %>%
    batch_set_row_values(
      mapping = set_rows_mapping,
      col = 'hh_id',
      change_data_type_funs = as.character)  %>%
    delete_row_values(
      instanceIDs = delete_rows)


  return(data)
}


#' Function to clean registration forms
#' @param data registration forms
#' @return clean registration form
clean_registration_data <- function(data, resolution_file){

    # read local resolution file
    resolution_file <- resolution_file %>%
      dplyr::filter(Form == 'registration') %>%
      tibble::as_tibble()

    # create mapping
    set_rows_mapping <- resolution_file %>%
      dplyr::filter(Operation == 'SET') %>%
      dplyr::select(instanceID, Operation, Column, `Set To`)
    delete_rows <- resolution_file %>%
      dplyr::filter(Operation == 'DELETE') %>%
      dplyr::select(instanceID)

    # consolidate here
    data <- data %>%
      batch_set_row_values(
        mapping = set_rows_mapping,
        col = 'wid',
        change_data_type_funs = as.numeric)  %>%
      delete_row_values(
        instanceIDs = delete_rows)
  return(data)
}


