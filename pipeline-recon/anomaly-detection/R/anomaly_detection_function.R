#' This function is used for getting duplicated CHV id
#' @data registration data
#' @return tibble with column type, anomaly_id, description
get_duplicated_chv_id <- function(data){
  data %>%
    group_by(wid = `wid`) %>%
    summarise(n = n(),
              instances = paste0(sort(unique(`instanceID`)), collapse = ';')) %>%
    mutate(description = glue::glue(
      'Worker ID {wid} was used {n} times in the Recon A',
      ' registration form, but worker IDs should be unique',
      ' and therefore only entered once.')) %>%
    filter(n > 1) %>%
    mutate(type = 'recona_duplicate_id') %>%
    mutate(anomaly_id = paste0(type, '_', instances)) %>%
    dplyr::select(type, anomaly_id, description)
}


#' This function is used for getting wid that reports as both CHA and CHV
#' @data registration data
#' @return tibble with column type, anomaly_id, description
get_identical_cha_chv <- function(data){
  cha <-  data %>%
    dplyr::filter(worker_type == 'CHV') %>%
    dplyr::select(`id` = `wid`,
                  chv_instance = `instanceID`)

  chv <- data %>%
    dplyr::filter(worker_type == 'CHA') %>%
    dplyr::select(`id` = `wid`,
                  cha_instance = `instanceID`)

  cha %>%
    inner_join(chv) %>%
    mutate(description = glue::glue(
      'ID: {id} was used for both a CHV (instance = {chv_instance} ) and a CHA (instance = {cha_instance}).')) %>%
    mutate(type = 'recona_id_for_both_cha_and_chv') %>%
    mutate(anomaly_id = paste0(type, '_', cha_instance, '_', chv_instance)) %>%
    dplyr::select(type, anomaly_id, description)
}

#' Mismatch between number of CHVs that indicate a CHA as their supervisor and the number of reported CHVs supervised by a CHA
get_mistmatch_between_reported_cha_chv_numbers <- function(data){
  return(data)
}


#' This function is used for getting duplicated Household id
#' @data registration data
#' @return tibble with column type, anomaly_id, description
get_duplicated_hh_id <- function(data) {
  data %>%
    dplyr::group_by(hh_id) %>%
    dplyr::summarise( n = n(), instances = paste0(sort(unique(`instanceID`)), collapse = ';')) %>%
    dplyr::ungroup() %>%
    dplyr::filter(n > 1) %>%
    dplyr::mutate(type = 'reconb_duplicate_id',
                  anomaly_id = glue::glue("{type}_{instances}"),
                  description = glue::glue(
                    'Household ID {hh_id} was used {n} times in the Recon B',
                    ' household form, but Household IDs should be unique',
                    ' and therefore only entered once.')) %>%
    dplyr::select(type, anomaly_id, description)
}


#' This function is used for getting Mismatch between number of CHVs
#' that indicate a CHA as their supervisor and the number of
#' reported CHVs supervised by a CHA
#' @data registration data
#' @return tibble with column type, anomaly_id, description
get_mistmached_cha_chv_numbers <- function(data){
  #' get number of cha supervision as reported from CHV
  n_by_chv <- data %>%
    mutate(wid_cha = ifelse(is.na(cha_wid_qr), cha_wid_manual, cha_wid_qr)) %>%
    dplyr::filter(worker_type == 'CHV') %>%
    dplyr::select(wid_cha, wid, instanceID) %>%
    dplyr::group_by(wid_cha) %>%
    dplyr::summarise(number_chv_supervise = n_distinct(wid),
                     instances = paste0(sort(unique(`instanceID`)), collapse = ';')) %>%
    dplyr::select(wid = wid_cha, number_chv_supervise, instances)

  #' get number of cha supervision as reported from CHA
  n_by_cha <- data %>%
    dplyr::filter(worker_type == 'CHA') %>%
    dplyr::select(wid, number_chv_supervise, instances = instanceID)

  #' create mismatch report by joining the two summary tables
  #' and search for mismatch in reported numbers
  mismatch <- n_by_cha %>%
    dplyr::inner_join(n_by_chv, by = c('wid')) %>%
    dplyr::mutate(
      number_chv_supervise.x = tidyr::replace_na(number_chv_supervise.x, 0),
      number_chv_supervise.y = tidyr::replace_na(number_chv_supervise.y, 0),
      is_mismatch = ifelse(
        number_chv_supervise.x != number_chv_supervise.y, TRUE, FALSE),
      instances = glue::glue("{instances.x};{instances.y}")) %>%
    dplyr::filter(is_mismatch) %>%
    dplyr::mutate(type = 'recona_mismatch_cha_supervise_chv',
                  anomaly_id = glue::glue("{type}_{instances}"),
                  description = glue::glue(
                    'CHA ID:{wid} reported {number_chv_supervise.x} of CHVs supervised.',
                    ' However CHVs reported that {wid} is supervising {number_chv_supervise.y} CHVs')) %>%
    dplyr::select(type, anomaly_id, description)

  return(mismatch)

}

