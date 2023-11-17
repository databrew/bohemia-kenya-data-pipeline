library(reactable)
library(data.table)
library(dplyr)
library(htmltools)
library(glue)
library(fontawesome)
library(ggplot2)
library(plotly)
library(tidyr)

ENV_PIPELINE_STAGE <- Sys.getenv("PIPELINE_STAGE")
DATA_STAGING_BUCKET_NAME <- 'databrew.org'
DATA_LAKE_BUCKET_NAME <- 'bohemia-lake-db'
PROJECT_SOURCE <- 'kwale'
SE_FOLDER_TARGET <- glue::glue('{PROJECT_SOURCE}/clean-form')
CLUSTER_TO_REMOVE <- c(1,4,6,3,35,47,52,66,71,76,86,89)

tryCatch({
  logger::log_info('Attempt AWS login')
  # login to AWS - this will be bypassed if executed in CI/CD environment
  cloudbrewr::aws_login(
    role_name = 'cloudbrewr-aws-role',
    profile_name =  'cloudbrewr-aws-role',
    pipeline_stage = ENV_PIPELINE_STAGE)

}, error = function(e){
  logger::log_error('AWS Login Failed')
  stop(e$message)
})

# Function to pad hhid
pad_hhid <- function(data){
  if('hhid' %in% names(data)){
    data %>%
      dplyr::mutate(hhid = stringr::str_pad(hhid, 5, pad = "0"))
  }else{
    data
  }
}

# Taken from Joe's generate_metadata.R  (https://bohemiakenya.slack.com/archives/C042KSRLYUA/p1698943128467689?thread_ts=1698907791.681019&cid=C042KSRLYUA)
# This is relevant for safety to remove any household with all individuals that has refusal status
# NOTE: THIS IS NOT USING SAFETY_STATUS
get_refusals <- function() {
  dx <- safety_merged_tbl %>%
    # special categories for "refusal"
    mutate(is_refusal = (!is.na(obvious_screening) & obvious_screening == 'Refusal') |
             ind_icf_thumbprint == 'no' |
             ind_agree_sign_icf == 'no' |
             minor_agree_sign_assent == 'no') %>%
    dplyr::select(hhid, visit, extid, is_refusal) %>%
    bind_rows(safetynew_merged_tbl %>%
                # special categories for "refusal"
                mutate(is_refusal = obvious_screening == 'Refusal' |
                         ind_icf_thumbprint == 'no' |
                         ind_agree_sign_icf == 'no' |
                         minor_agree_sign_assent == 'no')) %>%
    mutate(is_refusal = ifelse(is.na(is_refusal), FALSE, is_refusal)) %>%
    group_by(visit, hhid) %>%
    summarise(n_refusals = sum(as.numeric(is_refusal)),
              n_individual_submissions = n()) %>%
    ungroup %>%
    mutate(p_refusals = n_refusals / n_individual_submissions * 100) %>%
    arrange(desc(p_refusals))
  all_refusals <- dx %>%
    filter(p_refusals == 100) %>%
    arrange(hhid) %>%
    dplyr::mutate(is_refusal = TRUE) %>%
    dplyr::select(visit, hhid, is_refusal)
  return(all_refusals)
}


# Get Safety Targets
# Logic:
# 1. Remove cluster 1,4,6,3,35,47,52,66,71,76,86,89
# 2. V1 goals are coming from V0
# 3. V2-V4 goals are cascaded from previous visits
# 4. IF ALL INDIVIDUALS REFUSED IN A HOUSEHOLD, please remove household
# 5. IF INDIVIDUALS EOS, remove in next visit
# 6. IF ALL INDIVIDUALS EOS, no need to remove household in next visit as we can still trigger safetynew
get_safety_targets <- function(){
  master_list <- list()

  # create targets
  v0_target <- v0_merged_tbl %>%
    dplyr::group_by(assignment, cluster, village) %>%
    dplyr::summarise(
      hh_target = n_distinct(hhid),
      ind_target = n_distinct(extid))

  master_list$v1_goals <- v0_target %>%
    dplyr::ungroup() %>%
    dplyr::mutate(visit = 'V1')

  master_list$cascade_target <- dplyr::bind_rows(
    safetynew_merged_tbl  %>%
      dplyr::select(visit, assignment, cluster, village, extid, hhid, safety_status) %>% distinct(),
    safety_merged_tbl  %>%
      dplyr::select(visit, assignment, cluster, village, extid, hhid, safety_status),
  ) %>%
    dplyr::left_join(all_refusals) %>%
    dplyr::filter(is.na(is_refusal)) %>%
    dplyr::group_by(visit, assignment, cluster, village) %>%
    dplyr::summarise(
      hh_target = n_distinct(hhid),
      ind_target = n_distinct(extid),
      ind_eos = n_distinct(extid[safety_status=='eos'])) %>%
    dplyr::mutate(
      ind_target = ind_target - ind_eos,
      next_visit_num = as.numeric(stringr::str_extract(visit, "[0-9]+")) + 1,
      visit = glue::glue('V{next_visit_num}')) %>%
    dplyr::filter(next_visit_num < 5) %>% # stop cascading goals
    dplyr::select(-ind_eos, -next_visit_num)

  target <- master_list %>% purrr::reduce(dplyr::bind_rows)

  return(target)
}


# Get EfficacyTargets
# Logic:
# 1. Remove cluster 1,4,6,3,35,47,52,66,71,76,86,89
# 2. V1 goals are coming from V0
# 3. V2-V4 goals are cascaded from previous visits
# 4. IF INDIVIDUALS EOS, remove in next visit
# 5. IF ALL INDIVIDUALS EOS, no need to remove household in next visit as we can still trigger safetynew
get_efficacy_targets <- function(){
  master_list <- list()

  # create targets
  v0_target <- efficacy_selection %>%
    dplyr::left_join(v0_merged_tbl) %>%
    dplyr::group_by(assignment, cluster, village) %>%
    dplyr::summarise(
      hh_target = n_distinct(hhid),
      ind_target = n_distinct(extid)) %>%
    dplyr::ungroup()

  master_list$v1_goals <- v0_target %>%
    dplyr::ungroup() %>%
    dplyr::mutate(visit = 'V1')


  # Cascade down for V2 and so on
  # 1. Get safety new numbers and safety
  # 2. Remove individual if EOS
  # 3. If a household have all EOS, don't remove it since there can be new members
  # create targets coming from safety new
  master_list$cascade_target <- dplyr::bind_rows(
    efficacy_merged_tbl  %>%
      dplyr::select(visit, assignment, cluster, village, extid, hhid, efficacy_status)
  ) %>%
    dplyr::group_by(visit, assignment, cluster, village) %>%
    dplyr::summarise(
      hh_target = n_distinct(hhid),
      ind_target = n_distinct(extid),
      ind_eos = n_distinct(extid[efficacy_status=='eos'])) %>%
    dplyr::mutate(
      ind_target = ind_target - ind_eos,
      next_visit_num = as.numeric(stringr::str_extract(visit, "[0-9]+")) + 1,
      visit = glue::glue('V{next_visit_num}')) %>%
    dplyr::filter(next_visit_num < 8) %>% # stop cascading goals
    dplyr::select(-ind_eos, -next_visit_num)

  target <- master_list %>% purrr::reduce(dplyr::bind_rows)

  return(target)
}



#################################
# Fetch v0 demography
#################################
worker_registration <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = 'kwale/clean-form/reconaregistration/reconaregistration.csv'
) %>%
  pad_hhid()

v0 <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = 'kwale/clean-form/v0demography/v0demography.csv'
) %>%
  pad_hhid()

v0_repeat <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = 'kwale/clean-form/v0demography/v0demography-repeat_individual.csv'
) %>%
  pad_hhid()


efficacy <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = glue::glue('{PROJECT_SOURCE}/clean-form/efficacy/efficacy.csv')) %>%
  pad_hhid()

safety <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = glue::glue('{PROJECT_SOURCE}/clean-form/safety/safety.csv')) %>%
  pad_hhid()

safetynew <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = glue::glue('{PROJECT_SOURCE}/clean-form/safetynew/safetynew.csv')) %>%
  pad_hhid()

safety_repeat_individual <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = glue::glue('{PROJECT_SOURCE}/clean-form/safety/safety-repeat_individual.csv')) %>%
  pad_hhid()

safetynew_repeat_individual <- cloudbrewr::aws_s3_get_table(
  bucket = 'databrew.org',
  key = glue::glue('{PROJECT_SOURCE}/clean-form/safetynew/safetynew-repeat_individual.csv')) %>%
  pad_hhid()


assignment <- cloudbrewr::aws_s3_get_table(
  bucket = 'bohemia-lake-db',
  key = glue::glue('bohemia_prod/dim_arm_assignment/assignments.csv')) %>%
  pad_hhid()


efficacy_selection <- cloudbrewr::aws_s3_get_table(
  bucket = 'bohemia-lake-db',
  key = glue::glue('bohemia_ext/efficacy_selection.csv')) %>%
  pad_hhid() %>%
  dplyr::select(extid)

# village mapping
village_mapping <- v0 %>%
  dplyr::select(hhid, village) %>%
  unique()

# v0 merged_tbl
v0_merged_tbl <- v0 %>%
  dplyr::inner_join(v0_repeat, by = c('KEY' = 'PARENT_KEY'))  %>%
  dplyr::inner_join(
    assignment,
    by = c('geo_cluster_num' = 'cluster_number')) %>%
  dplyr::filter(!cluster %in% CLUSTER_TO_REMOVE)

# prep safety table
safety_merged_tbl <- safety %>%
  dplyr::inner_join(safety_repeat_individual, by = c('KEY' = 'PARENT_KEY')) %>%
  dplyr::left_join(assignment, by = c('cluster' = 'cluster_number')) %>%
  dplyr::left_join(village_mapping, by = 'hhid')  %>%
  dplyr::filter(!cluster %in% CLUSTER_TO_REMOVE) %>%
  # get most recent submission
  dplyr::group_by(visit, extid) %>%
  dplyr::mutate(max_time = max(end_time)) %>%
  dplyr::ungroup() %>%
  dplyr::filter(end_time == max_time,
                !is.na(visit))

# prep safety new
safetynew_merged_tbl <- safetynew %>%
  dplyr::inner_join(safetynew_repeat_individual, by = c('KEY' = 'PARENT_KEY')) %>%
  dplyr::left_join(assignment, by = c('cluster' = 'cluster_number')) %>%
  dplyr::left_join(village_mapping, by = 'hhid') %>%
  dplyr::filter(!cluster %in% CLUSTER_TO_REMOVE) %>%
  # get most recent submission
  dplyr::group_by(visit, extid) %>%
  dplyr::mutate(max_time = max(end_time)) %>%
  dplyr::ungroup() %>%
  dplyr::filter(end_time == max_time,
                !is.na(visit))


# efficacy df
efficacy_merged_tbl <- efficacy %>%
  dplyr::inner_join(assignment %>%
                      dplyr::select(cluster_number, assignment),
                    by = c('cluster' = 'cluster_number')) %>%
  dplyr::filter(!cluster %in% CLUSTER_TO_REMOVE) %>%
  # get most recent submission
  dplyr::group_by(visit, extid) %>%
  dplyr::mutate(max_time = max(end_time)) %>%
  dplyr::ungroup() %>%
  dplyr::filter(end_time == max_time,
                !is.na(visit))


# Consolidate all goals
all_refusals <- get_refusals()
safety_targets <- get_safety_targets()
efficacy_targets <- get_efficacy_targets()


# Store Goals to S3
dir.create('output/bohemia_prod/dwh/goal_safety_targets', recursive = TRUE)
safety_targets %>%
  tidyr::drop_na() %>%
  fwrite('output/bohemia_prod/dwh/goal_safety_targets/goal_safety_targets.csv')

# Store Efficacy Goals to S3
dir.create('output/bohemia_prod/dwh/goal_efficacy_targets', recursive = TRUE)
efficacy_targets %>%
  tidyr::drop_na() %>%
  fwrite('output/bohemia_prod/dwh/goal_efficacy_targets/goal_efficacy_targets.csv')

cloudbrewr::aws_s3_bulk_store(
  bucket = 'bohemia-lake-db',
  prefix = '/bohemia_prod',
  target_dir = 'output/bohemia_prod/'
)

