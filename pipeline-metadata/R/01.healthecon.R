###########################################################################################################################
#' Metadata generation for healtheconomics
#' Description: This script is used for metadata generation for healtheconomics and will store it to bohemia-lake-db
#' Author: joe.brew@gmail.com, atediarjo@gmail.com
#'
#' Data Sources: Go to Data Sources Section
#' Logic / Business Definition:
#' - v0_roster: V0Demography and Repeats
#' - hecon_new_roster: Data taken from healtheconnew
#' - household_eos: If Household is set to EOS then it is EOS entirely to the individual level
#' - individual_eos: Apply to individual if they are EOS, NOT THE WHOLE HOUSEHOLD
#' - v1_safety_status: ONLY V1 safety status
#' - hecon_departures: Filter on individual that is NON-ABSENT and remove any user that is migrated or dead
#' - hecon_baseline_in_out: Get in/out/eos for each household
#' - hecon_visits_done: Combine baseline and monthly check in to append V1 to V4 visits
###########################################################################################################################


library(logger)
library(purrr)
library(dplyr)
library(cloudbrewr)
library(data.table)
library(lubridate)
library(readr)
library(stringr)
source('R/utils.R')
source('R/data_utils.R')

# Global Variables
ENV_PIPELINE_STAGE <- Sys.getenv("PIPELINE_STAGE")
FORMS_BUCKET_NAME <- 'databrew.org'
DLAKE_BUCKET_NAME <- 'bohemia-lake-db'
RUN_DATE <- as.character(lubridate::date(lubridate::now()))
TARGET_S3_FOLDER <- 'metadata'

# ODK Project Target based on different pipeline stages
if(ENV_PIPELINE_STAGE != 'production'){
  V0_FOLDER_TARGET <- 'kwale'
  HECON_FOLDER_TARGET <- 'health_economics_testing'
  SAFETY_FOLDER_TARGET <- 'test_of_test'

}else{
  HECON_FOLDER_TARGET <- V0_FOLDER_TARGET <- SAFETY_FOLDER_TARGET <- 'kwale'

}


################################
# DATA SOURCES
################################

# FORM DATA SOURCES
FORM_INPUT_KEY <- list(
  v0demography = glue::glue('{V0_FOLDER_TARGET}/clean-form/v0demography/v0demography.csv'),
  v0demography_repeat_individual = glue::glue('{V0_FOLDER_TARGET}/clean-form/v0demography/v0demography-repeat_individual.csv'),
  healtheconnew  = glue::glue('{HECON_FOLDER_TARGET}/clean-form/healtheconnew/healtheconnew.csv'),
  healtheconnew_repeat_individual = glue::glue('{HECON_FOLDER_TARGET}/clean-form/healtheconnew/healtheconnew-repeat_individual.csv'),
  healtheconbaseline  = glue::glue('{HECON_FOLDER_TARGET}/clean-form/healtheconbaseline/healtheconbaseline.csv'),
  healtheconbaseline_repeat_individual = glue::glue('{HECON_FOLDER_TARGET}/clean-form/healtheconbaseline/healtheconbaseline-repeat_individual.csv'),
  healtheconbaseline_repeat_miss_work_school = glue::glue('{HECON_FOLDER_TARGET}/clean-form/healtheconbaseline/healtheconbaseline-repeat_miss_work_school.csv'),
  healtheconbaseline_repeat_other_employment_details = glue::glue('{HECON_FOLDER_TARGET}/clean-form/healtheconbaseline/healtheconbaseline-repeat_other_employment_details.csv'),
  healtheconmonthly  = glue::glue('{HECON_FOLDER_TARGET}/clean-form/healtheconmonthlyz/healtheconmonthlyz.csv'),
  healtheconmonthly_repeat_individual = glue::glue('{HECON_FOLDER_TARGET}/clean-form/healtheconmonthlyz/healtheconmonthlyz-repeat_individual.csv'),
  healtheconmonthly_repeat_miss_work_school = glue::glue('{HECON_FOLDER_TARGET}/clean-form/healtheconmonthlyz/healtheconmonthlyz-repeat_miss_work_school.csv'),
  healtheconmonthly_repeat_other_employment_details = glue::glue('{HECON_FOLDER_TARGET}/clean-form/healtheconmonthlyz/healtheconmonthlyz-repeat_other_employment_details.csv'),
  safety  = glue::glue('{SAFETY_FOLDER_TARGET}/clean-form/safety/safety.csv'),
  safety_repeat_individual = glue::glue('{SAFETY_FOLDER_TARGET}/clean-form/safety/safety-repeat_individual.csv'),
  safetynew = glue::glue('{SAFETY_FOLDER_TARGET}/clean-form/safetynew/safetynew.csv'),
  safetynew_repeat_individual = glue::glue('{SAFETY_FOLDER_TARGET}/clean-form/safetynew/safetynew-repeat_individual.csv')
)

# EXTERNAL DATA SOURCES
EXTERNAL_INPUT_KEY <- list(
  assignments = glue::glue('ext/assignments/assignments.csv'),
  intervention_assignments = glue::glue('ext/intervention_assignment/intervention_assignment.csv'),
  health_economics_cluster = glue::glue('ext/health_economics_clusters/health_economics_clusters.csv'),
  health_economics_households = glue::glue('ext/health_economics_households/health_economics_households.csv'),
  ntd_efficacy_preselection  = glue::glue('ext/health_economics_ntd_efficacy_preselection/health_economics_ntd_efficacy_preselection.csv'),
  ntd_safety_preselection = glue::glue('ext/health_economics_ntd_efficacy_preselection/health_economics_ntd_efficacy_preselection.csv')
)


# Attempt login to AWS Profile
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

# NOTES: RETURN EMPTY TIBBLE IF DATA DOES NOT EXIST
forms_list <- purrr::map(FORM_INPUT_KEY, function(key){
  try_read_to_s3(bucket = FORMS_BUCKET_NAME,
                 key = key) %>% cleanup()
})

randomization_list <- purrr::map(EXTERNAL_INPUT_KEY, function(key){
  try_read_to_s3(bucket = DLAKE_BUCKET_NAME,
                 key = key) %>% cleanup()
})

# combine data list
data_list <- c(forms_list, randomization_list)



######################################################
# Create intermediary tables
######################################################

# CTE 1: Get Initial Starting Roster Based on V0Demography
v0_roster <- tryCatch({
  logger::log_info('Fetch starting roster from v0')
  data_list$v0demography_repeat_individual %>%
    left_join(data_list$v0demography %>%
                dplyr::select(hhid, start_time, KEY),
              by = c('PARENT_KEY' = 'KEY')) %>%
    arrange(desc(start_time)) %>%
    dplyr::distinct(extid, .keep_all = TRUE) %>%
    dplyr::select(hhid, start_time, firstname, lastname, dob, sex, extid)
}, error = function(e){
  logger::log_error(e$message)
  logger::log_info('Returning NULL')
  return(NULL)
})

# CTE 2: Get new individual from starting roster in Health Economics
hecon_new_roster <- tryCatch({
  logger::log_info('Fetch hecon new individual')
  data_list$healtheconnew_repeat_individual %>%
    left_join(data_list$healtheconnew %>%
                dplyr::select(hhid, start_time, KEY),
              by = c('PARENT_KEY' = 'KEY')) %>%
    dplyr::select(hhid, start_time, firstname, lastname, dob, sex, extid) %>%
    dplyr::distinct(extid, .keep_all = TRUE) %>%
    mutate(hhid = as.character(hhid))
}, error = function(e){
  logger::log_error(e$message)
  logger::log_info('Returning NULL')
  return(NULL)
})

# CTE 3: Get Most Recent Statuses from Health Economics Repeats Individual
# Get the starting health economics status of each individual
# This should be most recent hecon_individual_status, which should be overridden and turned to eos if in the heconmonthly form hecon_household_status = eos
hecon_starting_statuses <- tryCatch({
  logger::log_info('Fetching hecon starting statuses')
  bind_rows(
    data_list$healtheconnew_repeat_individual %>%
      dplyr::select(extid, PARENT_KEY, hecon_individual_status) %>%
      left_join(data_list$healtheconnew %>%
                  dplyr::select(KEY, start_time) %>%
                  mutate(start_time = as.Date(start_time)),
                by = c('PARENT_KEY' = 'KEY')),
    data_list$healtheconbaseline_repeat_individual %>%
      dplyr::select(extid, PARENT_KEY, hecon_individual_status) %>%
      left_join(data_list$healtheconbaseline %>% dplyr::select(KEY, start_time) %>%
                  mutate(start_time = as.Date(start_time)), by = c('PARENT_KEY' = 'KEY')),
    data_list$healtheconmonthly_repeat_individual %>%
      dplyr::select(extid, PARENT_KEY, hecon_individual_status) %>%
      left_join(data_list$healtheconmonthly %>% dplyr::select(KEY, start_time) %>%
                  mutate(start_time = as.Date(start_time)), by = c('PARENT_KEY' = 'KEY'))
  ) %>%
    filter(!is.na(hecon_individual_status)) %>%
    arrange(desc(start_time)) %>%
    dplyr::distinct(extid, .keep_all = TRUE) %>%
    dplyr::select(extid, starting_hecon_status = hecon_individual_status)
}, error = function(e){
  logger::log_error(e$message)
  logger::log_info('Returning NULL')
  return(NULL)
})

# CTE 4: Check in main forms if HH in HECON is EOS
hecon_household_eos <- tryCatch({
  logger::log_info('Fetching hecon household eos')
  bind_rows(
    data_list$healtheconmonthly %>%
      dplyr::select(hhid, start_time, hecon_household_status),
    data_list$healtheconbaseline %>%
      dplyr::select(hhid, start_time, hecon_household_status)
  ) %>%
    filter(hecon_household_status == 'eos')
}, error = function(e){
  logger::log_error(e$message)
  logger::log_info('Returning NULL')
  return(NULL)
})

# CTE 5: Check in repeat forms if individuals in HECON is EOS
hecon_individual_ever_eos <- tryCatch({
  logger::log_info('Fetching hecon individual ever eos')
  bind_rows(
    data_list$healtheconnew_repeat_individual %>%
      dplyr::select(extid, PARENT_KEY, hecon_individual_status) %>%
      left_join(data_list$healtheconnew %>%
                  dplyr::select(KEY, start_time) %>%
                  mutate(start_time = as.Date(start_time)),
                by = c('PARENT_KEY' = 'KEY')),
    data_list$healtheconbaseline_repeat_individual %>%
      dplyr::select(extid, PARENT_KEY, hecon_individual_status) %>%
      left_join(data_list$healtheconbaseline %>%
                  dplyr::select(KEY, start_time) %>%
                  mutate(start_time = as.Date(start_time)),
                by = c('PARENT_KEY' = 'KEY')),
    data_list$healtheconmonthly_repeat_individual %>%
      dplyr::select(extid, PARENT_KEY, hecon_individual_status) %>%
      left_join(data_list$healtheconmonthly %>%
                  dplyr::select(KEY, start_time) %>%
                  mutate(start_time = as.Date(start_time)),
                by = c('PARENT_KEY' = 'KEY'))
  ) %>%
    filter(!is.na(hecon_individual_status)) %>%
    filter(hecon_individual_status == 'eos') %>%
    filter(!is.na(extid))

}, error = function(e){
  logger::log_error(e$message)
  logger::log_info('Returning NULL')
  return(NULL)
})

# CTE 6: Get visit 1 safety status
v1_safety_status <- tryCatch({
  logger::log_info('Fetching v1 safety statuses')
  bind_rows(
    data_list$safety_repeat_individual %>%
      left_join(data_list$safety %>%
                  dplyr::select(KEY, visit, start_time),
                by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(start_time = as.Date(start_time),
             extid = as.character(extid),
             safety_status = as.character(safety_status)) %>%
      dplyr::select(start_time, extid, safety_status, visit) %>%
      mutate(form = 'safety'),
    data_list$safetynew_repeat_individual %>%
      left_join(data_list$safetynew %>%
                  dplyr::select(KEY, visit, start_time),
                by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(start_time = as.Date(start_time),
             extid = as.character(extid),
             safety_status = as.character(safety_status)) %>%
      dplyr::select(start_time, extid, safety_status, visit) %>%
      mutate(form = 'safetynew')
  ) %>%
    # keep only visit 1
    filter(visit == 'V1') %>%
    # keep only most recent (relevant in case of duplicates)
    arrange(desc(start_time)) %>%
    dplyr::distinct(extid, .keep_all = TRUE) %>%
    dplyr::select(extid, v1_safety_status = safety_status)

}, error = function(e){
  logger::log_error(e$message)
  logger::log_info('Returning NULL')
  return(NULL)
})

# CTE 7: Deal with migrations / deaths
hecon_departures <- tryCatch({
  logger::log_info('Fetching hecon departures')
  bind_rows(
    data_list$healtheconbaseline_repeat_individual %>%
      left_join(data_list$healtheconbaseline %>% dplyr::select(KEY, start_time), by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(extid = as.character(extid)) %>%
      mutate(start_time = lubridate::as_datetime(start_time)) %>%
      filter(!is.na(person_absent_reason)) %>%
      filter(person_absent_reason != 'Absent') %>%
      dplyr::select(start_time, extid, person_absent_reason),
    data_list$healtheconmonthly_repeat_individual %>%
      left_join(data_list$healtheconmonthly %>% dplyr::select(KEY, start_time), by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(extid = as.character(extid)) %>%
      mutate(start_time = lubridate::as_datetime(start_time)) %>%
      filter(!is.na(person_absent_reason)) %>%
      filter(person_absent_reason != 'Absent') %>%
      dplyr::select(start_time, extid, person_absent_reason)
  )
}, error = function(e){
  logger::log_error(e$message)
  logger::log_info('Returning NULL')
  return(NULL)
})

# CTE 8: Get Assignments (use dummy if in development)
assignments <- tryCatch({
  if(!REAL_PRESELECTIONS){
    get_fake_assignments()
  }else{
    randomization_list$assignments
  }
}, error = function(e){
  logger::log_error(e$message)
  logger::log_info('Returning NULL')
  return(NULL)
})

# CTE 9: Get v0 household heads
v0_hh_heads <- tryCatch({
  data_list$v0demography_repeat_individual %>%
    filter(hh_head_yn == 'yes') %>%
    left_join(data_list$v0demography %>% dplyr::select(hhid, start_time, KEY), by = c('PARENT_KEY' = 'KEY')) %>%
    dplyr::select(hhid, start_time, firstname, lastname, extid) %>%
    arrange(desc(start_time)) %>%
    dplyr::distinct(hhid, .keep_all = TRUE) %>%
    mutate(household_head = paste0(firstname, ' ', lastname)) %>%
    dplyr::select(hhid, household_head)
}, error = function(e){
  logger::log_error(e$message)
  logger::log_info('Returning NULL')
  return(NULL)
})

# CTE 10: Get Baseline In/Out Status:
hecon_baseline_in_out <- tryCatch({
  # Get baseline in/out status
 data_list$healtheconbaseline %>%
    mutate(start_time = as.POSIXct(start_time)) %>%
    dplyr::select(start_time, hhid, hecon_hh_status = hecon_household_status) %>%
    bind_rows(data_list$healtheconmonthly %>%
                mutate(start_time = as.POSIXct(start_time)) %>%
                dplyr::select(start_time, hhid, hecon_hh_status = hecon_household_status)) %>%
    arrange(desc(start_time)) %>%
    dplyr::distinct(hhid, .keep_all = TRUE)
})

# CTE 11: Get Health Econ Visits done (V1, V2, V3 ....). Use Hecon baseline as base and bind with monthly updates from hecon
hecon_visits_done <- tryCatch({
  data_list$healtheconbaseline %>%
    mutate(visit = 'V1', hhid = as.character(hhid)) %>%
    dplyr::select(hhid, visit) %>%
    bind_rows(data_list$healtheconmonthly %>%
                mutate(hhid = as.character(hhid)) %>%
                dplyr::select(hhid, visit)) %>%
    group_by(hhid) %>%
    summarise(visits_done = paste0(sort(unique(visit)), collapse = ', '))
}, error = function(e){

})


######################################################
# Consolidate intermediary tables to starting roster
######################################################
starting_roster <- bind_rows(v0_roster, hecon_new_roster) %>%
  dplyr::mutate(firstname = stringr::str_replace_all(firstname, "[^[:alnum:]]", ""),
                lastname = stringr::str_replace_all(lastname, "[^[:alnum:]]", "")) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::mutate(fullname_dob = paste0(firstname, ' ', lastname, ' | ', dob)) %>%
  mutate(roster_name = paste0(firstname, ' ', lastname, ' (',
                              extid, ')')) %>%
  dplyr::rename(fullname_id = roster_name) %>%
  mutate(hecon_name = paste0(firstname, '-', lastname, '-', extid)) %>%
  left_join(hecon_starting_statuses) %>%
  mutate(starting_hecon_status = ifelse(is.na(starting_hecon_status),
                                        'out',
                                        starting_hecon_status)) %>%
  # Join the safety statuses from v1; in the case of someone who doesn't have a v1 status, this should
  # be NA per https://bohemiakenya.slack.com/archives/C058WT0ADBN/p1691172066177829?thread_ts=1691171197.538939&cid=C058WT0ADBN
  left_join(v1_safety_status)

# Run checks on EOS based on household level
if(nrow(hecon_household_eos) > 0){
  starting_roster$starting_hecon_status[starting_roster$hhid %in% hecon_household_eos$hhid] <- 'eos'
}

# Run checks on EOS based on individual level
if(nrow(hecon_individual_ever_eos) > 0) {
  starting_roster$starting_hecon_status[starting_roster$extid %in% hecon_individual_ever_eos$extid] <- 'eos'
}

# Paula's instructions (https://docs.google.com/document/d/1Tjpyh8O9oesnDiQgjEih1VpOIZFctpM7UA5aDK--N8o/edit)
starting_roster <- starting_roster %>%
  mutate(ntd_safety_preselected = ifelse(extid %in% data_list$ntd_safety_preselection$extid, 1, 0)) %>%
  mutate(ntd_efficacy_preselected = ifelse(extid %in% data_list$ntd_efficacy_preselection$extid, 1, 0))

hecon_deaths <- hecon_departures %>%
  filter(person_absent_reason == 'Died')
hecon_migrations <- hecon_departures %>%
  filter(person_absent_reason == 'Migrated')

# Set all migrants and dead people to eos
if(nrow(hecon_deaths) > 0){
  starting_roster$dead <- ifelse(starting_roster$extid %in% hecon_deaths$extid, 1, 0)
  starting_roster$dead[is.na(starting_roster$dead)] <- 0
  starting_roster$starting_hecon_status <- ifelse(starting_roster$extid %in% hecon_deaths$extid, 'eos', starting_roster$starting_hecon_status)
} else {
  starting_roster$dead <- 0
}
if(nrow(hecon_migrations) > 0){
  starting_roster$migrated <- ifelse(starting_roster$extid %in% hecon_migrations$extid, 1, 0)
  starting_roster$migrated[is.na(starting_roster$migrated)] <- 0
  starting_roster$starting_hecon_status <- ifelse(starting_roster$extid %in% hecon_migrations$extid, 'eos', starting_roster$starting_hecon_status)
} else {
  starting_roster$migrated <- 0
}

# Remove those who are dead and migrated
starting_roster <- starting_roster %>%
  filter(migrated != 1, dead != 1)


######################################################
# Consolidate starting roster to household data
######################################################
households <- starting_roster %>%
  mutate(roster_name = paste0(firstname, ' ', lastname, ' (',
                              extid, ')')) %>%
  group_by(hhid) %>%
  summarise(roster = paste0(roster_name, collapse = ', '),
            num_members = n()) %>%
  # get cluster
  left_join(data_list$v0demography %>%
              dplyr::select(hhid, cluster = geo_cluster_num)) %>%
  # get assignments
  left_join(randomization_list$assignments %>%
              dplyr::select(
                cluster = cluster_number,
                arm = assignment)) %>%
  # get interventions
  left_join(randomization_list$intervention_assignments) %>%
  left_join(v0_hh_heads) %>%
  left_join(hecon_visits_done) %>%
  left_join(hecon_baseline_in_out) %>%
  mutate(hecon_hh_status = ifelse(is.na(hecon_hh_status), 'out', hecon_hh_status)) %>%
  mutate(hecon_hh_preselected = ifelse(hhid %in%
                                         randomization_list$health_economics_households$hhid, 1, 0),
         herd_preselected = ifelse(hhid %in%
                                     randomization_list$health_economics_households$hhid[
                                       randomization_list$health_economics_households$herd_preselected == 'yes'], 1, 0),
         hecon_members = roster) %>%
  filter(hhid %in% randomization_list$health_economics_households$hhid)


######################################################
# Consolidate starting roster based on hecon households
######################################################
individuals <- starting_roster %>% filter(hhid %in% households$hhid)


######################################################
# Store .csv to target folder
######################################################
# Write csvs to a directory
if(!dir.exists('health_economics_metadata')){
  dir.create('health_economics_metadata')
}
write_csv(households, 'health_economics_metadata/household_data.csv')
write_csv(individuals, 'health_economics_metadata/individual_data.csv')


# Save current version files
cloudbrewr::aws_s3_store(
  filename = 'health_economics_metadata/household_data.csv',
  bucket = 'bohemia-lake-db',
  key =  glue::glue('{TARGET_S3_FOLDER}/health_economics_household/household_data.csv')
)

cloudbrewr::aws_s3_store(
  filename = 'health_economics_metadata/individual_data.csv',
  bucket = 'bohemia-lake-db',
  key =  glue::glue('{TARGET_S3_FOLDER}/health_economics_individual/individual_data.csv')
)

# Save metadata by run date
cloudbrewr::aws_s3_store(
  filename = 'health_economics_metadata/household_data.csv',
  bucket = 'bohemia-lake-db',
  key =  glue::glue('{TARGET_S3_FOLDER}/health_economics_household_hist/run_date={RUN_DATE}/household_data.csv')
)

cloudbrewr::aws_s3_store(
  filename = 'health_economics_metadata/individual_data.csv',
  bucket = 'bohemia-lake-db',
  key =  glue::glue('{TARGET_S3_FOLDER}/health_economics_individual_hist/run_date={RUN_DATE}/individual_data.csv')
)

unlink('health_economics_metadata', recursive = TRUE, force = TRUE)
