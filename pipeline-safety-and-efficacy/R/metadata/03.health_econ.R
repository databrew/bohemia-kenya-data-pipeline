# Metadata generation for healtheconomics
#


library(logger)
library(purrr)
library(dplyr)
library(cloudbrewr)
library(data.table)
library(sf)
library(sp)
library(lubridate)
library(readr)
source('R/metadata/utils.R')

# Global Variables
ENV_PIPELINE_STAGE <- Sys.getenv("PIPELINE_STAGE")
FORMS_BUCKET_NAME <- 'databrew.org'
DLAKE_BUCKET_NAME <- 'bohemia-lake-db'

# ODK Project Target based on different pipeline stages
if(ENV_PIPELINE_STAGE != 'production'){
  FOLDER_TARGET <- 'kwale_testing'
}else{
  FOLDER_TARGET <- 'kwale'
}

# Input Key
FORM_INPUT_KEY <- list(
  v0demography = glue::glue('{FOLDER_TARGET}/clean-form/v0demography/v0demography.csv'),
  v0demography_repeat_individual = glue::glue('{FOLDER_TARGET}/clean-form/v0demography/v0demography-repeat_individual.csv'),
  healtheconnew  = glue::glue('{FOLDER_TARGET}/clean-form/healtheconnew/healtheconnew.csv'),
  healtheconnew_repeat_individual = glue::glue('{FOLDER_TARGET}/clean-form/healtheconnew/healtheconnew-repeat_individual.csv'),
  healtheconnew_repeat_miss_work_school = glue::glue('{FOLDER_TARGET}/clean-form/healtheconnew/healtheconnew-repeat_miss_work_school.csv'),
  healtheconnew_repeat_other_employment_details = glue::glue('{FOLDER_TARGET}/clean-form/healtheconnew/healtheconnew-repeat_other_employment_details.csv'),
  healtheconbaseline  = glue::glue('{FOLDER_TARGET}/clean-form/healtheconbaseline/healtheconbaseline.csv'),
  healtheconbaseline_repeat_individual = glue::glue('{FOLDER_TARGET}/clean-form/healtheconbaseline/healtheconbaseline-repeat_individual.csv'),
  healtheconbaseline_repeat_miss_work_school = glue::glue('{FOLDER_TARGET}/clean-form/healtheconbaseline/healtheconbaseline-repeat_miss_work_school.csv'),
  healtheconbaseline_repeat_other_employment_details = glue::glue('{FOLDER_TARGET}/clean-form/healtheconbaseline/healtheconbaseline-repeat_other_employment_details.csv'),
  healtheconmonthly  = glue::glue('{FOLDER_TARGET}/clean-form/healtheconmonthly/healtheconmonthly.csv'),
  healtheconmonthly_repeat_individual = glue::glue('{FOLDER_TARGET}/clean-form/healtheconmonthly/healtheconmonthly-repeat_individual.csv'),
  healtheconmonthly_repeat_miss_work_school = glue::glue('{FOLDER_TARGET}/clean-form/healtheconmonthly/healtheconmonthly-repeat_miss_work_school.csv'),
  healtheconmonthly_repeat_other_employment_details = glue::glue('{FOLDER_TARGET}/clean-form/healtheconmonthly/healtheconmonthly-repeat_other_employment_details.csv'),
  safety  = glue::glue('{FOLDER_TARGET}/clean-form/safety/safety.csv'),
  safety_repeat_individual = glue::glue('{FOLDER_TARGET}/clean-form/safety/safety-repeat_individual.csv'),
  safetynew = glue::glue('{FOLDER_TARGET}/clean-form/safetynew/safetynew.csv'),
  safetynew_repeat_individual = glue::glue('{FOLDER_TARGET}/clean-form/safetynew/safetynew-repeat_individual.csv')
)

# External data input key
EXTERNAL_INPUT_KEY <- list(
  health_economics_cluster = glue::glue('bohemia_ext/randomization/health_economics_clusters.csv'),
  health_economics_households = glue::glue('bohemia_ext/randomization/health_economics_households.csv'),
  ntd_efficacy_preselection  = glue::glue('bohemia_ext/randomization/health_economics_ntd_efficacy_preselection.csv'),
  ntd_safety_preselection = glue::glue('bohemia_ext/randomization/health_economics_ntd_efficacy_preselection.csv')
)


# Attempt login to AWS via Profile
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

# STEP 1: Get Initial Starting Roster Based on V0Demography
starting_roster <- tryCatch({
  data_list$v0demography_repeat_individual %>%
    left_join(data_list$v0demography %>% dplyr::select(hhid, todays_date, KEY), by = c('PARENT_KEY' = 'KEY')) %>%
    arrange(desc(todays_date)) %>%
    dplyr::distinct(extid, .keep_all = TRUE) %>%
    dplyr::select(hhid, todays_date, firstname, lastname, dob, sex, extid)
}, error = function(e){
  logger::log_error(e$message)
  logger::log_info('Returning NULL')
  return(NULL)
})

# STEP 2: Get new individual from starting roster in Health Economics
new_people <- tryCatch({
  data_list$healtheconnew_repeat_individual %>%
    left_join(data_list$healtheconnew %>%
                dplyr::select(hhid, todays_date, KEY),
              by = c('PARENT_KEY' = 'KEY')) %>%
    dplyr::select(hhid, todays_date, firstname, lastname, dob, sex, extid) %>%
    dplyr::distinct(extid, .keep_all = TRUE) %>%
    mutate(hhid = as.character(hhid))
}, error = function(e){
  logger::log_error(e$message)
  logger::log_info('Returning NULL')
  return(NULL)
})


# STEP 3: Combine Roster for both v0 and health econ
starting_roster <- tryCatch({
  bind_rows(starting_roster, new_people) %>%
    arrange(desc(todays_date)) %>%
    dplyr::distinct(extid, .keep_all = TRUE) %>%
    dplyr::mutate(fullname_dob = paste0(firstname, ' ', lastname, ' | ', dob)) %>%
    mutate(roster_name = paste0(firstname, ' ', lastname, ' (',
                                extid, ')')) %>%
    dplyr::rename(fullname_id = roster_name) %>%
    mutate(hecon_name = paste0(firstname, '-', lastname, '-', extid))

}, error = function(e){
  logger::log_error(e$message)
  logger::log_info('Returning NULL')
  return(NULL)
})


# STEP 4: Get Most Recent Statuses
# Get the starting health economics status of each individual
# This should be most recent hecon_individual_status, which should be overridden and turned to eos if in the heconmonthly form hecon_household_status = eos
starting_hecon_statuses <- tryCatch({
  bind_rows(
    data_list$healtheconnew_repeat_individual %>%
      dplyr::select(extid, PARENT_KEY, hecon_individual_status) %>%
      left_join(data_list$healtheconnew %>%
                  dplyr::select(KEY, todays_date) %>%
                  mutate(todays_date = as.Date(todays_date)),
                by = c('PARENT_KEY' = 'KEY')),
    data_list$healtheconbaseline_repeat_individual %>%
      dplyr::select(extid, PARENT_KEY, hecon_individual_status) %>%
      left_join(data_list$healtheconbaseline %>% dplyr::select(KEY, todays_date) %>%
                  mutate(todays_date = as.Date(todays_date)), by = c('PARENT_KEY' = 'KEY')),
    data_list$healtheconmonthly_repeat_individual %>%
      dplyr::select(extid, PARENT_KEY, hecon_individual_status) %>%
      left_join(data_list$healtheconmonthly %>% dplyr::select(KEY, todays_date) %>%
                  mutate(todays_date = as.Date(todays_date)), by = c('PARENT_KEY' = 'KEY'))
  ) %>%
    filter(!is.na(hecon_individual_status)) %>%
    arrange(desc(todays_date)) %>%
    dplyr::distinct(extid, .keep_all = TRUE) %>%
    dplyr::select(extid, starting_hecon_status = hecon_individual_status)
}, error = function(e){
  logger::log_error(e$message)
  logger::log_info('Returning NULL')
  return(NULL)
})

# STEP 5: Join starting roster with information coming from hecon to get information
starting_roster <- left_join(
    starting_roster,
    starting_hecon_statuses) %>%
  mutate(starting_hecon_status = ifelse(is.na(starting_hecon_status),
                                        'out',
                                        starting_hecon_status))



# STEP 6: Check in main forms if HECON is EOS
household_eos <-
  bind_rows(
    data_list$healtheconmonthly %>%
      dplyr::select(hhid, todays_date, hecon_household_status),
    data_list$healtheconbaseline %>%
      dplyr::select(hhid, todays_date, hecon_household_status)
  ) %>%
  filter(hecon_household_status == 'eos')

if(nrow(household_eos) > 0){
  starting_roster$starting_hecon_status[starting_roster$hhid %in% household_eos$hhid] <- 'eos'
}

# If ever eos, always eos
ever_eos <-
  bind_rows(
    data_list$healtheconnew_repeat_individual %>%
      dplyr::select(extid, PARENT_KEY, hecon_individual_status) %>%
      left_join(data_list$healtheconnew %>%
                  dplyr::select(KEY, todays_date) %>%
                  mutate(todays_date = as.Date(todays_date)),
                by = c('PARENT_KEY' = 'KEY')),
    data_list$healtheconbaseline_repeat_individual %>%
      dplyr::select(extid, PARENT_KEY, hecon_individual_status) %>%
      left_join(data_list$healtheconbaseline %>%
                  dplyr::select(KEY, todays_date) %>%
                  mutate(todays_date = as.Date(todays_date)), by = c('PARENT_KEY' = 'KEY')),
    data_list$healtheconmonthly_repeat_individual %>%
      dplyr::select(extid, PARENT_KEY, hecon_individual_status) %>%
      left_join(data_list$healtheconmonthly %>%
                  dplyr::select(KEY, todays_date) %>%
                  mutate(todays_date = as.Date(todays_date)), by = c('PARENT_KEY' = 'KEY'))
  ) %>%
  filter(!is.na(hecon_individual_status)) %>%
  filter(hecon_individual_status == 'eos') %>%
  filter(!is.na(extid)) %>%
  dplyr::pull(extid)
starting_roster$starting_hecon_status[starting_roster$extid %in% ever_eos] <- 'eos'


# Paula's instructions (https://docs.google.com/document/d/1Tjpyh8O9oesnDiQgjEih1VpOIZFctpM7UA5aDK--N8o/edit)
starting_roster <- starting_roster %>%
  mutate(ntd_safety_preselected = ifelse(extid %in% data_list$ntd_safety_preselection$extid, 1, 0)) %>%
  mutate(ntd_efficacy_preselected = ifelse(extid %in% data_list$ntd_efficacy_preselection$extid, 1, 0))


# TODO ADD SAFETY
# Get visit 1 safety status
v1_safety_status <-
  bind_rows(
    data_list$safety_repeat_individual %>%
      left_join(data_list$safety %>%
                  dplyr::select(KEY, visit, todays_date),
                by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(todays_date = as.Date(todays_date),
             extid = as.character(extid),
             safety_status = as.character(safety_status)) %>%
      dplyr::select(todays_date, extid, safety_status, visit) %>%
      mutate(form = 'safety'),
    data_list$safetynew_repeat_individual %>%
      left_join(data_list$safetynew %>%
                  dplyr::select(KEY, visit, todays_date),
                by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(todays_date = as.Date(todays_date),
             extid = as.character(extid),
             safety_status = as.character(safety_status)) %>%
      dplyr::select(todays_date, extid, safety_status, visit) %>%
      mutate(form = 'safetynew')
  ) %>%
  # keep only visit 1
  filter(visit == 'V1') %>%
  # keep only most recent (relevant in case of duplicates)
  arrange(desc(todays_date)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::select(extid, v1_safety_status = safety_status)

# Join the safety statuses from v1; in the case of someone who doesn't have a v1 status, this should
# be NA per https://bohemiakenya.slack.com/archives/C058WT0ADBN/p1691172066177829?thread_ts=1691171197.538939&cid=C058WT0ADBN
starting_roster <- left_join(starting_roster, v1_safety_status)
# Deal with migrations / deaths
healthecon_departures <-
  bind_rows(
    data_list$healtheconbaseline_repeat_individual %>%
      left_join(data_list$healtheconbaseline %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(extid = as.character(extid)) %>%
      mutate(todays_date = lubridate::as_datetime(todays_date)) %>%
      filter(!is.na(person_absent_reason)) %>%
      filter(person_absent_reason != 'Absent') %>%
      dplyr::select(todays_date, extid, person_absent_reason),
    data_list$healtheconmonthly_repeat_individual %>%
      left_join(data_list$healtheconmonthly %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(extid = as.character(extid)) %>%
      mutate(todays_date = lubridate::as_datetime(todays_date)) %>%
      filter(!is.na(person_absent_reason)) %>%
      filter(person_absent_reason != 'Absent') %>%
      dplyr::select(todays_date, extid, person_absent_reason)
  )
healthecon_deaths <- healthecon_departures %>%
  filter(person_absent_reason == 'Died')
healthecon_migrations <- healthecon_departures %>%
  filter(person_absent_reason == 'Migrated')
# Set all migrants and dead people to eos
if(nrow(healthecon_deaths) > 0){
  starting_roster$dead <- ifelse(starting_roster$extid %in% healthecon_deaths$extid, 1, 0)
  starting_roster$dead[is.na(starting_roster$dead)] <- 0
  starting_roster$starting_hecon_status <- ifelse(starting_roster$extid %in% healthecon_deaths$extid, 'eos', starting_roster$starting_hecon_status)
} else {
  starting_roster$dead <- 0
}
if(nrow(healthecon_migrations) > 0){
  starting_roster$migrated <- ifelse(starting_roster$extid %in% healthecon_migrations$extid, 1, 0)
  starting_roster$migrated[is.na(starting_roster$migrated)] <- 0
  starting_roster$starting_hecon_status <- ifelse(starting_roster$extid %in% healthecon_migrations$extid, 'eos', starting_roster$starting_hecon_status)
} else {
  starting_roster$migrated <- 0
}

# Remove those who are dead and migrated
starting_roster <- starting_roster %>%
  filter(migrated != 1, dead != 1)

# Now use individual data to make household data
households <- starting_roster %>%
  mutate(roster_name = paste0(firstname, ' ', lastname, ' (',
                              extid, ')')) %>%
  group_by(hhid) %>%
  summarise(roster = paste0(roster_name[dead == 0 & migrated == 0], collapse = ', '),
            num_members = length(which(dead == 0 & migrated == 0))) %>%
  # get cluster
  left_join(v0demography %>%
              dplyr::select(hhid, cluster)) %>%
  # get intervention
  left_join(assignments %>% dplyr::select(cluster = cluster_number,
                                          arm = assignment)) %>%
  # get interventions
  left_join(intervention_assignment)
# Get household heads and geographic information
heads <- v0demography_repeat_individual %>%
  filter(hh_head_yn == 'yes') %>%
  left_join(v0demography %>% dplyr::select(hhid, todays_date, KEY), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid, todays_date, firstname, lastname, extid) %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(hhid, .keep_all = TRUE) %>%
  mutate(household_head = paste0(firstname, ' ', lastname)) %>%
  dplyr::select(hhid, household_head)
households <- households %>%
  left_join(heads)
# Get visits done
visits_done <- healtheconbaseline %>%
  mutate(visit = 'V1', hhid = as.character(hhid)) %>%
  dplyr::select(hhid, visit) %>%
  bind_rows(healtheconmonthly %>%
              mutate(hhid = as.character(hhid)) %>%
              dplyr::select(hhid, visit)) %>%
  group_by(hhid) %>%
  summarise(visits_done = paste0(sort(unique(visit)), collapse = ', '))
households <- left_join(households, visits_done)
# Get hecon_hh_preselected,
households <- households %>%
  mutate(hecon_hh_preselected = ifelse(hhid %in% health_economics_households$hhid, 1, 0))
# Get hecon_members
households$hecon_members <- households$roster
# Create the herd preselected variable (just random values for now)
households <- households %>%
  mutate(herd_preselected = ifelse(hhid %in% health_economics_households$hhid[health_economics_households$herd_preselected == 'yes'], 1, 0))
# Keep only health economics households
households <- households %>% filter(hhid %in% health_economics_households$hhid)
starting_roster <- starting_roster %>% filter(hhid %in% households$hhid)
# Write csvs
if(!dir.exists('health_economics_metadata')){
  dir.create('health_economics_metadata')
}
write_csv(households, 'health_economics_metadata/household_data.csv')
write_csv(starting_roster, 'health_economics_metadata/individual_data.csv')

