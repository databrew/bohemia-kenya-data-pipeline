#########################################################
# Safety metadata generation script
#
# Owner: joe.brew@gmail.com, atediarjo@gmail.com
#
# Logic used:
# -
#
#
#
#
#########################################################


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
  FOLDER_TARGET <- 'test_of_test'
  REAL_PRESELECTION <- FALSE
}else{
  FOLDER_TARGET <- 'kwale'
  REAL_PRESELECTION <- TRUE
}

# Input Key
FORM_INPUT_KEY <- list(
  v0demography = glue::glue('{FOLDER_TARGET}/clean-form/v0demography/v0demography.csv'),
  v0demography_repeat_individual = glue::glue('{FOLDER_TARGET}/clean-form/v0demography/v0demography-repeat_individual.csv'),
  safety  = glue::glue('{FOLDER_TARGET}/clean-form/safety/safety.csv'),
  safety_repeat_individual = glue::glue('{FOLDER_TARGET}/clean-form/safety/safety-repeat_individual.csv'),
  safetynew = glue::glue('{FOLDER_TARGET}/clean-form/safetynew/safetynew.csv'),
  safetynew_repeat_individual = glue::glue('{FOLDER_TARGET}/clean-form/safetynew/safetynew-repeat_individual.csv'),
  efficacy  = glue::glue('{FOLDER_TARGET}/clean-form/efficacy/efficacy.csv'),
  pfu  = glue::glue('{FOLDER_TARGET}/clean-form/pfu/pfu.csv')
)

# External data input key
EXTERNAL_INPUT_KEY <- list(
  efficacy_selection = glue::glue('bohemia_ext/randomization/efficacy_selection.csv')
)

##########################################################
# 0. Fetch data
#########################################################

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


##########################################################
# 1. Build Roster
#########################################################

# Get the starting roster
starting_roster <- data_list$v0demography_repeat_individual %>%
  left_join(data_list$v0demography %>% dplyr::select(hhid, todays_date, KEY), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid, todays_date, firstname, lastname, dob, sex, extid) %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  mutate(remove = FALSE) %>%
  mutate(index = 1:nrow(.)) %>%
  dplyr::mutate(dead = 0, migrated = 0)

# Flag the deaths (the "efficacy_deaths" and "efficacy_deaths" object is created in the safety section)
# Go through each departure and flag people as dead or migrated
starting_roster$dead <- starting_roster$migrated <- 0

if(nrow(efficacy_deaths) > 0){
  starting_roster$dead[starting_roster$extid %in% efficacy_deaths$extid] <- 1
}
if(nrow(efficacy_departures) > 0){
  starting_roster$migrated[starting_roster$extid %in% efficacy_departures$extid[efficacy_departures$person_absent_reason == 'Migrated']] <- 1
}

# Remove those who are dead and migrated
starting_roster <- starting_roster %>%
  filter(migrated != 1, dead != 1)

roster <- starting_roster %>%
  arrange(desc(todays_date)) %>%
  # keep only the most recent case
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  mutate(roster_name = paste0(firstname, ' ', lastname, ' (',
                              extid, ')')) %>%
  dplyr::select(hhid, extid, firstname, lastname, sex, dob, roster_name, dead, migrated) %>%
  # set dead and migrated to 0 if new arrivals
  mutate(dead = ifelse(is.na(dead), 0, dead),
         migrated = ifelse(is.na(migrated), 0, migrated))

# Add more info to individuals
individuals <- roster %>%
  dplyr::mutate(fullname_dob = paste0(firstname, ' ', lastname, ' | ', dob)) %>%
  dplyr::rename(fullname_id = roster_name) %>%
  # get intervention, village, ward, cluster
  left_join(households %>% dplyr::select(hhid, intervention, village, ward, cluster))


##########################################################
# 2. Get Weights and Heights
#########################################################

# Get starting weight
starting_weights <-
  bind_rows(
    data_list$safety_repeat_individual %>% filter(!is.na(current_weight)) %>%
      left_join(data_list$safety %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), current_weight = as.character(current_weight)) %>%
      dplyr::select(todays_date, extid, starting_weight = current_weight) %>% mutate(form = 'safety'),

    data_list$safetynew_repeat_individual %>% filter(!is.na(current_weight)) %>%
      left_join(data_list$safetynew %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), current_weight = as.character(current_weight)) %>%
      dplyr::select(todays_date, extid, starting_weight = current_weight) %>% mutate(form = 'safetynew'),

    data_list$efficacy %>% filter(!is.na(current_weight)) %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), current_weight = as.character(current_weight)) %>%
      dplyr::select(todays_date, extid, starting_weight = current_weight) %>% mutate(form = 'efficacy'),

    data_list$pfu %>% filter(!is.na(weight)) %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), weight = as.character(weight)) %>%
      dplyr::select(todays_date, extid, starting_weight = weight) %>% mutate(form = 'pfu')
  ) %>%
  filter(!is.na(extid), !is.na(starting_weight)) %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::select(extid, starting_weight)
individuals <- left_join(individuals, starting_weights)

# Get starting height
starting_heights <-
  bind_rows(
    data_list$safety_repeat_individual %>%
      mutate(height = ifelse(is.na(height), height_short, height)) %>%
      filter(!is.na(height)) %>%
      left_join(data_list$safety %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      dplyr::select(todays_date, extid, starting_height = height) %>% mutate(form = 'safety') %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), starting_height = as.character(starting_height)),
    data_list$safetynew_repeat_individual %>% filter(!is.na(height)) %>%
      left_join(data_list$safetynew %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      dplyr::select(todays_date, extid, starting_height = height) %>% mutate(form = 'safetynew') %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), starting_height = as.character(starting_height)),
    data_list$efficacy %>% filter(!is.na(height)) %>%
      dplyr::select(todays_date, extid, starting_height = height) %>% mutate(form = 'efficacy') %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), starting_height = as.character(starting_height))
  ) %>%
  filter(!is.na(extid), !is.na(starting_height)) %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::select(extid, starting_height)



# (generated in safety section)
individuals <- left_join(individuals, starting_weights)
# Get starting height (generated in safety section)
individuals <- left_join(individuals, starting_heights)

##########################################################
# 3. Define Efficacy Preselection
#########################################################

# # Get efficacy status  ################################################
# g <- gsheet::gsheet2tbl('https://docs.google.com/spreadsheets/d/1gff7p0MKejzllSEp7ONunpaSufvTWXxafktPK4xyCys/edit#gid=1430667203')
# g$extid[g$efficacy_preselected == 1]
if(REAL_PRESELECTION){
  efficacy_selection <- data_list$efficacy_selection$extid
  efficacy_preselected_ids <- sort(unique(efficacy_selection$extid))
  # # one-off: list of households in efficacy with cls for mercy
  # mercy <- efficacy_selection %>%
  #   mutate(hhid = substr(extid, 1, 5)) %>%
  #   dplyr::distinct(hhid) %>%
  #   arrange(hhid) %>%
  #   left_join(v0demography %>% dplyr::distinct(hhid, .keep_all = TRUE) %>%
  #               dplyr::select(hhid, wid))
} else {
  efficacy_preselected_ids <- c("01000-01",
                                "01000-04",
                                "12013-03",
                                "34102-02",
                                "34102-03",
                                "20001-01",
                                "20001-02",
                                "72034-01",
                                "72034-02")
}

individuals$efficacy_preselected <- ifelse(individuals$extid %in% efficacy_preselected_ids, 1, 0)
efficacy_ids <- sort(unique(individuals$extid[individuals$efficacy_preselected == 1]))

# Get some further efficacy status variables
# starting_efficacy_status
right <- data_list$efficacy %>%
  arrange(desc(todays_date)) %>%
  filter(!is.na(efficacy_status)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::mutate(starting_efficacy_status = efficacy_status) %>%
  dplyr::select(extid, starting_efficacy_status) %>%
  mutate(extid = as.character(extid))
# if someone has not yet been visited in efficacy AND they are preselected, they should get
# a starting status of "out"
individuals <- left_join(individuals, right) %>%
  mutate(starting_efficacy_status = ifelse(is.na(starting_efficacy_status) & extid %in% efficacy_ids,
                                           'out',
                                           starting_efficacy_status))

# efficacy_absent_most_recent_visit
right <- data_list$efficacy %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  filter(!is.na(person_absent), !is.na(extid)) %>%
  dplyr::mutate(efficacy_absent_most_recent_visit = person_absent) %>%
  dplyr::select(extid, efficacy_absent_most_recent_visit)
if(nrow(right) > 0){
  individuals <- left_join(individuals, right) %>%
    mutate(efficacy_absent_most_recent_visit = ifelse(is.na(efficacy_absent_most_recent_visit), 0, efficacy_absent_most_recent_visit))
} else {
  individuals$efficacy_absent_most_recent_visit <- 0
}

# efficacy_most_recent_visit_present
right <- data_list$efficacy %>%
  arrange(desc(todays_date)) %>%
  filter(!is.na(person_present_continue)) %>%
  filter(person_present_continue == 1) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::mutate(efficacy_absent_most_recent_visit = as.numeric(gsub('V', '', visit))) %>%
  dplyr::select(extid, efficacy_most_recent_visit_present,
                efficacy_most_recent_present_date = todays_date) %>%
  mutate(efficacy_most_recent_present_date = paste0('.', as.character(efficacy_most_recent_present_date))) %>%
  mutate(extid = as.character(extid))
individuals <- left_join(individuals, right)

# efficacy_visits_done
# (this includes absent visits) https://bohemiakenya.slack.com/archives/C042KSRLYUA/p1690378013017559?thread_ts=1690307177.615709&cid=C042KSRLYUA
right <- data_list$efficacy %>%
  filter(!is.na(extid)) %>%
  group_by(extid) %>%
  summarise(efficacy_visits_done = paste0(sort(unique(visit)), collapse = ', '))
if(nrow(right) > 0){
  individuals <- left_join(individuals, right)
} else {
  individuals$efficacy_visits_done <- ""
}

# If ever eos, always eos
ever_eos <- data_list$efficacy %>%
  filter(efficacy_status == 'eos') %>%
  filter(!is.na(extid)) %>%
  dplyr::pull(extid)

# Keep only individuals who are currently "in" or "out" of efficacy
individuals <- individuals %>%
  filter(!extid %in% ever_eos) %>%
  filter(!is.na(starting_efficacy_status)) %>%
  filter(starting_efficacy_status %in% c('in', 'out'))
# Write csvs
if(!dir.exists('efficacy_metadata')){
  dir.create('efficacy_metadata')
}
write_csv(individuals, 'efficacy_metadata/individual_data.csv')
