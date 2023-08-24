#########################################################
# Safety metadata generation script
#
# Owner: joe.brew@gmail.com, atediarjo@gmail.com
#
# Logic used:
#
# 1. Roster Building:
# - Safety Arrivals: Get starting roster from safetynew forms
# - Safety Departures: Retrieve from Safety where person left household, migrated, out-migrated
# - Safety Deaths: From Safety where person absent reason = 'Died'
# - Efficacy Departures: From Efficacy where person absent reason != 'Absent'
# - Efficacy Deaths : From Efficach where person absent reason = 'Died'
# - Remove all user that has migrated or died
#
# 2. Households Metadata:
# - From v0demography
# - Collect heads of each households, number of visits
# - For each household collect each user from the roster, number of visits etc
#
# 3. Individual Metadata:
# - Use initial roster
# - Collect information whether user is ever pregnant, ever eos
# - Check user starting heights, starting weights
# - Check user PK Status
# - Join each starting statuses into roster (LEFT JOIN)
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
}else{
  FOLDER_TARGET <- 'kwale'
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
  # add PK day once available
)

# External data input key
EXTERNAL_INPUT_KEY <- list(
  assignments = glue::glue('bohemia_ext/randomization/assignments.csv'),
  intervention_assignment = glue::glue('bohemia_ext/randomization/intervention_assignment.csv')
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

# Get safety arrivals
arrivals <- data_list$safetynew_repeat_individual %>%
  filter(!is.na(lastname), !is.na(dob)) %>%
  left_join(
    data_list$safetynew %>%
      dplyr::select(hhid, KEY, todays_date),
    by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid,
                todays_date,
                firstname,
                lastname,
                dob,
                sex,
                extid) %>%
  mutate(type = 'Arrival')  %>%
  mutate(todays_date = lubridate::as_datetime(todays_date)) %>%
  mutate(dob = lubridate::as_datetime(dob))

# Get safety departures
safety_departures <- data_list$safety_repeat_individual %>%
  filter(!is.na(lastname), !is.na(dob)) %>%
  filter(person_left_household == 1|
           person_migrated == 1 |
           person_out_migrated == 1) %>%
  left_join(data_list$safety %>%
              dplyr::select(hhid, KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid,
                todays_date,
                firstname,
                lastname,
                dob,
                sex,
                extid) %>%
  mutate(type = 'Departure')  %>%
  mutate(todays_date = lubridate::as_datetime(todays_date)) %>%
  mutate(dob = lubridate::as_datetime(dob))

# Get Safety Deaths
safety_deaths <- data_list$safety_repeat_individual %>%
  left_join(
    data_list$safety %>%
      dplyr::select(KEY, todays_date, hhid), by = c('PARENT_KEY' = 'KEY')) %>%
  filter(!is.na(lastname), !is.na(dob)) %>%
  mutate(firstname = as.character(firstname),
         lastname = as.character(lastname),
         sex = as.character(sex),
         extid = as.character(extid)) %>%
  mutate(todays_date = lubridate::as_datetime(todays_date)) %>%
  mutate(dob = lubridate::as_datetime(dob)) %>%
  filter(person_absent_reason %in% c('Died')) %>%
  dplyr::select(hhid, todays_date, firstname, lastname, dob, sex, extid) %>%
  mutate(type = 'Death')

# Get efficacy departures (but ignore migrations, per project instructions)
# https://bohemiakenya.slack.com/archives/C042KSRLYUA/p1690186129913529?thread_ts=1689946560.024259&cid=C042KSRLYUA
efficacy_departures <- data_list$efficacy %>%
  filter(!is.na(lastname), !is.na(dob)) %>%
  mutate(
    firstname = as.character(firstname),
    lastname = as.character(lastname),
    sex = as.character(sex),
    extid = as.character(extid)) %>%
  mutate(todays_date = lubridate::as_datetime(todays_date),
         dob = lubridate::as_datetime(dob)) %>%
  filter(person_absent_reason != 'Absent')


efficacy_deaths <- efficacy_departures %>%
  filter(person_absent_reason %in% c('Died')) %>%
  dplyr::select(hhid, todays_date, firstname, lastname, dob, sex, extid) %>%
  mutate(type = 'Death')

# Combine safety and efficacy departures
departures <- bind_rows(safety_departures,
                        safety_deaths,
                        efficacy_deaths)


# events <- bind_rows(arrivals, departures) %>% arrange(todays_date)
# Get the starting roster
starting_roster <- data_list$v0demography_repeat_individual %>%
  left_join(data_list$v0demography %>%
              dplyr::select(hhid, todays_date, KEY), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid, todays_date, firstname, lastname, dob, sex, extid) %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  mutate(remove = FALSE) %>%
  mutate(index = 1:nrow(.)) %>%
  mutate(todays_date = lubridate::as_datetime(todays_date),
         dob = lubridate::as_datetime(dob))

# Go through each departure and flag people as dead or migrated
starting_roster$dead <- starting_roster$migrated <- 0
if(nrow(departures) > 0){
  starting_roster$migrated[starting_roster$extid %in% departures$extid[departures$type == 'Departure']] <- 1
  starting_roster$dead[starting_roster$extid %in% departures$extid[departures$type == 'Died']] <- 1
}

# Remove those who are dead and migrated
starting_roster <- starting_roster %>%
  filter(migrated != 1,
         dead != 1)

# Go through each arrival and add information
roster <- bind_rows(
    starting_roster,
    arrivals %>% mutate(hhid = as.character(hhid))
  ) %>%
  arrange(desc(todays_date)) %>%
  # keep only the most recent case
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  mutate(roster_name = paste0(
    firstname, ' ',
    lastname, ' (',
    extid, ')')) %>%
  dplyr::select(hhid,
                extid,
                firstname,
                lastname,
                sex, dob,
                roster_name,
                dead, migrated) %>%
  # set dead and migrated to 0 if new arrivals
  mutate(dead = ifelse(is.na(dead), 0, dead),
         migrated = ifelse(is.na(migrated), 0, migrated))




##########################################################
# 2. Build Household Metadata
#########################################################

# Get household heads and geographic information
heads <- data_list$v0demography_repeat_individual %>%
  filter(hh_head_yn == 'yes') %>%
  left_join(data_list$v0demography %>%
              dplyr::select(hhid,
                            todays_date,
                            KEY,
                            village,
                            ward), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid, todays_date, firstname, lastname, dob, sex, extid, village, ward) %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(hhid, .keep_all = TRUE) %>%
  mutate(household_head = paste0(firstname, ' ', lastname)) %>%
  dplyr::select(hhid, household_head, village, ward)

# Get number of visits done
visits_done <- data_list$safety %>%
  group_by(hhid = as.character(hhid)) %>%
  summarise(visits_done = paste0(sort(unique(visit)), collapse = ', '))

# Build up the metadata, starting with the household IDs
households <- roster %>%
  group_by(hhid) %>%
  summarise(roster = paste0(roster_name[dead == 0 & migrated == 0], collapse = ', '),
            num_members = length(which(dead == 0 & migrated == 0))) %>%
  # TODO: Change this to geo_cluster_num
  left_join(data_list$v0demography %>%
              dplyr::distinct(hhid, .keep_all = TRUE) %>%
              dplyr::select(hhid, cluster)) %>%
  left_join(data_list$assignments %>%
              dplyr::select(cluster = cluster_number,
                            arm = assignment)) %>%
  # get interventions
  left_join(data_list$intervention_assignment) %>%
  # get household head and geographic info
  left_join(heads) %>%
  # get visits done
  left_join(visits_done)


##########################################################
# 3. Build Individual Metadata
#########################################################


# Households done, now get individuals
individuals <- roster %>%
  dplyr::mutate(fullname_dob = paste0(firstname, ' ', lastname, ' | ', dob)) %>%
  dplyr::rename(fullname_id = roster_name) %>%
  # get intervention, village, ward, cluster
  left_join(households %>% dplyr::select(hhid, intervention, village, ward, cluster))

# Get starting safety status
starter <-
  bind_rows(
    data_list$v0demography_repeat_individual %>%
      left_join(data_list$v0demography %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), safety_status = as.character(safety_status)) %>%
      dplyr::select(todays_date, extid, safety_status) %>% mutate(form = 'v0demography'),
    data_list$safety_repeat_individual %>%
      left_join(data_list$safety %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), safety_status = as.character(safety_status), pregnancy_status = as.character(pregnancy_status)) %>%
      dplyr::select(todays_date, extid, safety_status, pregnancy_status) %>% mutate(form = 'safety'),
    data_list$safetynew_repeat_individual %>%
      left_join(data_list$safetynew %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), safety_status = as.character(safety_status)) %>%
      dplyr::select(todays_date, extid, safety_status) %>% mutate(form = 'safetynew'),
    data_list$efficacy %>%
      mutate(todays_date = as.Date(todays_date), extid = as.character(extid), safety_status = as.character(safety_status)) %>%
      dplyr::select(todays_date, extid, safety_status) %>% mutate(form = 'efficacy')
  ) %>%
  filter(!is.na(safety_status)) %>%
  arrange(desc(todays_date)) %>%
  mutate(starting_pregnancy_status = ifelse(is.na(pregnancy_status), 'out', pregnancy_status)) %>%
  dplyr::select(-pregnancy_status)
right <- starter %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  filter(!is.na(extid), !is.na(safety_status)) %>%
  dplyr::select(extid, starting_safety_status = safety_status, starting_pregnancy_status)

# Any individual who is not in the above gets an "out" assignation
individuals <- left_join(individuals, right) %>%
  mutate(starting_safety_status = ifelse(is.na(starting_safety_status), 'out', starting_safety_status))


# Double check: any individual who was ever eos is always eos
ever_eos <- starter %>%
  filter(!is.na(safety_status)) %>%
  filter(safety_status == 'eos') %>%
  filter(!is.na(extid)) %>%
  # dplyr::distinct(extid) %>%
  pull(extid)
individuals$starting_safety_status[individuals$extid %in% ever_eos] <- 'eos'

# Double check: any individual who was ever pregnant is always safety eos and in pfu
ever_pregnant <-
  bind_rows(
    data_list$safety_repeat_individual %>% filter(!is.na(pregnancy_status)) %>%
      filter(pregnancy_status == 'in') %>%
      dplyr::select(extid),
    data_list$pfu %>%
      dplyr::select(extid)) %>%
  pull(extid)
individuals$starting_safety_status[individuals$extid %in% ever_pregnant] <- 'eos'
individuals$starting_pregnancy_status[individuals$extid %in% ever_pregnant] <- 'in'

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
individuals <- left_join(individuals, starting_heights)


# Get pk status ################################################# (placeholder)
# individuals$pk_preselected <- sample(c(NA, 0, 1), nrow(individuals), replace = TRUE) ############# placeholder
# g <- gsheet::gsheet2tbl('https://docs.google.com/spreadsheets/d/1gff7p0MKejzllSEp7ONunpaSufvTWXxafktPK4xyCys/edit#gid=1430667203')
# g$extid[g$pk_preselected == 1]

pk_preselected_ids <- c("01000-01", "56123-01")
individuals$pk_preselected <- ifelse(individuals$extid %in% pk_preselected_ids, 1, 0)
pk_ids <- sort(unique(individuals$extid[individuals$pk_preselected == 1]))
right <- bind_rows(
  data_list$safety_repeat_individual %>%
    filter(!is.na(pk_status)) %>%
    left_join(data_list$safety %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
    dplyr::select(todays_date, extid, pk_status) %>%
    mutate(form = 'safety', todays_date = as.Date(todays_date))
  # data_list$pkday0 %>%
  #   filter(!is.na(pk_status)) %>%
  #   dplyr::select(todays_date, extid, pk_status) %>%
  #   mutate(form = 'pkday0',
  #          todays_date = as.Date(todays_date)),
  # data_list$pkdays123 %>%
  #   filter(!is.na(pk_status)) %>%
  #   dplyr::select(todays_date, extid, pk_status) %>% mutate(form = 'pkdays123', todays_date = as.Date(todays_date)),
  # data_list$pkfollowup %>%
  #   filter(!is.na(pk_status)) %>%
  #   dplyr::select(todays_date, extid, pk_status) %>% mutate(form = 'pkfollowup', todays_date = as.Date(todays_date)),
) %>%
  filter(!is.na(pk_status), !is.na(extid)) %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::select(extid, starting_pk_status = pk_status)
individuals <- left_join(individuals, right) %>%
  # if one is pk pre-selected by not in any of the "right" data, she is "out"; otherwise NA
  mutate(starting_pk_status = ifelse(is.na(starting_pk_status) & extid %in% pk_ids,
                                     'out',
                                     starting_pk_status))
# Write csvs
if(!dir.exists('safety_metadata')){
  dir.create('safety_metadata')
}
write_csv(households, 'safety_metadata/household_data.csv')
write_csv(individuals, 'safety_metadata/individual_data.csv')
