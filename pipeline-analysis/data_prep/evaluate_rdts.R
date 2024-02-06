library(lubridate)
library(cloudbrewr)
library(tidyverse)

# log into AWS and load data
ENV_PIPELINE_STAGE <- Sys.getenv("PIPELINE_STAGE")
DATA_STAGING_BUCKET_NAME <- 'databrew.org'
DATA_LAKE_BUCKET_NAME <- 'bohemia-lake-db'
PROJECT_SOURCE <- 'kwale'
SE_FOLDER_TARGET <- glue::glue('{PROJECT_SOURCE}/clean-form')

pad_hhid <- function(data){
  if('hhid' %in% names(data)){
    data %>% dplyr::mutate(hhid = stringr::str_pad(hhid, 5, pad = "0"))
  } else{
    data
  }
}

tryCatch({
  logger::log_info('Attempt AWS login')
  # login to AWS - this will be bypassed if executed in CI/CD environment
  cloudbrewr::aws_login(
    role_name = 'cloudbrewr-aws-role',
    profile_name =  'cloudbrewr-aws-role',
    pipeline_stage = ENV_PIPELINE_STAGE )
  
}, error = function(e){
  logger::log_error('AWS Login Failed')
  stop(e$message)
})

# load the data
# to get this data go to S3 bucket in AWS, databrew.org, kwale, v0demography or efficacy
# v0repeat <- cloudbrewr::aws_s3_get_table(
#   bucket = DATA_STAGING_BUCKET_NAME,
#   key = glue::glue('{PROJECT_SOURCE}/sanitized-form/v0demography/v0demography-repeat_individual.csv')) %>%
#   pad_hhid()

# v0demography <- cloudbrewr::aws_s3_get_table(
#   bucket = DATA_STAGING_BUCKET_NAME,
#   key = glue::glue('{PROJECT_SOURCE}/sanitized-form/v0demography/v0demography.csv')) %>%
#   pad_hhid()

efficacy <- cloudbrewr::aws_s3_get_table(
  bucket = DATA_STAGING_BUCKET_NAME,
  key = glue::glue('{PROJECT_SOURCE}/clean-form/efficacy/efficacy.csv')) %>%
  # key = glue::glue('{PROJECT_SOURCE}/sanitized-form/efficacy/efficacy.csv')) %>%
  pad_hhid()

# assignment list received from Nika / Joe
assignment <- cloudbrewr::aws_s3_get_table(
  bucket = 'bohemia-lake-db',
  key = glue::glue('bohemia_ext_data/assignments/assignments.csv')) %>%
  # key = glue::glue('{PROJECT_SOURCE}/sanitized-form/efficacy/efficacy.csv')) %>%
  pad_hhid()

# get data for prevalence and incidence calculations :

test_results <- left_join(efficacy, assignment, by=c('cluster'='cluster_number')) %>%      # ass_tar_eff %>%
  filter( efficacy_status == 'in' ) %>%
  select(extid, cluster, assignment, visit, pan_result, pf_result, child_took_coartem, SubmissionDate, todays_date ) %>%
  # NOTE: there can be duplicated rows before final cleaning
  distinct() %>% 
  mutate( visit_number = as.numeric(substr(visit, 2, 2)), .after = visit ) %>%
  mutate( days_since_prev_visit = NA, .after = todays_date ) %>%
  mutate( days_at_risk = NA, .after = days_since_prev_visit ) %>%
  mutate( incident_case = NA, .after = child_took_coartem ) 
          # child_took_coartem = (child_took_coartem == "yes" ),
          # pan_not_pf = ( pan_result == "Positive" ) & ( pf_result == "Negative" ) ) 

extids <- unique( test_results$extid )

# results: a data.frame containing the RDT results for ONE child ; N.B. that this function assumes that 
# the results have been arranged by visit number. Thus
#   - the first row corresponds to the enrollment visit
#   - check whether there are other visits; if there are, assess RDT results according to the following flowchart
#     https://docs.google.com/presentation/d/13v5idnDXFv26u2SNhyGQmSpc1jN-5FKTBG6ofWfTauY/edit?pli=1#slide=id.g2ac1b6bfb49_0_47
# N.B. pan_result and pf_result each have 1 of 3 possible values: "Positive", "Negative", "" 
# in particular, neither result can have the value NA

evaluate_rdt_results <- function( results ){
  
  # enrollment visit
  # a positive test (either pf or pan) at enrollment visit is an incident case :
  results$incident_case[1] <- (results$pan_result[1] == "Positive") | (results$pf_result[1] == "Positive")
  
  n_visits <- nrow( results )
  
  # assess subsequent visits (if there are any) and calculate days since last visit :
  if( n_visits > 1 ){
    for( i in 2:n_visits ){
      results$days_since_prev_visit[i] <- as.numeric( difftime( results$todays_date[i], results$todays_date[i-1], units = 'day' ) )
      results$days_at_risk[i] <- results$days_since_prev_visit[i] - 14*as.numeric( results$child_took_coartem[i-1] == "yes" )
      if( results$pan_result[i] == "Positive" ){
        results$incident_case[i] <- TRUE
      } else{
        if( results$pf_result[i] == "Negative" ){
          results$incident_case[i] <- FALSE
        } else{
          if( results$pf_result[i] == "Positive" ){
            if( results$visit_number[i] != (results$visit_number[i-1] + 1) ){
              # in this case, child skipped previous visit
              results$incident_case[i] <- TRUE
            } else{
              if( !is.na(results$incident_case[i-1]) & !results$incident_case[i-1] ){
                # was known NOT to be an incident case at previous visit
                results$incident_case[i] <- TRUE
              } else{
                if( results$pf_result[i-1] != "Positive" ){
                  results$incident_case[i] <- TRUE
                } else{
                  if( results$child_took_coartem[i-1] == "yes" ){
                    results$incident_case[i] <- FALSE
                  } else{
                    results$incident_case[i] <- TRUE
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  
  return( results )
}

evaluated_test_results <- data.frame()
for( i in 1:length( extids )){
  message("evaluating extid ", i, " of ", length( extids ) )
  evaluated_test_results <- rbind( evaluated_test_results,
                                  evaluate_rdt_results( test_results %>% filter( extid == extids[i] ) %>% arrange( visit_number ) ) )
}


write_csv(evaluated_test_results, "data/incident_cases.csv" )
