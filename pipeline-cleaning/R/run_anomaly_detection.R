# TBD FORM CLEANING FUNCTION
library(dplyr)
library(lubridate)
library(magrittr)
library(paws)
library(config)
library(cloudbrewr)
library(logger)
library(config)
library(tictoc)
library(data.table)
source('R/utils.R')


# start timer
tic()

# create log message
logger::log_info('Starting Anomaly detection')

# end timer
toc()
