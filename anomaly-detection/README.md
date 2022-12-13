# DataBrew ODK Anomaly Detection

This repository is used for DataBrew ODK forms anomaly detection. Survey forms will be processed through data cleaning process and anomaly identification. This process will going through continuous iteration to increase survey data quality.

## Reproducing Environment

### Clone repository

``` r
git clone https://github.com/databrew/anomaly-detection.git
```

This project uses `renv` to restore all the libraries being used for data processing. It is recommended to use `renv` library to reproduce this analysis

### Install Renv

    install.packages("renv")

### Initiate Renv

    library(renv)
    renv::restore()

The above command will reproduce the analysis environment used for data stored in S3

## Contributing

To contribute, design your new feature in a new branch and create a PR to the main branch when ready

### Create new branch

``` r
git checkout -b [name_of_your_new_branch]
git push origin [name_of_your_new_branch]
```

### Data Cleaning Functions

All data cleaning functions are [here](R/data_cleaning_functions.R). Once function is created, append your function in [here](R/clean_survey_forms.R)

### Anomaly Detection Functions

All anomaly detection functions are [here](R/anomaly_detection_function.R). To add more anomaly identification procedure, create a new function that takes in registration/household data that returns a tibble dataframe with these 3 columns `type`, `anomaly_id`, `description`. Once function is created, append your function in [here]('R/run_anomaly_identification.R)

### Deploying your Changes

Github repository has been integrated with Github Actions, doing `git push` to this repo `main` branch will automatically sync with (DockerhHb)[<https://hub.docker.com/r/databrewllc/anomaly-detection>]

## References

-   [Anomaly detection Google Sheets](https://docs.google.com/spreadsheets/d/1kcTXqt2SREr9J4XQLFsLdXLfzcQPkQM8RHXI_-SUe_4/edit#gid=0)
