# Interaction guidance with databrew.org S3 bucket

In `databrew.org` bucket - files are organized as projects. Each projects will contain different steps and staging of the data curation processes. Inside databrew.org will also contain anomalies folder where it will be used as a detection/amendment process for data QA

S3 Folder Hierarchy Structure for Projects:

1.  `project`: Name of the project from ODK Server

2.  `raw/clean-form`: Indicator whether form has been cleaned or not

3.  `form-id`: Form ID shown in ODK Server

4.  `form`: CSV Form and Repeats: Form Data from ODK Server

S3 Folder Hierarchy Structure for Anomalies:

1.  `gsheets-fix`: This is where you upload [google sheets manual fix](https://docs.google.com/spreadsheets/d/1i98uVuSj3qETbrH7beC8BkFmKV80rcImGobBvUGuqbU/edit#gid=0)

2.  `detection-history`: This is where anomalies detection data is dump, it will take historical data to show how much anomalies have been reduced by day

3.  `detection-current`: Current state of anomalies

4.  `adhoc-fix`: Any manual amendment data that is used to clean the data that can't be fixed via manual gsheets fix

## Pipeline Overview

![Image](../images/s3_bucket_flow.png)
