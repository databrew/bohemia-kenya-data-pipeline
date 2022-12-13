# run data pipeline:
# 1). do git pull to fetch updates
# 2). run script
pipeline: update scripts

# do git pull
update:
	git pull

# run scripts
scripts:
	Rscript R/run_data_cleaning.R
	Rscript R/run_anomaly_identification.R
	Rscript R/run_summarize_anomalies_by_date.R
