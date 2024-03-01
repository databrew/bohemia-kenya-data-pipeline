pipeline:
	Rscript R/01.run_form_cleaning.R
	Rscript R/02.run_data_sanitation.R
	Rscript R/03.run_parquet_load_clean_form.R
	Rscript R/04.run_parquet_load_sanitized_form.R
	Rscript R/05.run_anomaly_detection.R
