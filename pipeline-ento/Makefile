ento_reports:
	Rscript R/extract_data.R
	Rscript R/knit_reports.R
	Rscript R/store_reports.R

cleanup:
	Rscript R/folder_cleanups.R

pipeline: ento_reports cleanup
