ento_reports:
	Rscript R/00.extract_data.R
	Rscript R/01.knit_reports.R
	Rscript R/02.store_reports.R

cleanup:
	Rscript R/03.folder_cleanups.R

pipeline: ento_reports cleanup
