scripts:
	Rscript R/00.generate_empty_objects.R
	Rscript generate_metadata.R
	Rscript R/02.store_metadata_to_aws.R
	Rscript R/03.store_metadata_to_odk.R

pipeline: scripts
