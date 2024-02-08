clean:
	rm -rf _site
	rm -rf data

prepare:
	mkdir data
	Rscript data_prep/evaluate_rdts.R

render:
	quarto render

store:
	Rscript utils/store_site_to_s3.R

pipeline: clean prepare render store
