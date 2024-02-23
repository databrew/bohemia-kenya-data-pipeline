prep:
	rm -rf key
	rm -rf input
	rm -rf output
	mkdir key
	mkdir input
	mkdir output

get_key:
	aws secretsmanager get-secret-value --secret-id ${SECRET_ID} --query SecretString --output text > key/key.json

get_detected_anomalies:
	aws s3 cp s3://${BUCKET_NAME}/bohemia_prod/anomalies_detection/anomalies_detection.csv input/anomalies_detection.csv
	aws s3 cp s3://${BUCKET_NAME}/bohemia_prod/dwh/ento_labs_individual_mosquitoes_cdc/ento_labs_individual_mosquitoes_cdc.csv input/ento_labs_individual_mosquitoes_cdc.csv
	aws s3 cp s3://${BUCKET_NAME}/bohemia_prod/dwh/ento_labs_individual_mosquitoes_cdc/ento_labs_pooled_mosquitoes_cdc.csv input/ento_labs_pooled_mosquitoes_cdc.csv
	aws s3 cp s3://${BUCKET_NAME}/bohemia_prod/dwh/ento_labs_individual_mosquitoes_cdc/ento_labs_individual_mosquitoes_rc.csv input/ento_labs_individual_mosquitoes_rc.csv
	aws s3 cp s3://${BUCKET_NAME}/bohemia_prod/dwh/ento_labs_individual_mosquitoes_cdc/ento_labs_pooled_mosquitoes_rc.csv input/ento_labs_pooled_mosquitoes_rc.csv


get_curr_resolution:
	. gsheets_src/venv/bin/activate
	python3 gsheets_src/anomalies_update.py
	aws s3 sync output s3://${BUCKET_NAME}/bohemia_prod/

get_ever_resolved:
	. aws_src/venv/bin/activate
	python3 aws_src/get_ever_resolved.py

update_google_sheets:
	. gsheets_src/venv/bin/activate
	python3 gsheets_src/anomalies_populate.py
	python3 gsheets_src/ento_populate.py

	
pipeline: prep get_key get_detected_anomalies get_curr_resolution get_ever_resolved update_google_sheets