# export your access key id and secret access key 
# (session token is only used for SSO access)
# this will test the microservice in a sandbox account
docker_tests:
	docker run \
	-e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
	-e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
	-e AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN} \
	-e AWS_REGION='us-east-1' \
	-e ODK_CREDENTIALS_SECRETS_NAME='test/odk-credentials' \
	-e BUCKET_PREFIX='databrew-test-' \
	aryton/extract-bohemia-kenya-project
	
# run data pipeline:
# 1). do git pull to fetch updates
# 2). run script 
pipeline: update scripts

# do git pull
update:
	git pull

# run scripts
scripts:
	Rscript R/create_credentials.R
	Rscript R/get_data_from_central.R

