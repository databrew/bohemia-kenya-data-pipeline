#!/bin/bash

# create cron job
sudo docker pull databrewllc/odk-form-extraction:production
# stop docker
sudo docker stop odk_form_docker
# remove docker
sudo docker rm odk_form_docker
# create detached interactive session
sudo docker run -itd --env-file ~/.databrew_kwale_key_vars --name odk_form_docker databrewllc/odk-form-extraction:production /bin/bash
#write out current crontab
crontab -l > odk_form_extraction_cron
#echo new cron into cron file
echo "* * * * * sudo docker exec -t odk_form_docker make pipeline >> ~/odk_run.log" > odk_form_extraction_cron
#install new cron file
crontab odk_form_extraction_cron
# remove text
rm odk_form_extraction_cron
