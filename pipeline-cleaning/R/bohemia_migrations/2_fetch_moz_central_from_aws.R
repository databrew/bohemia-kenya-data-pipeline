# This code is used to fetch Moz data from Joe's AWS account manually (faster)
# into databrew account. Each RData is fetched and stored as csv (and zip files) into AWS

library(cloudbrewr)
library(dplyr)

# Cloudbrewr is not configured cross-account fetch temp key id from SSO page
Sys.setenv(
  AWS_ACCESS_KEY_ID = 'ACCESS_KEY_ID',
  AWS_SECRET_ACCESS_KEY = 'SECRET_ACCESS_KEY',
  AWS_SESSION_TOKEN = "SESSION_TOKEN",
  AWS_DEFAULT_REGION = 'eu-west-3'
)

# crossectional
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = 'mopeia.org/BOHEMIA/crosssectional/2023-09-11 09:04:53.RData',
  output_dir = 'R/bohemia_migrations/output/tmp/crossectional/raw'
)

clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = 'mopeia.org/BOHEMIA/crosssectional/clean/2023-09-11 01:44:19.RData',
  output_dir = 'R/bohemia_migrations/output/tmp/crossectional/clean'
)


# diary recovery attempt 1
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = 'mopeia.org/BOHEMIA/diary_recovery_attempt_1/2022-11-08 00:01:04.RData',
  output_dir = 'R/bohemia_migrations/output/tmp/diary_recovery_attempt_1/raw'
)

clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = 'mopeia.org/BOHEMIA/diary_recovery_attempt_1/2022-11-08 00:01:04.RData',
  output_dir = 'R/bohemia_migrations/output/tmp/diary_recovery_attempt_1/clean'
)


# entoa1a2
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = 'mopeia.org/BOHEMIA/entoa1a2/clean/2022-11-09 09:32:20.RData',
  output_dir = 'R/bohemia_migrations/output/tmp/entoa1a2/clean'
)


# entoa1a2
form_id = 'entof2f3f4f5'
filename = '2022-11-09 23:50:22.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/clean/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)

# healtheconmonthly
form_id = 'healtheconmonthly'
filename = '2023-09-11 09:04:53.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)

# healtheconmonthly
form_id = 'healtheconmonthly'
filename = '2023-09-11 01:44:17.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/clean/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)


# householdfinder
form_id = 'householdfinder'
filename = '2023-09-11 09:04:53.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)

# householdfinder
form_id = 'householdfinder'
filename = '2023-09-11 09:04:53.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)


########## Medical assessment capture ##############
form_id = 'medicalassessmentCapture'
filename = '2023-09-11 09:04:53.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)


form_id = 'medicalassessmentCapture'
filename = '2023-09-11 09:04:53.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)


########## Passive Pregnancy Case Detection ##############
form_id = 'passivepregnancycasedetection'
filename = '2022-04-21 22:59:04.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)


form_id = 'passivepregnancycasedetection'
filename = '2022-04-21 22:59:04.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)

########## pigsa ##############
form_id = 'pigsa'
filename = '2023-09-11 09:04:53.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)


form_id = 'pigsa'
filename = '2023-09-11 01:43:48.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/clean/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)


########## pigsb ##############
form_id = 'pigsb'
filename = '2023-09-11 09:04:53.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)


form_id = 'pigsb'
filename = '2023-09-11 01:43:48.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/clean/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)


########## pigsc ##############
form_id = 'pigsc'
filename = '2023-09-11 09:04:53.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)


form_id = 'pigsc'
filename = '2023-09-11 01:43:49.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/clean/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)


########## pkdatacollectiontool ##############
form_id = 'pkdatacollectiontool'
filename = '2023-09-11 09:04:53.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)


form_id = 'pkdatacollectiontool'
filename = '2023-09-11 01:44:14.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/clean/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)


########## pregnancyactivecasedetection ##############
form_id = 'pregnancyactivecasedetection'
filename = '2023-09-11 09:04:53.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)


form_id = 'pregnancyactivecasedetection'
filename = '2023-09-11 01:44:19.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/clean/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)


########## qr_capture ##############
form_id = 'qr_capture'
filename = '2023-09-11 09:04:53.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)


form_id = 'qr_capture'
filename = '2023-09-11 09:04:53.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)


########## requesition_capture ##############
form_id = 'requesition_capture'
filename = '2023-09-11 09:04:53.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)


form_id = 'requesition_capture'
filename = '2023-09-11 09:04:53.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)

########## safetyefficacylivestock ##############
form_id = 'safetyefficacylivestock'
filename = '2023-09-11 09:04:53.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)


form_id = 'safetyefficacylivestock'
filename = '2023-09-11 01:44:17.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/clean/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)


########## sefull ##############
form_id = 'sefull'
filename = '2023-09-11 09:04:53.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)


form_id = 'sefull'
filename = '2023-09-11 01:43:16.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/clean/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)


########## sefullv1 ##############
form_id = 'sefullv1'
filename = '2022-10-27 23:39:04.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)


form_id = 'sefullv1'
filename = '2022-10-27 23:39:04.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)


########## sefullv2 ##############
form_id = 'sefullv2'
filename = '2022-10-27 23:39:04.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)


form_id = 'sefullv2'
filename = '2022-10-27 23:39:04.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)


########## sefullv3 ##############
form_id = 'sefullv3'
filename = '2022-10-27 23:39:04.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)


form_id = 'sefullv3'
filename = '2022-10-27 23:39:04.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)

########## sefullv4 ##############
form_id = 'sefullv4'
filename = '2022-10-27 23:39:04.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)


form_id = 'sefullv4'
filename = '2022-10-27 23:39:04.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)

########## sefullv5 ##############
form_id = 'sefullv5'
filename = '2022-10-27 23:39:04.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)


form_id = 'sefullv5'
filename = '2022-10-27 23:39:04.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)


########## sentds ##############
form_id = 'sentds'
filename = '2023-09-11 09:04:53.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)


form_id = 'sentds'
filename = '2023-09-11 09:04:53.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)


########## serefusalabsences ##############
form_id = 'serefusalsabsences'
filename = '2023-09-11 09:04:53.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)


form_id = 'serefusalsabsences'
filename = '2023-09-11 01:43:58.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/clean/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)



########## sesae ##############
form_id = 'sesae'
filename = '2023-09-11 09:04:53.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)


form_id = 'sesae'
filename = '2023-09-11 09:04:53.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)


########## supervisionform_MDA ##############
form_id = 'supervisionform_MDA'
filename = '2023-09-11 09:04:53.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)


form_id = 'supervisionform_MDA'
filename = '2023-09-11 09:04:53.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)


########## vadiagnosisfinal ##############
form_id = 'vadiagnosisfinal'
filename = '2023-09-11 09:04:53.RData'
raw_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/raw')
)


form_id = 'vadiagnosisfinal'
filename = '2023-09-11 09:04:53.RData'
clean_obj <- cloudbrewr::aws_s3_get_object(
  bucket ='bohemia2022',
  key = glue::glue('mopeia.org/BOHEMIA/{form_id}/{filename}'),
  output_dir = glue::glue('R/bohemia_migrations/output/tmp/{form_id}/clean')
)

