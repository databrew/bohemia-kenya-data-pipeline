# Description:
# - This script is used for creating credentials to instantiate bohemiase
# - It creates bohemia credentials and save it into dot file stored in $HOME directory
#
# Author: Aryton Tediarjo (atediarjo@gmail.com)
library(yaml)
library(paws)
library(magrittr)
library(config)
library(aws.s3)
library(ruODK)

conf <- config::get()
message("Log Message: Creating Bohemiase credentials into yaml file")

# get ODK credentials from secrets manager
svc  <- paws::secretsmanager()
creds <- svc$get_secret_value(Sys.getenv('ODK_CREDENTIALS_SECRETS_NAME')) %>%
  .$SecretString %>%
  jsonlite::parse_json(.)

# write to yaml file as . systems file
out <- list(
  url= conf$server,
  un= creds$username,
  pw= creds$password
)

# write to yaml
yaml::write_yaml(out, "~/.bohemia_credentials")
message("Log Message: credentials created")
