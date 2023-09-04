library(dplyr)
library(paws)
source("utils.R")

s3obj <- paws::s3()
instantiate_prod_dev_bucket_namespace()

file_path = tempfile(fileext = ".csv")
data <- dplyr::tribble(~user, ~team, ~project,
                       "joe", "databrew", "bohemia") %>%
  write.csv(file_path)

bucket_name = paste(Sys.getenv("BUCKET_PREFIX"), "databrew.org", sep = "-")
object_key = "kwale/new-feature/new-feature.csv"
save_to_s3_bucket(s3obj, file_path, bucket_name, object_key)
