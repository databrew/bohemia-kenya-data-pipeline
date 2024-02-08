library(dplyr)
library(readr)
library(readxl)
library(sf)
options( scipen = 999 )

# load data ----

coordinates_of_hf <- read_excel("data/coordinates_of_hf.xlsx") %>%
  select( Name, LONG, LAT )
v0demography_points <- cloudbrewr::aws_s3_get_table(
  bucket = DATA_STAGING_BUCKET_NAME,
  key = glue::glue('{PROJECT_SOURCE}/clean-form/v0demography/v0demography.csv')) %>%
  pad_hhid() %>%
  select( hhid, LONG = Longitude, LAT = Latitude )

# convert to sf objects for easy analysis 

coordinates_of_hf <- st_as_sf( coordinates_of_hf, coords = c("LONG", "LAT"), remove = F, crs = st_crs(4326) )
v0demography_points <- st_as_sf( v0demography_points, coords = c("LONG","LAT"), remove = F, crs = st_crs(4326) )

distances <- st_distance( v0demography_points, coordinates_of_hf, by_element = FALSE)
v0demography_points$closest_hf <- coordinates_of_hf$Name[ apply( distances, 1, FUN = which.min ) ]
v0demography_points$meters_to_closest_hf <- apply( distances, 1, FUN = min )

summary( v0demography_points$meters_to_closest_hf )

write_csv( v0demography_points, "data/hh_to_hf.csv" )
