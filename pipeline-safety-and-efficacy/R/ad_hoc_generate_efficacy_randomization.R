###########################
# EFFICACY
###########################
library(data.table)
library(dplyr)
library(glue)
library(ggplot2)
library(readr)
library(rgdal)
library(sp)
library(sf)

env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
bucket_source <- 'databrew.org'
bucket_lake_db <- 'bohemia-lake-db'
input_key <- list(
  v0 = 'kwale/clean-form/v0demography/v0demography.csv',
  v0_repeat = 'kwale/clean-form/v0demography/v0demography-repeat_individual.csv',
  sev0rab = 'kwale/clean-form/sev0rab/sev0rab.csv',
  sev0ra = 'kwale/clean-form/sev0ra/sev0ra.csv',
  goals = 'bohemia_prod/dim_kwale_cluster_goal/dim_kwale_cluster_goal.csv'
)
tryCatch({
  logger::log_info('Attempt AWS login')
  # login to AWS - this will be bypassed if executed in CI/CD environment
  cloudbrewr::aws_login(
    role_name = 'cloudbrewr-aws-role',
    profile_name =  'cloudbrewr-aws-role',
    pipeline_stage = env_pipeline_stage)

}, error = function(e){
  logger::log_error('AWS Login Failed')
  stop(e$message)
})

load('new_cores.RData')
load('new_clusters.RData')
cores <- new_cores
clusters <- new_clusters


v0demography <- cloudbrewr::aws_s3_get_table(
  bucket = bucket_source,
  key = input_key$v0)

v0demography_repeat_individual <- cloudbrewr::aws_s3_get_table(
  bucket = bucket_source,
  key = input_key$v0_repeat)

# Make a spatial version of households
v0demography_spatial <- v0demography %>% mutate(x = Longitude, y = Latitude)
coordinates(v0demography_spatial) <- ~x+y
proj4string(v0demography_spatial) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0")
p4s <- "+proj=utm +zone=37 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
crs <- CRS(p4s)
v0demography_spatial_projected <- spTransform(v0demography_spatial, crs)
cores_projected <- spTransform(cores, crs)
cores_projected_buffered <- rgeos::gBuffer(cores_projected, byid = TRUE, width = 50)
clusters_projected <- spTransform(clusters, crs)
clusters_projected_buffered <- rgeos::gBuffer(clusters_projected, byid = TRUE, width = 50)

o <- sp::over(v0demography_spatial_projected, polygons(cores_projected_buffered))
v0demography_spatial@data$in_core <- !is.na(o)
v0demography_spatial$core_cluster_number <- as.numeric(cores@data$cluster_nu[o])

o <- sp::over(v0demography_spatial_projected, polygons(clusters_projected_buffered))
v0demography_spatial@data$in_cluster <- !is.na(o)
v0demography_spatial$cluster_cluster_number <- as.numeric(clusters@data$cluster_nu[o])

people <- v0demography_repeat_individual %>%
  left_join(v0demography_spatial@data %>%
              dplyr::select(KEY,
                            lng = Longitude,
                            lat = Latitude,
                            cluster,
                            in_core,
                            in_cluster,
                            cluster_cluster_number,
                            core_cluster_number),
            by = c('PARENT_KEY' = 'KEY'))
date_of_enrollment <- lubridate::date('2023-10-01')
# See who is eligible or not
people <- people %>%
  mutate(age_at_enrollment  = date_of_enrollment - lubridate::date(dob)) %>%
  mutate(age_at_enrollment = age_at_enrollment / 365.25) %>%
  mutate(efficacy_eligible = age_at_enrollment >= 5.0 & age_at_enrollment < 15.0)
# Get a dataframe of only efficacy eligible children
eligibles <- people %>% filter(efficacy_eligible) %>%
  filter(in_cluster)
# Make spatial and project
eligibles_sp <- eligibles %>% mutate(x = lng, y = lat)
coordinates(eligibles_sp) <- ~x+y
proj4string(eligibles_sp) <- proj4string(clusters)
eligibles_sp_projected <- spTransform(eligibles_sp, crs)

# Assign each child a priority number based on location within cluster
st_eligibles <- sf::st_as_sf(eligibles_sp_projected)
st_cores_projected <- sf::st_as_sf(cores_projected)
st_clusters_projected <- sf::st_as_sf(clusters_projected)


dist_for_edge <- st_geometry(obj = st_clusters_projected) %>%
  st_cast(to = 'MULTILINESTRING') %>%
  st_distance(y=st_eligibles)
distances <- apply(dist_for_edge, 2, min)
st_eligibles$distance_to_edge <- distances
eligibles_sp_projected@data$distance_to_edge <- distances
plot(clusters_projected)
points(eligibles_sp_projected, pch = '.')

ggplot(data = eligibles_sp_projected@data,
       aes(x = lng,
           y = lat,
           color = distance_to_edge)) +
  geom_point(size = 0.6, alpha = 0.6) +
  theme_bw() +
  scale_color_gradient(name = 'Distance\nto edge', low = 'yellow',
                       high = 'black')

# Assign number to each child
priority_numbers <- eligibles_sp_projected@data %>%
  # filter(!is.na(expanded_core_number)) %>%
  arrange(desc(distance_to_edge)) %>%
  mutate(dummy = 1) %>%
  group_by(cluster_cluster_number) %>%
  mutate(cs = cumsum(dummy)) %>%
  ungroup %>%
  dplyr::select(KEY, priority_number = cs)
eligibles_sp_projected@data <-
  left_join(eligibles_sp_projected@data,
            priority_numbers)

# Plot relationship between priority number and distance to contamination
ggplot(data = eligibles_sp_projected@data,
       aes(x = lng,
           y = lat, color = priority_number)) +
  geom_point(alpha = 0.7, size = 0.5)

# Plot relationship between priority number and distance to contamination
ggplot(data = eligibles_sp_projected@data,
       aes(x = priority_number,
           y = distance_to_edge)) +
  geom_point(alpha = 0.2, size = 2) +
  labs(x = 'Priority number',
       y = 'Distance to edge of cluster')

# Create efficacy cohort
file_path <- 'outputs/efficacy_selection.csv'
if(file.exists(file_path)){
  efficacy_selection <- read_csv(file_path)
} else {
  efficacy_selection <- eligibles_sp_projected@data %>%
    filter(priority_number <= 35) %>%
    dplyr::select(cluster = cluster_cluster_number, extid, priority_number) %>%
    arrange(cluster, priority_number)
  write_csv(efficacy_selection, file_path)
}
