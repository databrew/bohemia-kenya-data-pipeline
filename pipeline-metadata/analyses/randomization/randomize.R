# Libraries
library(dplyr)
library(readr)
library(leaflet)
library(rgdal)
library(sp)

# Based on plot, define each cluster as north or south
clusters <- tibble(
  cluster_number = 1:96,
  location = c(rep('North', 48),
               'South', 'South', 'South', 'North', 'North', 'South',
               'North', 'North',
               rep('South', 40)))



# Load in spatial files

## DEPRECATED AS OF AUG 1 2023
# load('../recon_clustering/final/cores.RData')
# load('../recon_clustering/final/buffers.RData')

load('../../data_public/spatial/clusters.RData')
old_clusters <- clusters
load('../../data_public/spatial/new_cores.RData')
load('../../data_public/spatial/new_clusters.RData')
cores <- new_cores
clusters <- new_clusters
# buffers <- rgeos::gDifference(clusters, cores)


# Plot to see what is above/below the highway
if(FALSE){
  leaflet() %>%
    addTiles() %>%
    addPolygons(data = cores,
                label = cores@data$cluster_number,
                labelOptions = list('permanent' = TRUE,
                                    'autclose' = FALSE))  
  # Inspect
  cores@data <- left_join(cores@data, clusters)
  leaflet() %>%
    addTiles() %>%
    addPolygons(data = cores[cores@data$location == 'North',],
                label = cores@data$cluster_number[cores@data$location == 'North'],
                # labelOptions = list('permanent' = TRUE,
                #                     'autclose' = FALSE),
                fillColor = 'blue', color = 'blue'
    ) %>%
    addPolygons(data = cores[cores@data$location == 'South',],
                label = cores@data$cluster_number[cores@data$location == 'South'],
                # labelOptions = list('permanent' = TRUE,
                #                     'autclose' = FALSE),
                fillColor = 'red', color = 'red'
    )
}

# North / South  randomization
if('assignments.csv' %in% dir('outputs')){
  assignments <- read_csv('outputs/assignments.csv')
} else {
  # Set a randomization seed
  set.seed(123)
  # Define a list of As and Bs
  assignment_options_north <- c(rep(1, 26), rep(2, 26))
  assignment_options_south <- c(rep(1, 22), rep(2, 22))
  assignment_options_north <- sample(assignment_options_north, size = length(assignment_options_north), replace = FALSE)
  assignment_options_south <- sample(assignment_options_south, size = length(assignment_options_south), replace = FALSE)
  assignments <- tibble(cluster_number = 1:96) %>%
    left_join(clusters) %>%
    arrange(location) %>%
    mutate(assignment = c(assignment_options_north, assignment_options_south))
  # Manual changes per project direction August 3, 2023
  # https://bohemiakenya.slack.com/archives/C03DXF6SPC2/p1690899640727899
  # ento needs to be re-assigned in paired ways
  ento_pairs <- tibble(
    cluster = c(2, 7, 12, 25, 34, 40, 82, 69, 74, 75, 79, 87),
    grp =     c(2, 7, 2,  25, 25, 7,  74, 69, 74, 69, 79, 79)
  )
  ento_pairs <- ento_pairs %>% dplyr::sample_n(nrow(.)) %>% mutate(dummy = 1) %>% group_by(grp) %>% mutate(new_assignment = cumsum(dummy)) %>% ungroup %>% arrange(grp, new_assignment) %>% dplyr::select(cluster, new_assignment)
  assignments <- left_join(assignments, ento_pairs %>% dplyr::rename(cluster_number = cluster)) %>%
    mutate(assignment = ifelse(is.na(new_assignment), assignment, new_assignment))
  assignments$new_assignment <- NULL
  assignments <- assignments[assignments$cluster_number %in% new_clusters$cluster_nu,]
  # Write a csv
  write_csv(assignments, 'outputs/assignments.csv')
  file.copy('outputs/assignments.csv', '../../data_public/randomization/assignments.csv')
}

# See results
if(FALSE){
  
  # Inspect
  cores@data <- left_join(cores@data %>%
                            mutate(cluster_number = as.numeric(cluster_nu)), 
                          assignments)
  l <- leaflet() %>%
    addTiles() %>%
    addPolygons(data = clusters, fillColor = 'grey', color = 'grey', fillOpacity = 0.5, weight = 0,
                label = clusters@data$cluster_nu,
                labelOptions = list('permanent' = TRUE,
                                    'autclose' = FALSE)) %>%
    addPolygons(data = cores[cores@data$assignment == 1,],
                label = paste0('Cluster ', cores@data$cluster_number[cores@data$assignment == 1], '. Assignment = 1'),
                weight = 1,
                # labelOptions = list('permanent' = TRUE,
                #                     'autclose' = FALSE),
                fillColor = 'blue', color = 'blue'
    ) %>%
    addPolygons(data = cores[cores@data$assignment == 2,],
                label = paste0('Cluster ', cores@data$cluster_number[cores@data$assignment == 1], '. Assignment = 2'),
                weight = 1,
                # labelOptions = list('permanent' = TRUE,
                #                     'autclose' = FALSE),
                fillColor = 'red', color = 'red'
    )
  htmltools::save_html(l, file = '~/Desktop/kwaleclustermap.html')
  
  # See some social science stuff
  # hh <- readOGR('../../data_public/spatial/households/', 'households')
  # hhx <- hh[hh@data$village == 'Kilulu',]
  # l %>% addCircleMarkers(data = hhx, radius = 1, col = 'green') %>%
  #   addCircleMarkers(data = inclusion, radius = 1, col = 'orange')
  # load('../../analyses/recon_clustering/inclusion.RData')
}



# Spatial file for Almu
if(FALSE){
  # load('../recon_clustering/final/clusters.RData')
  almu <- clusters 
  almu@data <- left_join(almu@data  %>%
                           mutate(cluster_number = as.numeric(cluster_nu)), 
                         assignments)
  raster::shapefile(x = almu, file = "clusters_with_assignment.shp", overwrite = TRUE)
}

###########################
# ENTOMOLOGY
###########################

clusters$cluster_number <- as.numeric(clusters@data$cluster_nu)
cores$cluster_number <- as.numeric(cores@data$cluster_nu)
# https://docs.google.com/document/d/1MNiDi-Jln-CrNrFMgzgReJqSGkIJ1FCSaagQbxcmjOk/edit#heading=h.vo9rz17me9hc

# August 4 re-do, instead of using recon data, use v0demography data
# Define production
is_production <- TRUE
Sys.setenv(PIPELINE_STAGE = ifelse(is_production, 'production', 'develop')) # change to production
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")
start_fresh <- TRUE

if(start_fresh){
  # Log in
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
  
  
  # Define datasets for which I'm retrieving data
  datasets <- c('v0demography', 'sev0rab', 'sev0ra')
  datasets_names <- datasets
  # Loop through each dataset and retrieve
  # bucket <- 'databrew.org'
  # folder <- 'kwale'
  bucket <- 'databrew.org'
  if(is_production){
    folder <- 'kwale'
  } else {
    folder <- 'kwale_testing'
  }
  
  for(i in 1:length(datasets)){
    this_dataset <- datasets[i]
    object_keys <- glue::glue('/{folder}/clean-form/{this_dataset}',
                              folder = folder,
                              this_dataset = this_dataset)
    output_dir <- glue::glue('{folder}/clean-form/{this_dataset}',
                             folder = folder,
                             this_dataset = this_dataset)
    dir.create(object_keys, recursive = TRUE, showWarnings = FALSE)
    print(object_keys)
    cloudbrewr::aws_s3_bulk_get(
      bucket = bucket,
      prefix = as.character(object_keys),
      output_dir = output_dir
    )
  }
  
  v0demography <- read_csv('kwale/clean-form/v0demography/v0demography.csv')
  v0demography_repeat_individual <- read_csv('kwale/clean-form/v0demography/v0demography-repeat_individual.csv')
  ra <- read_csv('kwale/clean-form/sev0ra/sev0ra.csv') %>% dplyr::select(todays_date, Longitude, Latitude, cluster, refusal_or_absence, form_version = FormVersion, hhid, recon_hhid_map_manual, recon_hhid_painted_manual) %>% mutate(version = 'a')
  rab <- read_csv('kwale/clean-form/sev0rab/sev0rab.csv') %>% dplyr::select(todays_date, Longitude, Latitude, cluster, refusal_or_absence, form_version = FormVersion, hhid, recon_hhid_map_manual, recon_hhid_painted_manual) %>% mutate(version = 'b')
  rx <- bind_rows(ra, rab) %>%
    mutate(id = ifelse(is.na(hhid), recon_hhid_map_manual, hhid)) %>%
    mutate(id = ifelse(is.na(id), recon_hhid_painted_manual, id)) %>%
    mutate(recon_id = ifelse(!is.na(recon_hhid_map_manual), recon_hhid_map_manual, recon_hhid_painted_manual))
  save(ra, rab, rx, v0demography, v0demography_repeat_individual, file = 'data.RData')
} else {
  load('data.RData')
}

# Read in inputs from Almudena
health_economics_clusters <- read_delim('inputs/HEcon_clusters.csv', delim = ';') %>% filter(!is.na(cluster))
health_economics_households <- read_delim('inputs/HEcon_hhs.csv', delim = ';')
# # sanity check on locations of households
# sanity <- health_economics_households %>%
#   left_join(v0demography %>% dplyr::select(geo_cluster_num, hhid)) %>%
#   filter(!is.na(geo_cluster_num)) %>%
#   group_by(cluster = geo_cluster_num) %>% tally %>%
#   left_join(health_economics_clusters)
# sanity$cluster %in% health_economics_clusters$cluster


# Save to outputs
file_name <- 'outputs/health_economics_clusters.csv'
if(!file.exists(file_name)){
  write_csv(health_economics_clusters, file_name)
}
file_name <- 'outputs/health_economics_households.csv'
if(!file.exists(file_name)){
  write_csv(health_economics_households, file_name)
}


if(FALSE){
  # DEPRECATED, USING RECON DATA
  # Start by reading in the "curated" recon data
  # https://s3.console.aws.amazon.com/s3/object/databrew.org?region=us-east-1&prefix=kwale/recon/clean-form/reconbhousehold/reconbhousehold.csv
  recon <- read_csv('inputs/reconbhousehold.csv')
  # https://s3.console.aws.amazon.com/s3/object/databrew.org?region=us-east-1&prefix=kwale/recon/clean-form/reconbhousehold/curated_recon_household_data.csv
  recon_curated <- read_csv('inputs/curated_recon_household_data.csv')
  

  
  # Process households for preparation for household-specific deliverables
  # Get sub-counties for use in final output
  # load('../recon_clustering/final/clusters.RData')
  load('../../data_public/spatial/clusters.RData')
  
  # Get sub-counties
  sub_counties <- recon %>%
    group_by(ward, community_health_unit, village, sub_county) %>%
    tally %>%
    ungroup %>%
    arrange(desc(n)) %>%
    dplyr::distinct(ward, .keep_all = TRUE) %>%
    dplyr::select(ward, sub_county) %>%
    filter(!is.na(ward))
  
  # Read in and organize recon data
  hhsp <- readOGR('../../data_public/spatial/households/', 'households')
  hhsp@data <- left_join(hhsp@data %>% dplyr::rename(hh_id_clean = hh_id) %>% dplyr::select(-village, ward),
                         recon_curated %>% dplyr::select(hh_id_raw, hh_id_clean,
                                                         community_health_unit,
                                                         village,
                                                         ward))
  
  # Make community health unit map
  library(dismo)
  library(rgeos)
  
  data <- hhsp@data %>% mutate(lng = Longitude, lat = Latitude) %>% mutate(x = lng, y = lat)
  cluster_points <- data %>% dplyr::select(x, y, community_health_unit)
  coordinates(cluster_points) <- ~x+y
  proj4string(cluster_points) <- proj4string(clusters)
  row.names(cluster_points) <- 1:nrow(cluster_points)
  cluster_points$median_distance <- NA
  # Remove outliers (will take a a couple of minutes)
  for(i in 1:nrow(cluster_points)){
    message(i, ' of ', nrow(cluster_points))
    this_point <- cluster_points[i,]
    other_points_in_chu <- cluster_points[cluster_points$community_health_unit == this_point$community_health_unit,]
    distances <- rgeos::gDistance(this_point, other_points_in_chu, byid = TRUE)
    median_distance <- median(distances)
    cluster_points$median_distance[i] <- median_distance
  }
  # Remove those with large median distance
  cluster_points <- cluster_points[cluster_points$median_distance <= 0.04,]
  
  # voronoi tesselation
  v <- dismo::voronoi(cluster_points)
  # inspect
  plot(v)
  # collapse
  library(rgeos)
  x = gUnaryUnion(v, id = v$community_health_unit, checkValidity = 2)
  # match row names
  ward_table <- cluster_points@data %>% group_by(community_health_unit) %>% tally
  ward_table <- data.frame(ward_table)
  row.names(ward_table) <- as.character(ward_table$community_health_unit)
  # make polygons dataframe
  dfv <- SpatialPolygonsDataFrame(Sr = x, data = ward_table, match.ID = TRUE)
  # inspect data
  dfv@data
  # reproject
  proj4string(dfv) <- proj4string(clusters)
  # clip
  load('../../data_public/spatial/pongwe_kikoneni_ramisi.RData')
  pk <- pongwe_kikoneni_ramisi
  r <- gIntersection(pk, dfv, byid = TRUE)
  ids <- c()
  for(i in 1:30){
    ids[i] <- r@polygons[[i]]@ID
  }
  ids <- substr(ids, 8, nchar(ids))
  ids_table <- tibble(community_health_unit = ids)
  ids_table <- data.frame(ids_table)
  ids_table <- ids_table %>% mutate(dummy = 1) %>% group_by(community_health_unit) %>%
    mutate(cs = cumsum(dummy)) %>%
    ungroup %>% mutate(community_health_unit = paste0(community_health_unit, ' ', cs))
  row.names(ids_table) <- ids_table$community_health_unit
  for(i in 1:30){
    r@polygons[[i]]@ID <- ids_table$community_health_unit[i]
  }
  r <- SpatialPolygonsDataFrame(Sr = r, data = ids_table, match.ID = TRUE)
  # Fortify
  library(ggplot2)
  library(ggrepel)
  rf <- fortify(r, regions = r@data$community_health_unit)
  coords <- coordinates(r)
  coords <- data.frame(coords)
  names(coords) <- c('x', 'y')
  coords$community_health_unit <- r@data$community_health_unit
  # plot
  ggplot() +
    geom_polygon(data = rf,
                 aes(x = long,
                     y = lat,
                     group = group,
                     fill = id),
                 color = 'black') +
    ggthemes::theme_map() +
    geom_label_repel(data = coords,
                     aes(x = x,
                         y = y,
                         label = community_health_unit)) +
    scale_fill_manual(name = '', values = rainbow(length(unique(rf$id)))) +
    theme(legend.position = 'none')
  
  if(!dir.exists('../../data_public/spatial/community_health_units')){
    dir.create('../../data_public/spatial/community_health_units')
  }
  owd <- getwd()
  setwd('../../data_public/spatial/community_health_units') 
  community_health_units <- r
  raster::shapefile(community_health_units, 'community_health_units.shp')
  save(community_health_units, file = 'community_health_units.RData')
  setwd(owd)
}

hhsp <- v0demography
hhsp$x <- hhsp$Longitude
hhsp$y <- hhsp$Latitude
coordinates(hhsp) <- ~x+y
proj4string(hhsp) <- proj4string(clusters)

# See if in cluster core / buffer
o <- sp::over(hhsp, polygons(clusters))
hhsp@data$cluster <- clusters@data$cluster_number[o]
# o <- sp::over(hhsp, polygons(buffers))
# hhsp@data$buffer <- buffers@data$cluster_number[o]
o <- sp::over(hhsp, polygons(cores))
hhsp@data$core <- cores@data$cluster_number[o]
# Keep only those which are in clusters
hhsp <- hhsp[!is.na(hhsp@data$cluster),]
# Define buffer / core status
hhsp@data$core_buffer <- ifelse(!is.na(hhsp@data$core), 'Core',
                                ifelse(!is.na(hhsp@data$cluster), 'Buffer', NA))
hhsp@data$core_buffer <- factor(hhsp@data$core_buffer, levels = c('Core', 'Buffer'))
hhsp <- hhsp[!is.na(hhsp@data$core_buffer),]

###########################
# ENTOMOLOGY (CONT)
###########################

# Deliverable 1 ################    
# select 6 clusters per arm, a total of 12 clusters. ALL the hh from these clusters will be metadata for the three Entomology data collection tools. Select only from clusters that have more than 15 hh in the core.
# Deliverable 1: a table named “Table 1_ento_clusters.csv” in which one row is an Ento cluster with the column: 
# Cluster#
# Arm (just the code (1 or 2), not the intervention)
set.seed(18)
if('table_1_ento_clusters.csv' %in% dir('outputs/')){
  ento_clusters <- read_csv('outputs/table_1_ento_clusters.csv')
  if(FALSE){
    # Manual replacement of cluster 47 as per project request, May 31 2023
    # Options are 
    #  2  3  4  9 10 12 15 18 20 24 25 27 28 30 31 34 35 38 41 47 48 52 55
    # sample(c(2, 3, 4, 9, 10, 12, 15, 18, 20, 24, 25, 27, 28, 30, 31, 34, 
    #          35, 38, 41, 47, 48, 52, 55), 1)
    # 52
    # ento_clusters$cluster_number[ento_clusters$cluster_number == 47] <- 52
    # Manual replacement of cluster 52 with 82, as per project request Aug 3 2023
    ento_clusters$cluster_number[ento_clusters$cluster_number == 52] <- 82
    ento_clusters <- ento_clusters %>% arrange(cluster_number)
    # # fix assignment
    # ento_clusters <- ento_clusters %>% dplyr::select(cluster_number)
    # ento_clusters <- left_join(ento_clusters, assignments)
    write_csv(ento_clusters, 'outputs/table_1_ento_clusters.csv')
  }
} else {
  # Get the number of households per core
  hh_per_core <- hhsp@data %>%
    filter(core_buffer == 'Core') %>%
    group_by(cluster) %>%
    tally %>%
    ungroup %>%
    filter(n >= 15)
  # Select 6 clusters per arm
  ento_clusters <- assignments %>%
    # keep only those households which have at least 15 households in the core
    filter(cluster_number %in% hh_per_core$cluster) %>%
    group_by(assignment) %>%
    dplyr::sample_n(6) %>%
    ungroup
  write_csv(ento_clusters, 'outputs/table_1_ento_clusters.csv')
}

# Examine a bit
if(FALSE){
  ento_sp <- clusters
  ento_sp <- ento_sp[ento_sp@data$cluster_number %in% ento_clusters$cluster_number,]
  cores@data <- left_join(cores@data, assignments)
  
  l <- leaflet() %>%
    addTiles() %>%
    addPolygons(data = clusters, fillColor = 'grey', color = 'grey', fillOpacity = 0.2, weight = 1,
                label = clusters@data$cluster_number) %>%
    addPolygons(data = cores[cores@data$assignment == 1,],
                label = cores@data$cluster_number[cores@data$assignment == 1],
                weight = 0,
                # labelOptions = list('permanent' = TRUE,
                #                     'autclose' = FALSE),
                fillOpacity = 0.2,
                fillColor = 'blue', color = 'blue'
    ) %>%
    addPolygons(data = cores[cores@data$assignment == 2,],
                label = cores@data$cluster_number[cores@data$assignment == 2],
                weight = 0,
                fillOpacity = 0.2,
                # labelOptions = list('permanent' = TRUE,
                #                     'autclose' = FALSE),
                fillColor = 'red', color = 'red') %>%
    addPolygons(data = ento_sp,
                label = ento_sp@data$cluster_number,
                weight = 1,
                labelOptions = list('permanent' = TRUE,
                                    'autclose' = FALSE),
                fillOpacity = 0.6,
                fillColor = 'yellow', color = 'purple'
    )
  l
  
  # Write a shapefile
  owd <- getwd()
  dir.create('../../data_public/spatial/ento_clusters')
  setwd('../../data_public/spatial/ento_clusters')
  raster::shapefile(x = ento_sp, file = "ento_clusters.shp", overwrite = TRUE)
  # writeOGR(cores, dsn = '.', layer = 'clusters', driver = "ESRI Shapefile")
  setwd(owd)
  
  # htmltools::save_html(html = l, file = '~/Desktop/kenyaentoclusters.html')
  htmlwidgets::saveWidget(widget = l,
                          file = '~/Desktop/kenyaentoclusters.html',
                          selfcontained = TRUE)
  plot(cores)
  plot(ento_sp, add = T, col = 'red')
  plot(clusters, add = T)
  plot(cores, add = T)
  library(sp)
  raster::text(x = (ento_sp), ento_sp@data$cluster_number, cex = 0.5)
}


# SPATIAL OBJECTS FOR LOCUS GIS #######################
if(!dir.exists('outputs/general_spatial')){
  dir.create('outputs/general_spatial')
}
if(!file.exists('outputs/general_spatial/clusters.shp')){
  message('Writing shapefile')
  raster::shapefile(clusters, 'outputs/general_spatial/clusters.shp')
} else {
  message('Shapefile already written')
}
if(!file.exists('outputs/general_spatial/households.shp')){
  message('Writing shapefile')
  all_study_households <- hhsp
  all_study_households@data <- all_study_households@data %>%
    mutate(hh_id_clean = hhid,
           hh_id_raw = hhid) %>%
    dplyr::select(hh_id_clean, hh_id_raw,
                  cluster, Longitude, Latitude, core_buffer)
  raster::shapefile(all_study_households, 'outputs/general_spatial/households.shp', overwrite = TRUE)
} else {
  message('Shapefile already written')
}

# Assign cluster number
hhsp@data$cluster_number <- hhsp@data$cluster

# Get list of cleaned IDs for Xing
xing <- hhsp@data %>% 
  mutate(hh_id_clean = hhid,
         hh_id_raw = hhid) %>%
  dplyr::select(cluster_number, hh_id_clean,
                                    hh_id_raw, core_buffer) %>%
  arrange(cluster_number, hh_id_clean)
# write csv
write_csv(xing, '/tmp/recon_clean_ids.csv')

# Filter down to only those in ento clusters
hhsp <- hhsp[hhsp@data$cluster_number %in% ento_clusters$cluster_number,]
xing <- hhsp@data %>% 
  mutate(hh_id_clean = hhid,
         hh_id_raw = hhid) %>%
  dplyr::select(cluster_number, hh_id_clean,
                                    hh_id_raw, core_buffer) %>%
  arrange(cluster_number, hh_id_clean)

write_csv(xing, '/tmp/ento_recon_clean_ids.csv')


# Deliverable 2 ################    
#  	Deliverable 2: “Table2_cdc_resting_households_core”: One table for each of the 12 entomology clusters from deliverable 1, in which each row is one household. Randomly order all hh in the core of the cluster. 

# Each table will contain the Cluster # at the top. The columns of the tables will be: 
# 
# Randomization number (row number)
# Painted Recon Hh_id (i.e.; from the Raw Reconnaissance)
# Map Recon Hh_id (i.e.; from the Clean Reconnaissance)
# Ward
# Community unit
# Village
# Geolocation (lng  and lat)
# Wall type
# Roof type

hhsp@data <- hhsp@data %>%
  mutate(hh_id_clean = hhid,
         hh_id_raw = hhid) 

# Get community health unit
load('../../data_public/spatial/community_health_units.RData')
# overlay
o <- sp::over(hhsp, polygons(community_health_units))
hhsp@data$community_health_unit <- community_health_units@data$community_health_unit[o]

set.seed(17)
if('table_2_cdc_resting_households_core.csv' %in% dir('outputs/')){
  cdc_resting_households_core <- read_csv('outputs/table_2_cdc_resting_households_core.csv')
  if(FALSE){
    # Replacement of cluster 47 by 52 as per entomology team's may 2023 request
    # Done via interactive scripting
    # Auygust 4 2023 Replacement of cluster 52 by 82 as per entomology team's August 2023 request
    # Done via interactive scripting
  }
} else {
  cdc_resting_households_core <- hhsp@data %>%
    mutate(dummy = 1) %>%
    # keep just core
    filter(core_buffer == 'Core') %>%
    # randomize the order
    dplyr::sample_n(nrow(.)) %>%
    group_by(cluster_number) %>%
    # create the assignment order
    mutate(randomization_number = cumsum(dummy)) %>%
    ungroup %>%
    # Keep only the relevant columns
    dplyr::select(cluster_number,
                  randomization_number,
                  painted_recon_hh_id = hh_id_raw,
                  map_recon_hh_id = hh_id_clean,
                  ward,
                  community_health_unit,
                  village,
                  longitude = Longitude,
                  latitude  = Latitude,
                  wall_type = house_wall,
                  roof_type = house_roof) %>%
    arrange(cluster_number, randomization_number)
  write_csv(cdc_resting_households_core, 'outputs/table_2_cdc_resting_households_core.csv')
  # Create a supporting document of entomology table 2 (ie, 1 table per cluster, formatted, etc.)
  rmarkdown::render('rmds/entomology_table_2_cdc_resting_households_core.Rmd')
}

# # # Deliverable 3 ################   
# One table for each of the 12 entomology clusters from deliverable 1 in which each row is one household. Randomly order all hh in the buffer of the cluster. 
# 
# Each table will contain the Cluster # at the top. The columns of the table will be:
# 
# Randomization number (row number)
# Painted Recon Hh_id (i.e.; from the Raw Reconnaissance)
# Map Recon Hh_id (i.e.; from the Clean Reconnaissance)
# Ward
# Community unit
# Village
# Geolocation (lng  and lat)
# Wall type
# Roof type
set.seed(17)
if('table_3_cdc_resting_households_buffer.csv' %in% dir('outputs/')){
  cdc_resting_households_buffer <- read_csv('outputs/table_3_cdc_resting_households_buffer.csv')
} else {
  cdc_resting_households_buffer <- hhsp@data %>%
    mutate(dummy = 1) %>%
    # keep just core
    filter(core_buffer == 'Buffer') %>%
    # randomize the order
    dplyr::sample_n(nrow(.)) %>%
    group_by(cluster_number) %>%
    # create the assignment order
    mutate(randomization_number = cumsum(dummy)) %>%
    ungroup %>%
    # Keep only the relevant columns
    dplyr::select(cluster_number,
                  randomization_number,
                  painted_recon_hh_id = hh_id_raw,
                  map_recon_hh_id = hh_id_clean,
                  ward,
                  community_health_unit,
                  village,
                  longitude = Longitude,
                  latitude  = Latitude,
                  wall_type = house_wall,
                  roof_type = house_roof) %>%
    arrange(cluster_number, randomization_number)
  write_csv(cdc_resting_households_buffer, 'outputs/table_3_cdc_resting_households_buffer.csv')
  # Create a supporting document of entomology table 2 (ie, 1 table per cluster, formatted, etc.)
  rmarkdown::render('rmds/entomology_table_3_cdc_resting_households_buffer.Rmd')
}


# # Deliverable 4 ################   
# Table 4 resting household pit shelter

if('table_4_resting_household_pit_shelter.csv' %in% dir('outputs')){
  resting_household_pit_shelter <- read_csv('outputs/table_4_resting_household_pit_shelter.csv')
} else {
  resting_household_pit_shelter <-  hhsp@data %>%
    mutate(dummy = 1) %>%
    # randomize the order
    dplyr::sample_n(nrow(.)) %>%
    # Keep only core
    filter(core_buffer == 'Core') %>%
    group_by(cluster_number) %>%
    # create the assignment order
    mutate(randomization_number = cumsum(dummy)) %>%
    ungroup %>%
    # Keep just 3 per cluster (ie, 1 selection plus 2 backups)
    filter(randomization_number <= 3) %>%
    # left_join(sub_counties) %>%
    # # Keep only the relevant columns
    dplyr::select(cluster_number,
                  randomization_number,
                  painted_recon_hh_id = hh_id_raw,
                  map_recon_hh_id = hh_id_clean,
                  # sub_county,
                  ward,
                  community_health_unit,
                  village,
                  longitude = Longitude,
                  latitude  = Latitude,
                  wall_type = house_wall,
                  roof_type = house_roof) %>%
    arrange(cluster_number, randomization_number)
  
  resting_household_pit_shelter_b <-  hhsp@data %>%
    filter(!hh_id_raw %in% resting_household_pit_shelter$painted_recon_hh_id,
           !hh_id_clean %in% resting_household_pit_shelter$map_recon_hh_id) %>%
    mutate(dummy = 1) %>%
    # randomize the order
    dplyr::sample_n(nrow(.)) %>%
    # Keep only core
    filter(core_buffer == 'Core') %>%
    group_by(cluster_number) %>%
    # create the assignment order
    mutate(randomization_number = cumsum(dummy) + 3) %>%
    ungroup %>%
    # Keep just 3 per cluster (ie, 1 selection plus 2 backups)
    filter(randomization_number <= 10 & randomization_number >= 4) %>%
    # left_join(sub_counties) %>%
    # # Keep only the relevant columns
    dplyr::select(cluster_number,
                  randomization_number,
                  painted_recon_hh_id = hh_id_raw,
                  map_recon_hh_id = hh_id_clean,
                  # sub_county,
                  ward,
                  community_health_unit,
                  village,
                  longitude = Longitude,
                  latitude  = Latitude,
                  wall_type = house_wall,
                  roof_type = house_roof) %>%
    arrange(cluster_number, randomization_number)
  resting_household_pit_shelter <- resting_household_pit_shelter %>%
    bind_rows(resting_household_pit_shelter_b) %>%
    arrange(cluster_number, randomization_number)
  
  write_csv(resting_household_pit_shelter, 'outputs/table_4_resting_household_pit_shelter.csv')
  rmarkdown::render('rmds/resting_household_pit_shelter.Rmd')
}

# # Deliverable 5 ################   
# Deliverable 5: Table5_cdc_light_trap_livestock_enclosure: one table per cluster in which one row is one household and ALL the households of the cluster are ordered randomly. Each table will contain at the top: 
#   Cluster #
# L(cluster #)-11
#   E.g. Cluster 76, L76-11, Cluster 05, L05-11
if('table_5_cdc_light_trap_livestock_enclosures.csv' %in% dir('outputs')){
  cdc_light_trap_livestock_enclosures <- read_csv('outputs/table_5_cdc_light_trap_livestock_enclosures.csv')
} else {
  cdc_light_trap_livestock_enclosures <-  hhsp@data %>%
    mutate(dummy = 1) %>%
    # randomize the order
    dplyr::sample_n(nrow(.)) %>%
    group_by(cluster_number) %>%
    # create the assignment order
    mutate(randomization_number = cumsum(dummy)) %>%
    ungroup %>%
    # left_join(sub_counties) %>%
    # Keep only the relevant columns
    dplyr::select(cluster_number,
                  # core_buffer,
                  randomization_number,
                  painted_recon_hh_id = hh_id_raw,
                  map_recon_hh_id = hh_id_clean,
                  # sub_county,
                  ward,
                  community_health_unit,
                  village,
                  longitude = Longitude,
                  latitude  = Latitude,
                  wall_type = house_wall,
                  roof_type = house_roof) %>%
    arrange(cluster_number, randomization_number)
  
  write_csv(cdc_light_trap_livestock_enclosures, 'outputs/table_5_cdc_light_trap_livestock_enclosures.csv')
  rmarkdown::render('rmds/cdc_light_trap_livestock_enclosures.Rmd')
}

# Deliverable 9, geographic files
#####################################################################
# Generate spatial files and then manually copy-paste to this google drive:
# https://drive.google.com/drive/folders/1pVEcZzPevVCcHe4Sc4lAn5Gmri1xR_RB?usp=sharing
if(!dir.exists('outputs/ento_households_shp')){
  dir.create('outputs/ento_households_shp')
}
if(!dir.exists('outputs/cores_shp')){
  dir.create('outputs/cores_shp')
}
if(!dir.exists('outputs/buffers_shp')){
  dir.create('outputs/buffers_shp')
}

if(!file.exists('outputs/ento_households_shp/households.shp')){
  message('Writing shapefile')  
  households <- hhsp
  # households <- households[households@data$cluster_number %in% ento_clusters$cluster_number,]
  households@data <- households@data %>% dplyr::select(cluster_number, core_buffer, hh_id_clean, hh_id_raw, Longitude, Latitude)
  raster::shapefile(households, 'outputs/ento_households_shp/households.shp', overwrite = TRUE)
} else {
  message('Shapefile already written')
}

if(!file.exists('outputs/buffers_shp/buffers_shp.shp')){
  clusters <- new_clusters
  cores <- new_cores
  row.names(cores@data) <- cores@data$cluster_nu
  row.names(clusters@data) <- clusters@data$cluster_nu
  AddHoleToPolygon <-function(poly,hole){
    # invert the coordinates for Polygons to flag it as a hole
    coordsHole <-  hole@polygons[[1]]@Polygons[[1]]@coords
    newHole <- Polygon(coordsHole,hole=TRUE)
    
    # punch the hole in the main poly
    listPol <- poly@polygons[[1]]@Polygons
    listPol[[length(listPol)+1]] <- newHole
    punch <- Polygons(listPol,poly@polygons[[1]]@ID)
    
    # make the polygon a SpatialPolygonsDataFrame as the entry
    new <- SpatialPolygons(list(punch),proj4string=poly@proj4string)
    new <- SpatialPolygonsDataFrame(new,data=as(poly,"data.frame"))
    
    return(new)
  }
  cluster_numbers <- sort(unique(clusters@data$cluster_nu))
  buffer_list <- list()
  for(i in 1:length(cluster_numbers)){
    this_cluster_number <- cluster_numbers[i]
    outer <- clusters[clusters@data$cluster_nu == this_cluster_number,]
    inner <- cores[cores@data$cluster_nu == this_cluster_number,]
    row.names(outer) <- row.names(inner) <- this_cluster_number
    buffers <- AddHoleToPolygon(outer, inner)
    buffer_list[[i]] <- buffers
  }
  buffers <- rbind(buffer_list[[1]], buffer_list[[2]], makeUniqueIDs = TRUE)
  for(i in 3:length(cluster_numbers)){
    buffers <- rbind(buffers, buffer_list[[i]], makeUniqueIDs = TRUE)
  }
  plot(buffers)
  buffers@data$cluster <- cluster_numbers
  buffers@data$cluster_number <- buffers@data$cluster
  message('Writing shapefile')  
  buffers_shp <- buffers[buffers@data$cluster_number %in% ento_clusters$cluster_number,]
  raster::shapefile(buffers_shp, 'outputs/buffers_shp/buffers_shp.shp')
  save(buffers, file = '../../data_public/spatial/buffers.RData')
  raster::shapefile(buffers, '../../data_public/spatial/buffers/buffers.shp', overwrite = TRUE)
} else {
  message('Shapefile already written')
}

if(!file.exists('outputs/cores_shp/cores_shp.shp')){
  message('Writing shapefile')
  cores$cluster_number <- as.numeric(cores$cluster_nu)
  cores_shp <- cores[cores@data$cluster_number %in% ento_clusters$cluster_number,]
  raster::shapefile(cores_shp, 'outputs/cores_shp/cores_shp.shp', overwrite = TRUE)
} else {
  message('Shapefile already written')
}

# Assign arms 1 and 2 to control / intervention
set.seed(321)
if('intervention_assignment.csv' %in% dir('outputs')){
  intervention_assignment <- read_csv('outputs/intervention_assignment.csv')
} else {
  intervention_assignment <- tibble(arm = sample(1:2, 2),
                                    intervention = c('Control', 'Treatment'))
  write_csv(intervention_assignment, 'outputs/intervention_assignment.csv')
}

##########################
# DELIVERABLE 7
##########################
# Deliverable 7: “Ento_enrolled_households” Table of all houses enrolled for Entomology after round 1 of Ento work (from round 2 onwards Caroline Kiuru will generate this list herself if any hh has changed): One table that includes all clusters in which one row is one household. This table is generated using the data collected from the Screening form where the answer to “What are you screening?” == household and “Has the household head or household head substitute agreed to participate to mosquito collections“  == Yes
# 
# The columns of the list will be: 
#   Cluster
# Collection method (□ CDC-light trap □ Resting household indoor □ Resting household pit-shelter)
# Recon Hh_id entered in the Screening form (Map HhID: “Write the Recon5-character household ID of the pin that you see in the map”)
# New Hh ID assigned
# Core/buffer
# Ward
# Community unit
# Village
# Geolocation (lng  and lat)
# Wall type
# Roof type

# Read in the cleaned ento screening results (generated in scripts/ento_screening)
es <- read_csv('inputs/entoscreening_households_cleaned.csv')
if(!file.exists('outputs/table_7_ento_enrolled_households.csv')){
  out <- es %>%
    mutate(longitude = Longitude,
           latitude = Latitude) %>%
    mutate(x = longitude, y = latitude)
  coordinates(out) <- ~x+y
  proj4string(out) <- proj4string(clusters)
  o <- sp::over(out, polygons(clusters))
  clusters@data$cluster_number <- as.numeric(clusters@data$cluster_nu)
  out@data$cluster <- clusters@data$cluster_number[o]
  # Examine those not in cluster
  if(FALSE){
    bad <- out[is.na(out@data$cluster),]
    right <- recon[recon$hh_id %in% bad$recon_hhid_map,]
    combined <- bind_rows(
      bad@data %>%
        dplyr::rename(new_hhid = hhid) %>%
        dplyr::select(hhid = recon_hhid_map, longitude, latitude) %>% mutate(type = 'Ento screening'),
      right %>% dplyr::select(hhid = hh_id, longitude = Longitude,
                              latitude = Latitude) %>% mutate(type = 'Recon')
    ) %>%
      mutate(color_number = as.numeric(factor(hhid)))
    cols <- rainbow(length(unique(combined$color_number)))
    combined$col <- cols[combined$color_number]
    leaflet() %>%
      addTiles() %>%
      addPolygons(data = clusters, label = clusters$cluster_number) %>%
      addCircleMarkers(data = combined, col = combined$col, 
                       popup = combined$hhid,
                       label = combined$hhid)
    library(ggplot2)
    clusters_fortified <- fortify(clusters[clusters$cluster_number %in% c(43,47),], id = clusters$cluster_number)
    
    ggplot() +
      geom_polygon(data = clusters_fortified,
                   aes(x = long,
                       y =lat,
                       group = group),
                   color = 'black',
                   alpha = 0.5) +
      geom_point(data = combined,
                 aes(x = longitude,
                     y = latitude,
                     group = hhid,
                     color = hhid),
                 size = 3) +
      geom_path(data = combined,
                 aes(x = longitude,
                     y = latitude,
                     group = hhid,
                     color = hhid)) +
      theme_bw() +
      theme(legend.position = 'bottom')
  }
  # REMOVE THOSE NOT IN A CLUSTER
  outside <- out[is.na(out@data$cluster),]
  plot(outside, pch = 2, cex = 2, col = 'blue')
  plot(clusters, add = T, col = 'red')
  text(coordinates(clusters), label = clusters@data$cluster_number)
  text(coordinates(outside), label = outside@data$hhid)
  outside@data %>% dplyr::select(recon_hhid_map, hhid, wid) %>% arrange(hhid)
  out <- out[!is.na(out@data$cluster),]
  # See whether in core or buffer
  o <- sp::over(out, polygons(cores))
  out@data$in_core <- !is.na(o)
  out@data$core_or_buffer <- ifelse(out@data$in_core, 'Core', 'Buffer')
  # Create the requested columns
  ento_enrolled_households <- out@data %>%
    dplyr::select(cluster,
                  hhid,
                  collection_method = hh_collection,
                  hh_id = recon_hhid_map,
                  core_or_buffer,
                  ward,
                  community_health_unit,
                  village,
                  longitude,
                  latitude) %>%
    # left_join(recon %>% dplyr::select(hh_id,
    #                                   wall_type = house_wall_material,
    #                                   roof_type)) %>%
    # remove the recon id
    dplyr::rename(recon_id = hh_id) %>%
    arrange(cluster, hhid)
  # Remove (invalid) cluster 47
  ento_enrolled_households <- ento_enrolled_households %>%
    filter(cluster != 47, cluster != 52)
  write_csv(ento_enrolled_households, 'outputs/table_7_ento_enrolled_households.csv')
} else {
  ento_enrolled_households <- read_csv('outputs/table_7_ento_enrolled_households.csv')
}


##########################
# DELIVERABLE 8
##########################
# Deliverable 8: “Ento_enrolled_livestock enclosures” Table of all livestock enclosures enrolled for Entomology. One table that includes all clusters in which one row is one household. This table is generated using the data collected from the Screening form where the answer to “What are you screening?” == Livestock enclosure and “Has the owner of the livestock enclosure or representative given informed consent?” == yes
# The columns of the list will be: 
#   Cluster
# Hh_id (i.e.; Clean Recon HhId of the closest hh from the LE GPS registered)
# Livestock enclosure ID
# Ward
# Community unit
# Village
# Geolocation (lng  and lat)
# Wall type
# Roof type
# Download entoscreeningke.zip from databrew.org
# unzip('entoscreeningke.zip')
# Read
if(file.exists('outputs/table_8_entomology_enrolled_livestock_enclosures.csv')){
  le <- read_csv('outputs/table_8_entomology_enrolled_livestock_enclosures.csv')
} else {
  # entoscreeningke <- read_csv('../../scripts/ento_screening/entoscreeningke.csv')
  entoscreeningke <- read_csv('../../scripts/ento_screening/kwale/clean-form/entoscreeningke/entoscreeningke.csv')
  
  out <- entoscreeningke %>%
    filter(site == 'Livestock enclosure',
           le_owner_consent == 'yes') %>%
    mutate(longitude = `Longitude`,
           latitude = `Latitude`) %>%
    mutate(x = longitude, y = latitude)
  # Get which cluster each is in
  out_sp <- out
  coordinates(out_sp) <- ~x+y
  proj4string(out_sp) <- proj4string(hhsp)
  o <- sp::over(out_sp, polygons(clusters))
  out_sp@data$cluster <- clusters@data$cluster_number[o]
  # Get nearest household
  # recon_curated_sp <- recon_curated %>%
  #   left_join(recon %>% dplyr::select(instanceID, latitude = Latitude,
  #                                     longitude = Longitude,
  #                                     ward,
  #                                     community_unit = community_health_unit,
  #                                     village,
  #                                     wall_type = house_wall_material,
  #                                     roof_type)) %>%
  #   mutate(x = longitude, y = latitude)
  recon_curated_sp <- v0demography %>%
    mutate(x = Longitude, y = Latitude)
  coordinates(recon_curated_sp) <- ~x+y
  proj4string(recon_curated_sp) <- proj4string(hhsp)
  distances <- rgeos::gDistance(out_sp, recon_curated_sp, byid = TRUE)
  min_distances <- apply(distances, 2, which.min)
  out_sp@data$nearest_household <- recon_curated_sp$hhid[min_distances]
  out <- out_sp@data %>%
    dplyr::select(cluster,
                  hh_id = nearest_household,
                  livestock_enclosure_id = leid,
                  ward,
                  # community_unit = community_health_unit,
                  village,
                  longitude = Longitude,
                  latitude = Latitude) %>%
    # # get wall type and roof type based on nearest household
    left_join(v0demography %>% dplyr::select(hh_id = hhid,
                                             roof_type = house_roof,
                                             wall_type = house_wall))
    # left_join(recon_curated_sp@data %>% dplyr::select(hh_id = hh_id_clean,
    #                                                   roof_type,
    #                                                   wall_type))
  le <- out
  # Remove deprecated clusters
  le <- le %>% filter(!is.na(cluster))
  write_csv(le, 'outputs/table_8_entomology_enrolled_livestock_enclosures.csv')
}

###########################
# EFFICACY
###########################

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
date_of_enrollment <- as.Date('2023-10-01')
# See who is eligible or not
people <- people %>%
  mutate(age_at_enrollment  = date_of_enrollment - dob) %>%
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

library(sf)
dist_for_edge <- st_geometry(obj = st_clusters_projected) %>%
  st_cast(to = 'MULTILINESTRING') %>%
  st_distance(y=st_eligibles)
distances <- apply(dist_for_edge, 2, min)
st_eligibles$distance_to_edge <- distances
eligibles_sp_projected@data$distance_to_edge <- distances
plot(clusters_projected)
points(eligibles_sp_projected, pch = '.')
library(ggplot2)
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

# One-off request for Carlos
# https://trello.com/c/iSs67XrF/2000-efficacy-selection-with-additional-information
# distance_to_edge
# age as of oct 1 2023
if(FALSE){
  carlos <- efficacy_selection %>%
    left_join(eligibles_sp_projected@data %>%
                mutate(age_at_enrollment = as.numeric(age_at_enrollment)) %>%
                dplyr::select(extid, distance_to_edge, age_at_enrollment)) %>%
    dplyr::select(cluster, extid, priority_number, distance_to_edge, age_at_enrollment) %>%
    arrange(cluster, priority_number)
  write_csv(carlos, '~/Desktop/efficacy_expanded.csv')
}

# Per Paula's instructions (https://docs.google.com/document/d/1Tjpyh8O9oesnDiQgjEih1VpOIZFctpM7UA5aDK--N8o/edit)
# create "ntd_efficacy_preselected" variable for those who are both :
# a) from a health economics household
# b) efficacy preselected
file_path1 <- 'outputs/health_economics_ntd_efficacy_preselection.csv'
file_path2 <- 'outputs/health_economics_ntd_safety_preselection.csv'
if(file.exists(file_path1)){
  ntd_efficacy_preselection <- read_csv(file_path1)
  ntd_safety_preselection <- read_csv(file_path2)
} else {
  ntd_efficacy_preselection <- efficacy_selection %>%
    mutate(hhid = substr(extid, 1, 5)) %>%
    dplyr::select(hhid, extid) %>%
    filter(hhid %in% health_economics_households$hhid) %>%
    dplyr::select(extid) %>%
    mutate(ntd_efficacy_preselected =1 ) 
  ntd_safety_preselection <- v0demography_repeat_individual %>%
    left_join(v0demography_spatial@data %>%
                dplyr::select(KEY,
                              hhid,
                              lng = Longitude,
                              lat = Latitude,
                              # cluster,
                              in_core,
                              in_cluster,
                              cluster_cluster_number,
                              core_cluster_number),
              by = c('PARENT_KEY' = 'KEY')) %>%
    filter(hhid %in% health_economics_households$hhid) %>%
    filter(!extid %in% efficacy_selection$extid) %>%
    filter(dob < as.Date('2018-10-01')) %>%
    mutate(dummy = 1) %>%
    dplyr::sample_n(nrow(.)) %>%
    filter(!is.na(cluster_cluster_number)) %>%
    group_by(cluster = cluster_cluster_number) %>%
    mutate(cs = cumsum(dummy)) %>%
    ungroup %>%
    filter(cs <= 50) %>%
    dplyr::select(cluster, extid) %>%
    mutate(ntd_safety_preselected = 1)
  write_csv(ntd_efficacy_preselection, file_path1)
  write_csv(ntd_safety_preselection, file_path2)
}

# PK selection

# PK Clusters
# Instructions: https://docs.google.com/document/d/1Tjpyh8O9oesnDiQgjEih1VpOIZFctpM7UA5aDK--N8o/edit
file_path <- 'outputs/pk_clusters.csv'
if(file.exists(file_path)){
  pk_clusters <- read_csv(file_path)
} else {
  # Pick 10 of the following 12
  possible_pk_clusters <- c(1,
                            52,
                            71,
                            76,
                            89,
                            4,
                            6,
                            32,
                            66,
                            86,
                            35,
                            47)
  pk_clusters <- tibble(cluster_number = sample(possible_pk_clusters, size = 10, replace = FALSE))
  pk_clusters <- pk_clusters %>% arrange(cluster_number)
  # New specifications on September 11 2023 from project:
  # https://bohemiakenya.slack.com/archives/C059Q4RU2CA/p1694420124056119
  # pk_clusters <- clusters@data %>%
  #   dplyr::select(cluster_number) %>%
  #   left_join(assignments %>% dplyr::select(cluster_number, assignment)) %>%
  #   left_join(intervention_assignment, by = c('assignment' = 'arm')) %>%
  #   filter(intervention == 'Treatment') %>%
  #   dplyr::sample_n(10) %>%
  #   dplyr::select(cluster_number) %>%
  #   arrange(cluster_number)
  write_csv(pk_clusters, file_path)
}

# PROJECT NO LONGER DOING THIS: https://bohemiakenya.slack.com/archives/C042KSRLYUA/p1693205208066469?thread_ts=1692786669.002209&cid=C042KSRLYUA
# # PK Individuals
# # Instructions at https://docs.google.com/document/d/1Tjpyh8O9oesnDiQgjEih1VpOIZFctpM7UA5aDK--N8o/edit
# # Select 8 individuals per cluster from the Pk clusters (pk_cluster=yes) that are
# # ≥ 18 - <65 years old as of Oct 1st
# # NTD_efficacy_preselected = no (actually, not possible because Efficacy kids are all 5-15, but just in case)
# # NTD_safety_preselected = no
# 
# file_path <- 'outputs/pk_individuals.csv'
# if(file.exists(file_path)){
#   pk_individuals <- read_csv(file_path)
# } else {
#   pk_individuals <-  v0demography_repeat_individual %>%
#     left_join(v0demography_spatial@data %>%
#                 dplyr::select(KEY,
#                               hhid,
#                               lng = Longitude,
#                               lat = Latitude,
#                               # cluster,
#                               in_core,
#                               in_cluster,
#                               cluster_cluster_number,
#                               core_cluster_number),
#               by = c('PARENT_KEY' = 'KEY')) %>%
#     filter(!extid %in% ntd_efficacy_preselection$extid) %>%
#     filter(!extid %in% ntd_safety_preselection$extid) %>%
#     # must be >= 18 and < 65 as of oct 1 2023
#     # >=18
#     filter(dob <= as.Date('2005-10-01')) %>%
#     # < 65
#     filter(dob > as.Date('1958-10-01')) %>%
#     # randomize order
#     dplyr::sample_n(nrow(.)) %>%
#     # select 8 for each cluster
#     mutate(dummy = 1) %>%
#     filter(!is.na(cluster_cluster_number)) %>%
#     filter(cluster_cluster_number %in% pk_clusters$cluster_number) %>%
#     group_by(cluster = cluster_cluster_number) %>%
#     mutate(cs = cumsum(dummy)) %>%
#     filter(cs <= 8) %>%
#     ungroup %>%
#     dplyr::select(cluster, extid) %>%
#     arrange(cluster, extid)
#   write_csv(pk_individuals, file_path)
# }

# New PK individuals selection instructions September 11 2023
file_path <- 'outputs/pk_individuals.csv'
if(file.exists(file_path)){
  pk_individuals <- read_csv(file_path)
} else {
  # Get the OLD clusters (dropped clusters) from v0 demography
  v0demography_spatial_old <- v0demography %>% mutate(x = Longitude, y = Latitude)
  coordinates(v0demography_spatial_old) <- ~x+y
  proj4string(v0demography_spatial_old) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0")
  p4s <- "+proj=utm +zone=37 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
  crs <- CRS(p4s)
  v0demography_spatial_old_projected <- spTransform(v0demography_spatial_old, crs)
  old_clusters_projected <- spTransform(old_clusters, crs)
  old_clusters_projected_buffered <- rgeos::gBuffer(old_clusters_projected, byid = TRUE, width = 50)
  
  o <- sp::over(v0demography_spatial_old_projected, polygons(old_clusters_projected_buffered))
  v0demography_spatial_old@data$in_cluster <- !is.na(o)
  v0demography_spatial_old$old_cluster_number <- as.numeric(old_clusters_projected_buffered@data$cluster_number[o])
  
  # Select 15 individuals per cluster from the Pk clusters (pk_cluster=yes)
  eligibles <- v0demography_repeat_individual %>%
    left_join(v0demography_spatial_old@data %>%
                dplyr::select(KEY,
                              lng = Longitude, lat = Latitude,
                              old_cluster_number),
              by = c('PARENT_KEY' = 'KEY'))
  date_of_enrollment <- as.Date('2023-10-01')
  # Keep only those in pk clusters who are of correct age, and only 15 per cluster
  eligibles <- eligibles %>%
    mutate(age_at_enrollment  = date_of_enrollment - dob) %>%
    mutate(age_at_enrollment = age_at_enrollment / 365.25) %>%
    mutate(pk_eligible = age_at_enrollment >= 18.0 & age_at_enrollment < 65.0) %>%
    filter(!is.na(old_cluster_number)) %>%
    filter(old_cluster_number %in% pk_clusters$cluster_number) %>%
    filter(pk_eligible) %>%
    dplyr::sample_n(nrow(.)) %>%
    mutate(dummy = 1) %>%
    group_by(cluster = old_cluster_number) %>%
    mutate(cs = cumsum(dummy)) %>%
    ungroup %>%
    filter(cs <= 15) %>%
    dplyr::select(extid, cluster, cs, age, lng, lat, dob)
  # Keep only relevant columns
  pk_individuals <- eligibles %>%
    dplyr::select(cluster, extid)  %>%
    arrange(cluster, extid)
  write_csv(pk_individuals, file_path)
}
