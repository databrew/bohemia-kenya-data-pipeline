library(ggvoronoi)
library(dplyr)
library(magrittr)
library(data.table)
library(ggplot2)
library(sp)
library(sf)
library(plotly)
library(leaflet)
library(RColorBrewer)
library(rgeos)
library(raster)
library(smoothr)
Sys.setenv(SHAPE_RESTORE_SHX = 'YES')

# set your pipeline stage here to define prod/dev environment
# to test it locally do Sys.setenv(PIPELINE_STAGE = 'develop')
cloudbrewr::aws_login(pipeline_stage = Sys.getenv('PIPELINE_STAGE'))

# input
INPUT_KEY <- list(
  household = 'kwale/clean-form/reconbhousehold/reconbhousehold.csv',
  shapefiles = 'kwale/shapefiles/StudyAreaWards.zip'
)
# bucket name
S3_BUCKET_NAME <- 'databrew.org'

hh <- cloudbrewr::aws_s3_get_table(
  bucket = S3_BUCKET_NAME,
  key = INPUT_KEY$household) %>%
  distinct(Longitude, Latitude, .keep_all = TRUE) %>%
  dplyr::select(instanceID, hh_id,Longitude, Latitude, village) %>%
  tidyr::drop_na()

study_wards <- cloudbrewr::aws_s3_get_object(
  bucket = S3_BUCKET_NAME,
  key = INPUT_KEY$shapefiles
)

# append this file with more wards
list_wards <- c('Pongwekikoneni', 'Ramisi')

# create using shapefile
unzip(study_wards$file_path)
studywards <- sf::st_read(dsn = "StudyAreaWards") %>% st_transform(4326)
spatial_dataframe <- sf::as_Spatial(studywards,IDs = studywards$Ward)
wards_fortified <- broom::tidy(
  spatial_dataframe,
  regions = spatial_dataframe@data$Ward)
ward_mapping <- tibble(id = seq(1, length(spatial_dataframe@data$Ward)), ward = spatial_dataframe@data$Ward) %>%
  dplyr::mutate(id = as.character(id))
wards_fortified <- wards_fortified %>%
  dplyr::inner_join(ward_mapping, by = c("id")) %>%
  dplyr::filter(ward %in% list_wards)

# village color palette
mycolors <- colorRampPalette(brewer.pal(8, "Set3"))(hh$village %>% unique() %>% length())

# create output directories
dir.create("output/htmls", recursive = TRUE, showWarnings = FALSE)
dir.create("output/village_shapefiles", recursive = TRUE, showWarnings = FALSE)

# create voronoi polygons with pongwe outline
ox_diagram <- voronoi_polygon(
  hh,
  x="Longitude",
  y="Latitude",
  outline = spatial_dataframe[spatial_dataframe@data$Ward %in% list_wards,])

# clean paths inside convex hull
ox_diagram_union<- gUnaryUnion(
  ox_diagram,
  id = ox_diagram$village)

village_index <- data.frame(
  id = unique(ox_diagram$village),
  row.names = unique(ox_diagram$village))

ox_diagram_spdf <- SpatialPolygonsDataFrame(
  ox_diagram_union,
  village_index
)


######################################################################

## new process using sf and leaflet
unlink('output/village_shapefile',recursive = TRUE)
ox_diagram_spdf_new <- sf::st_as_sf(ox_diagram_spdf)
sf::st_crs(ox_diagram_spdf_new) = 4326
poly <- ox_diagram_spdf_new %>%
  smoothr::fill_holes(threshold =  units::set_units(1, km^2)) %>%
  smoothr::smooth(method = "ksmooth")
dir.create('output/village_shapefile')
st_write(poly,
         'output/village_shapefile/village.shp',
         overwrite = TRUE)
zip(
  files = 'output/village_shapefile',
  zipfile = 'output/village_shapefile/village_spdf.zip')


content_placeholder <- paste0("Household ID: {hh_id}<br/>",
                              "Village: {village}<br/>",
                              "InstanceI ID: {instanceID}<br/>")
hh_new <- hh  %>%
  dplyr::mutate(content = glue::glue(content_placeholder))
pal <- colorFactor(palette = "Dark2",
                   domain = unique(hh_new$village))


unlink('output/village_html', recursive = TRUE)
lf <- leaflet(hh_new) %>%
  addProviderTiles(providers$Stamen.Toner, group = "Basic") %>%
  addProviderTiles(providers$OpenStreetMap, group = "OSM") %>%
  addProviderTiles(providers$Esri.WorldImagery, group = "Satellite") %>%
  addProviderTiles(providers$CartoDB.Positron, group = "CartoDB") %>%
  addPolygons(data = poly,
              weight = 0.5,
              fillColor = 'white',
              group = "Village Boundaries") %>%
  addCircleMarkers(
    lng=~Longitude,
    stroke = FALSE,
    fillOpacity = 0.5,
    radius = 3,
    lat=~Latitude,
    popup = ~content,
    color = ~pal(village),
    group = "Households"
  ) %>%
  addLayersControl(
    baseGroups = c("Basic", "OSM", "Satellite", "CartoDB"),
    overlayGroups = c(#"Hamlet centroids",
      'Village Boundaries',
      'Households'),
    options = layersControlOptions(collapsed = FALSE)
  )
dir.create('output/village_html')
htmlwidgets::saveWidget(lf,
                        file = 'output/village_html/village.html',
                        selfcontained = TRUE)
