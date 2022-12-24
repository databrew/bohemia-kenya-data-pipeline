library(paws)
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

# get household data and remove unused columns
hh <- get_household_forms() %>%
  tibble::as_tibble() %>%
  tidyr::drop_na() %>%
  distinct(Longitude, Latitude, .keep_all = TRUE) %>%
  dplyr::select(Longitude, Latitude, village)

# thanks joe for the code
ken3 <- raster::getData(name = 'GADM',
                        download = TRUE,
                        country = 'KEN',
                        level = 3)
ken3_fortified <- fortify(ken3, regions = ken3@data$NAME_3)
kwale <- ken3[ken3@data$NAME_1 == 'Kwale',]
kwale_fortified <- fortify(kwale, regions = ken3@data$NAME_3)
pongwe_kikoneni <- ken3[ken3@data$NAME_3 == 'Pongwe/Kikoneni',]
pongwe_kikoneni_fortified <- fortify(pongwe_kikoneni, regions = ken3@data$NAME_3)



# create using shapefile
studywards <- sf::st_read(dsn = "voronoi/StudyAreaWards") %>% st_transform(4326)
spatial_dataframe <- sf::as_Spatial(studywards,IDs = studywards$Ward)
wards_fortified <- broom::tidy(
  spatial_dataframe,
  regions = spatial_dataframe@data$Ward)
ward_mapping <- tibble(id = seq(1, length(spatial_dataframe@data$Ward)), ward = spatial_dataframe@data$Ward) %>%
  dplyr::mutate(id = as.character(id))

list_wards <- c('Pongwekikoneni')
wards_fortified <- wards_fortified %>%
  dplyr::inner_join(ward_mapping, by = c("id")) %>%
  dplyr::filter(ward %in% list_wards)



# village color palette
mycolors <- colorRampPalette(brewer.pal(8, "Set3"))(hh$village %>% unique() %>% length())

# basic village plot
p <- ggplot() +
  geom_polygon(data = wards_fortified,
               aes(x = long,
                   y = lat,
                   group = group),
               color = 'black',
               fill = 'white',
               alpha = 0.5) +
  geom_point(data = hh,
             aes(x = Longitude,
                 y = Latitude,
                 color = village),
             alpha = 1,
             size = 0.9) +
  scale_color_manual(values = mycolors) +
  ggthemes::theme_map() +
  labs(title = "1. Household Forms Collected",
       subtitle = "Ward: Pongwe") +
  theme(legend.position =  "none", plot.title = element_text(face="bold"))
p.interactive <- ggplotly(p)
htmlwidgets::saveWidget(
  p.interactive,
  "villages_points.html")


# create voronoi polygons with pongwe outline
ox_diagram <- voronoi_polygon(
  hh,
  x="Longitude",
  y="Latitude",
  outline = spatial_dataframe[spatial_dataframe@data$Ward == 'Pongwekikoneni',])


# clean paths inside convex hull
ox_diagram_union<- gUnaryUnion(
  ox_diagram,
  id = ox_diagram$village)
ox_data_union <- broom::tidy(ox_diagram_union)
pv2 <- ggplot() +
  geom_polygon(
    data = ox_data_union %>%
      dplyr::select(village = id,
                    everything()),
    aes(x=long,
        y=lat,
        group=group,
        fill = village)) +
  geom_path(data = wards_fortified,
            aes(x = long,
                y = lat,
                group = group)) +
  coord_equal() +
  scale_fill_manual(values = mycolors) +
  theme_minimal() +
  theme(legend.position = "none")
p.interactive <- ggplotly(pv2)
htmlwidgets::saveWidget(
  p.interactive,
  "villages_voronoi.html")
shapefile(x = ox_diagram_union,
          file = "village.shp",
          overwrite=TRUE)

