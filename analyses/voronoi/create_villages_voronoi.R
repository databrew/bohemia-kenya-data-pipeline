library(paws)
library(ggvoronoi)
library(dplyr)
library(magrittr)
library(data.table)
library(ggplot2)
library(sp)
library(plotly)
library(leaflet)
library(RColorBrewer)

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

# village color palette
mycolors <- colorRampPalette(brewer.pal(8, "Set3"))(hh$village %>% unique() %>% length())

# basic village plot
p <- ggplot() +
  geom_polygon(data = pongwe_kikoneni_fortified,
               aes(x = long,
                   y = lat,
                   group = group),
               color = 'black',
               fill = 'beige',
               alpha = 0.5) +
  geom_point(data = hh,
             aes(x = Longitude,
                 y = Latitude,
                 color = village),
             alpha = 0.6,
             size = 0.9) +
  scale_color_manual(values = mycolors) +
  ggthemes::theme_map() +
  theme(legend.position =  "none")

# create voronoi polygons with pongwe outline
ox_diagram <- voronoi_polygon(
  hh, x="Longitude",
  y="Latitude",
  outline = pongwe_kikoneni)

# create html for villages
ox_data <- broom::tidy(
  ox_diagram,
  region = "village")
p <- ox_data %>%
  ggplot(aes(x=long,
             y=lat,
             fill=id,
             group=group)) +
  geom_polygon() +
  geom_path() +
  scale_fill_manual(values = mycolors) +
  coord_equal() +
  theme_minimal() +
  theme(legend.position = "none")
p.interactive <- ggplotly(p)
htmlwidgets::saveWidget(p.interactive,
                        "pongwe_villages_with_paths.html")




# clean paths inside convex hull
ox_diagram_2 <- gUnaryUnion(
  ox_diagram,
  id = ox_diagram$village)
ox_data_2 <- broom::tidy(ox_diagram_2)
pv2 <- ox_data_2 %>%
  ggplot(aes(x=long,
             y=lat,
             fill=id,
             group=group)) +
  geom_polygon() +
  geom_path() +
  scale_fill_manual(values = mycolors) +
  coord_equal() +
  theme_minimal() +
  theme(legend.position = "none")
p.interactive <- ggplotly(pv2)
htmlwidgets::saveWidget(p.interactive,
                        "pongwe_villages_clean.html")
shapefile(x = ox_diagram_2,
          file = "pongwe.shp",
          overwrite=TRUE)
