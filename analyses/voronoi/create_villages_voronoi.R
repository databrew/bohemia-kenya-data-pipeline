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
ken3 <- raster::getData(name = 'GADM', download = TRUE, country = 'KEN', level = 3)
ken3_fortified <- fortify(ken3, regions = ken3@data$NAME_3)
kwale <- ken3[ken3@data$NAME_1 == 'Kwale',]
kwale_fortified <- fortify(kwale, regions = ken3@data$NAME_3)
pongwe_kikoneni <- ken3[ken3@data$NAME_3 == 'Pongwe/Kikoneni',]
pongwe_kikoneni_fortified <- fortify(pongwe_kikoneni, regions = ken3@data$NAME_3)

# create voronoi polygons with pongwe outline
ox_diagram <- voronoi_polygon(hh,x="Longitude",y="Latitude",
                              outline =  pongwe_kikoneni)
# fortify for geom_polygon -friendly format
ox_data <- fortify_voronoi(ox_diagram)

# create html for villages
mycolors <- colorRampPalette(brewer.pal(8, "Set3"))(hh$village %>% unique() %>% length())
p <- ox_data %>%
  ggplot(aes(x=x,
             y=y,
             fill=village,
             group=group)) +
  geom_polygon() +
  scale_fill_manual(values = mycolors) +
  coord_equal() +
  theme_minimal() +
  theme(legend.position = "none")
p.interactive <- ggplotly(p)
htmlwidgets::saveWidget(p.interactive,
                        "villages.html")
