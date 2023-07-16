# Get duplication
detect_duplication <- function(data,
                               col,
                               form_id,
                               anomalies_id,
                               anomalies_description,
                               threshold = 2,
                               key = 'KEY'){

  data %>%
    dplyr::group_by(!!sym(col)) %>%
    dplyr::mutate(n = n()) %>%
    dplyr::filter(n >= threshold) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      form_id = form_id,
      anomalies_id = anomalies_id,
      anomalies_description = glue::glue('hhid:{var} duplicated {n} times',
                                         var = !!sym(col))) %>%
    tidyr::drop_na(hhid) %>%
    dplyr::select(!!sym(key),
                  form_id,
                  anomalies_id,
                  anomalies_description)
}

# Get threshold breach
detect_threshold <- function(data,
                             col,
                             form_id,
                             anomalies_id,
                             anomalies_description,
                             threshold,
                             direction = 'more',
                             key = 'KEY') {

  if(direction == 'more') {
    data <- data %>%
      dplyr::filter(!!sym(col) > threshold)
  }else{
    data <- data %>%
      dplyr::filter(!!sym(col) < threshold)
  }

  data %>%
    dplyr::mutate(
      form_id = form_id,
      anomalies_id = anomalies_id,
      anomalies_description = anomalies_description) %>%
    dplyr::select(!!sym(key),
                  form_id,
                  anomalies_id,
                  anomalies_description)

}


# function for detecting boundaries
detect_outside_cluster_boundaries <- function(data,
                                              form_id) {
  temp_folder <- '/tmp'
  cluster_obj <- aws_s3_get_object(
    bucket = 'bohemia-spatial-assets',
    key = 'kwale/clusters.zip',
    output_dir = temp_folder)

  unzip(
    cluster_obj$file_path,
    exdir = temp_folder)

  clusters <- rgdal::readOGR(
    glue::glue('{temp_folder}/clusters/'),
    'clusters')

  coordinates(data) <- ~Longitude+Latitude
  proj4string(data) <- proj4string(clusters)

  # Add a 100 meter buffer
  # Convert objects to projected UTM reference system
  p4s <- "+proj=utm +zone=37 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
  crs <- CRS(p4s)
  clusters_projected <- spTransform(clusters, crs)
  clusters_projected_buffered <- rgeos::gBuffer(clusters_projected, byid = TRUE, width = 100)
  data_projected <- spTransform(data, crs)
  o <- sp::over(data_projected, polygons(clusters_projected_buffered))
  o_strict <- sp::over(data_projected, polygons(clusters_projected))
  anom <- data_projected@data
  anom$outside_cluster <- is.na(o_strict)
  anom$outside_cluster_number <- clusters_projected@data$cluster_nu[o]
  anom$outside_cluster_by_more_than_100_m <- is.na(o)
  anom$outside_cluster_by_more_than_100_m_number <- clusters_projected_buffered@data$cluster_nu[o]

  anom <- anom %>% dplyr::select(
    cluster,
    instanceID,
    SubmissionDate,
    start_time,
    todays_date,
    wid, fa_id, cluster,
    # Latitude, Longitude,
    recon_hhid_map, hhid,
    outside_cluster_by_more_than_100_m,
    outside_cluster,
    outside_cluster_number,
    outside_cluster_by_more_than_100_m_number) %>%
    mutate(cluster = as.character(cluster)) %>%
    mutate(cluster_number_mismatch = outside_cluster_number != cluster)

  anom_final <- anom %>%
    filter(outside_cluster_by_more_than_100_m | outside_cluster | cluster_number_mismatch)

  # 1 outside
  outside <- anom_final %>%
    dplyr::select(
      cluster,
      outside_cluster,
      outside_cluster_number,
      instanceID,
      hhid) %>%
    dplyr::filter(outside_cluster) %>%
    dplyr::mutate(
      anomalies_id = 'hh_outside_cluster',
      anomalies_description =
        glue::glue(
          'hhid:{hhid} is outside of original cluster:{cluster}')) %>%
    dplyr::select(KEY = instanceID, anomalies_id, anomalies_description)

  # 2. outside buffer 100 m
  outside_buffer <- anom_final %>%
    dplyr::select(
      cluster,
      outside_cluster_by_more_than_100_m,
      outside_cluster_by_more_than_100_m_number,
      instanceID,
      hhid) %>%
    dplyr::filter(outside_cluster_by_more_than_100_m) %>%
    dplyr::mutate(
      anomalies_id = 'hh_outside_cluster_more_than_100_m',
      anomalies_description =
        glue::glue(
          'hhid:{hhid} is outside original cluster:{cluster} with 100m buffer')) %>%
    dplyr::select(KEY = instanceID, anomalies_id, anomalies_description)

  # 3 cluster mismatch
  mismatch <- anom_final %>%
    dplyr::select(
      cluster,
      cluster_number_mismatch,
      outside_cluster_number,
      outside_cluster_by_more_than_100_m_number,
      instanceID,
      hhid) %>%
    dplyr::filter(cluster_number_mismatch) %>%
    dplyr::mutate(
      anomalies_id = 'hh_cluster_number_mismatch',
      anomalies_description =
        glue::glue(
          'hhid:{hhid} is in outside of original cluster:{cluster} and located in inaccurate cluster:{outside_cluster_number}')) %>%
    dplyr::select(KEY = instanceID,
                  anomalies_id,
                  anomalies_description)

  final <- dplyr::bind_rows(outside,outside_buffer,mismatch) %>%
    dplyr::mutate(form_id = form_id)
  return(final)
}
