# function to pad hhid
pad_hhid <- function(data){
  if('hhid' %in% names(data)){
    data %>%
      dplyr::mutate(hhid = stringr::str_pad(hhid, 5, pad = "0"))
  }else{
    data
  }
}

wrap_download <- function(reactable_obj, element_id, output_filename){
  onclick_command <- glue::glue(
    "Reactable.downloadDataCSV('{element_id}', '{output_filename}')")
  htmltools::browsable(
    tagList(
      tags$button(
        tagList(fontawesome::fa("download"), "Download as CSV"),
        onclick = onclick_command),
      reactable_obj
    ))
}

plot_submission_by_day <- function(data) {
  submissions <- data %>%
    dplyr::mutate(metric_date = lubridate::date(SubmissionDate)) %>%
    dplyr::group_by(metric_date) %>%
    dplyr::summarise(n_submission = n_distinct(KEY))%>%
    dplyr::select(`date` = metric_date,
                  `submission` = n_submission)

  submissions_by_dow <- submissions %>%
    dplyr::mutate(dow = lubridate::wday(date, label = TRUE)) %>%
    dplyr::group_by(dow) %>%
    dplyr::summarise(submission = sum(submission))

  p1 <- submissions %>%
    ggplot(aes(x = `date`, y = `submission`)) +
    geom_line() +
    geom_point() +
    theme_minimal() +
    labs(
      y = 'Submissions',
      x = '')

  p2 <- submissions_by_dow %>%
    ggplot(aes(x = `dow`, y = `submission`)) +
    geom_col() +
    theme_minimal() +
    labs(
      y = 'Submissions',
      x = '')

  subplot(ggplotly(p1), ggplotly(p2))
}
