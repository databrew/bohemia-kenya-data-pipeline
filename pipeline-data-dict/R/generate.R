library(dplyr)
library(gsheet)

index <- gsheet::gsheet2tbl('https://docs.google.com/spreadsheets/d/10fsAAnARFzLqn5OVHgVhxIfPkmplrgir7n7BOGLyhfI/edit#gid=456395473') %>%
  dplyr::filter(status != 'deprecated')

dir.create('pdfs')
i = 3

# index <- index %>% filter(!is.na(choices_url))

for(i in 1:nrow(index)){
  try({
    this_name <- index$`form.full.name`[i]
    this_id <- index$`form.ID`[i]
    url_survey <- index$google_url[i]
    url_choices <- index$choices_url[i]
    # read the xls
    survey <- gsheet::gsheet2tbl(url_survey)
    choices <- gsheet::gsheet2tbl(url_choices)
    # Clean up the survey
    survey <- survey[,1:3]
    names(survey) <- c('type', 'name', 'label')
    survey$list_name <- ifelse(grepl('select', survey$type),
                               gsub('select_one |select_multiple ', '', survey$type),
                               NA)
    # Clean up the choices
    choices <- choices[,1:3]
    names(choices) <- c('list_name', 'name', 'label')
    out <- choices %>%
      filter(!is.na(list_name)) %>%
      filter(!is.na(name)) %>%
      # mutate(name_label = paste0(name, ':', label)) %>%
      mutate(name_label = paste0(name)) %>%
      group_by(list_name) %>%
      summarise(options = paste0(name_label, collapse = '; '))
    # Join the choices to the survey
    survey <- survey %>%
      left_join(out)
    # Remove the "group" variables
    survey <- survey %>%
      filter(!grepl('group', type))
    # Remove list_name
    survey$list_name <- NULL
    # Rename variables
    names(survey) <- c('type', 'variable', 'question', 'choices')
    # Remove notes
    survey <- survey %>%
      filter(type != 'note') %>%
      # remove calculates
      filter(type != 'calculate') %>%
      # remove repeats
      filter(!grepl('repeat', type))
    # Keep only first word
    survey$type <- unlist(lapply(strsplit(survey$type, split = ' '), function(x){x[1]}))

    # Pass this over to the rmarkdown
    table_kable <- survey %>% kable("simple")
    output_filename <- glue('docs/kwale_{this_id}.Rmd')
    writeLines(table_kable,output_filename)


  })
  Sys.sleep(5)
}
