source('R/utils.R')

get_fake_assignments <- function() {
  structure(list(cluster_number = c(1, 12, 20, 34, 35, 56, 72),
                 location = c("North", 'North',
                              "North", "North", "North", "North", "South"),
                 assignment = c(1, 2, 2, 1, 2, 1, 1)),
            row.names = c(NA,
                          -7L),
            class = c("tbl_df", "tbl", "data.frame"))
}
