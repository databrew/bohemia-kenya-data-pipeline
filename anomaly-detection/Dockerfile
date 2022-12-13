## Start with the official rocker image (lightweight Debian)
FROM rocker/tidyverse:4.2.1

# install preliminary requirements
RUN apt-get update -y\
    && apt-get install -y git

# clone github repository
RUN git clone https://github.com/databrew/anomaly-detection.git /root/anomaly-detection

# use the bohemia kenya work directory
WORKDIR /root/anomaly-detection

# use renv to replicate
ENV RENV_VERSION 0.16.0
RUN R -e "install.packages('remotes', repos = c(CRAN = 'https://cloud.r-project.org'))"
RUN R -e "remotes::install_github('rstudio/renv@${RENV_VERSION}')"
RUN R -e "renv::init(bare = TRUE)"
RUN R -e "renv::restore()"

# container run R script
CMD make pipeline
