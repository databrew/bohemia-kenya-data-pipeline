## Start with the official rocker image (lightweight Debian)
FROM rocker/tidyverse:4.2.1

# install preliminary requirements
RUN apt-get update -y\
    && apt-get install -y git

# clone github repository
COPY . /root/anomaly-detection

# use the bohemia kenya work directory
WORKDIR /root/anomaly-detection

# use renv to replicate
RUN R -e "install.packages('renv')"
RUN R -e "renv::init(bare = TRUE)"
RUN R -e "renv::restore()"

# container run R script
CMD make pipeline
