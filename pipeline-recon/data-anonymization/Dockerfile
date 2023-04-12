## Start with the official rocker image (lightweight Debian)
FROM rocker/tidyverse:4.2.1

# install updates for docker container
RUN apt-get update -y

# clone github repository
COPY . /root/data-anonymization

# use the bohemia kenya work directory
WORKDIR /root/data-anonymization

# use renv to replicate
RUN R -e "install.packages('renv')"
RUN R -e "renv::init(bare = TRUE)"
RUN R -e "renv::restore()"

# container run R script
CMD make pipeline
