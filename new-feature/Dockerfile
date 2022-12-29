## Start with the official rocker image (lightweight Debian)
FROM rocker/tidyverse:4.2.1

# install preliminary requirements
RUN apt-get update -y

# copy working directory
COPY . /new_feature
# use the bohemia kenya work directory
WORKDIR /new_feature

# use renv to replicate
RUN R -e "install.packages('renv')"
RUN R -e "renv::init(bare = TRUE)"
RUN R -e "renv::restore()"

# container run R script
CMD bash run.sh
