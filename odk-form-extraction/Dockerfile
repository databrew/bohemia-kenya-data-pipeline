## Start with the official rocker image (lightweight Debian)
FROM rocker/tidyverse:4.2.1

# install preliminary requirements
RUN apt-get update -y\
    && apt-get install -y git

# copy working directory
COPY . /root/odk-form-extraction

# use the bohemia kenya work directory
WORKDIR /root/odk-form-extraction

# use renv to replicate
RUN R -e "install.packages('renv')"
RUN R -e "renv::init(bare = TRUE)"
RUN R -e "renv::restore()"

# run all pipeline
CMD make pipeline
