## Start with the official rocker image (lightweight Debian)
FROM rocker/tidyverse:4.2.1

# install preliminary requirements
RUN apt-get update -y\
  && apt-get install -y git\
  && apt-get install -y libudunits2-dev\
  && apt-get install -y libgdal-dev\
  && apt-get install -y libpoppler-cpp-dev\
  && apt-get install -y awscli

# run git clone
RUN git clone -b fit_metadata_with_pipeline https://github.com/databrew/bk.git /root/bk

# copy working directory
COPY . /root/bk/scripts/metadata

# use the bohemia kenya work directory
WORKDIR /root/bk/scripts/metadata

# use renv to replicate
RUN R -e "install.packages('renv')"
RUN R -e "renv::init(bare = TRUE)"
RUN R -e "renv::restore()"

# run all pipeline
CMD make pipeline
