## Start with the official rocker image (lightweight Debian)
FROM rocker/tidyverse:4.2.1

# install preliminary requirements
RUN apt-get update -y\
    && apt-get install -y git\
    && apt-get install -y libudunits2-dev\
    && apt-get install -y libgdal-dev\
    && apt-get install -y awscli\
    && apt-get install -y curl\
    && apt-get install -y gdebi-core

# copy working directory
COPY . /root/pipeline-analysis

# use the bohemia kenya work directory
WORKDIR /root/pipeline-analysis

# install quarto
ARG QUARTO_VERSION=1.3.433
RUN mkdir -p /opt/quarto/${QUARTO_VERSION}
RUN curl -o quarto-linux-amd64.deb -L https://github.com/quarto-dev/quarto-cli/releases/download/v${QUARTO_VERSION}/quarto-${QUARTO_VERSION}-linux-amd64.deb
RUN gdebi quarto-linux-amd64.deb

# use renv to replicate
RUN R -e "install.packages('renv', repos = c(CRAN = 'https://cloud.r-project.org'))"
RUN R -e "renv::init(bare = TRUE)"
RUN R -e "renv::restore()"

# run all pipeline
CMD make pipeline
