# Writing Dockerfile

Docker is used to persist the analysis environment. For example, under each workflow folder there will be a Dockerfile with the following contents: 


## Template
```dockerfile
## Start with the official rocker image (lightweight Debian)
FROM rocker/tidyverse:4.2.1

# install preliminary requirements
RUN apt-get update -y\
    && apt-get install -y git

# copy working directory
COPY . /<your_folder_name> # change this to your folder

# use the bohemia kenya work directory
WORKDIR /<your_folder_name> # change this to your folder

# use renv to replicate
ENV RENV_VERSION 0.16.0
RUN R -e "install.packages('remotes', repos = c(CRAN = 'https://cloud.r-project.org'))"
RUN R -e "remotes::install_github('rstudio/renv@${RENV_VERSION}')"
RUN R -e "renv::init(bare = TRUE)"
RUN R -e "renv::restore()"

# container run R script
CMD make pipeline # or can use bash script as an alternative
```

1. Create base `tidyverse image to enable dplyr / tidyverse-related libraries` in image
2. Run some installation in the image
3. Copy folder contents into image
4. Set working directory to the copied folder in image
5. Install `Renv` in image
6. Use `renv` to restore environment in image
7. Run RScript using `Makefiles` when container starts

You will be required to create this Dockerfile into the workflow folder. The only portions you need to change is on the `CMD` portion of the Dockerfile and the corresponding folder names. You are free to use Makefiles or Bash script to run several R scripts from your workflow.

## Docker Commands Reference
`FROM`: Inherit from another image, in our case we are inheriting our environment from `rocker/tidyverse` where it has the working base-image used for working with tidyverse

`RUN`: Image build step, this is where you do all the installation required in your image

`COPY`: Copy contents of the folder to the Docker image

`WORKDIR`: This will define your working directory for the Docker image

`ENV`: Set image environment variable

`CMD`: This is command that gets executed after image is built and running as a container