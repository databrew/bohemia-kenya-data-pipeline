# ODK Forms Extraction
@author: Aryton Tediarjo (atediarjo@gmail.com)
@reviewedBy: Joe Brew

## About
This Github repository is used as a microservice for extracting forms (from selected projects) to be stored to S3. This microservice assumes databrew.org as the default server. 

### Reproduce Analysis Environment
This project uses [R renv](https://rstudio.github.io/renv/articles/renv.html) to act as a virtual environment and snapshots the library requirements and the version being used during the analysis. 

```zsh
# cd to your cloned project
cd ..relative_path/odk-form-extraction
```
In the project directory, run these following R commands:

```r
install.packages("renv")
renv::init(bare=TRUE)
renv::restore()
```

Once sync-ed with the project environment, you can start running the R scripts. All R-packages are being indexed into a Lockfile (renv.lock). To add more packages you can do the usual R installation and snapshot that information into the Lockfile.

```r
install.packages('your_new_package')
renv::snapshot()
```

## Getting Started with Docker
[Install Docker Desktop on Local Computer](https://docs.docker.com/desktop/)

### Build Docker Image
Use this [Dockerfile Reference](https://docs.docker.com/build/building/packaging/) to get familiar with Dockerfile. Once Dockerfile is built, run this command:

```zsh
docker build -t odk-form-extraction .
```

### Testing Docker Image Locally
Before pushing to Dockerhub, test it locally:

```zsh
docker run \
-e AWS_ACCESS_KEY=... \ 
-e AWS_SECRET_ACCESS_KEY=... \
-e AWS_SESSION_TOKEN=... \
-e AWS_REGION= 'us-east-1' \
-e ODK_CREDENTIALS_SECRETS_NAME='test/odk-credentials' \
odk-form-extraction
```

### Pushing to Dockerhub


