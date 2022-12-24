# Bohemia Kenya Data Pipeline
Author: atediarjo@gmail.com

## Docker CI/CD Status
![example workflow](https://github.com/databrew/bohemia-kenya-data-pipeline/actions/workflows/dockerhub-ci-odk-form-extraction.yaml/badge.svg)
![example workflow](https://github.com/databrew/bohemia-kenya-data-pipeline/actions/workflows/dockerhub-ci-anomaly-detection.yaml/badge.svg)

## About
An end-to-end ETL tools used for Bohemia Kenya project. 

Existing Data workflows ➜
1. ODK Form Extraction
2. Data Cleaning
3. Anomaly Identification

Each data processes above is represented as folders. Each folders will be configured with continuous integration process to our [DataBrew Dockerhub](https://hub.docker.com/search?q=databrewllc) using GitHub Actions. Each images stored in Dockerhub will then be captured by our [ETL Tool](https://github.com/databrew/ecs-data-workflow/tree/main) and deployed to our workflow in AWS.

## Getting Started

![Image](images/gitflow.jpeg)
This date pipeline uses a 4-step process for creating new data workflow for Bohemia Kenya.
1. Clone and create feature branch
2. Make changes into workflow folder
3. Create a PR to `main` branch
4. Merging to `main` will trigger CI/CD using Github Actions

### Clone this repository
```
git clone https://github.com/databrew/bohemia-kenya-data-pipeline.git
```
### Create a feature branch
To make contribution to data workflow, start by creating a feature branch. 
```
git checkout -b [name_of_your_new_branch]
git push origin [name_of_your_new_branch]
```
*Preferred git branch naming convention: [email_handle]_[feature_name]*

### Folder Structure Requirements for Workflow Orchestration
Each folder is a microservice used for data orchestration. Each folder will have these 4 components:
```
|-- your-workflow-folder
        |-- renv.lock
        |-- Dockerfile
        |-- Makefile
        |-- R/
```
- `renv.lock`: This is the file used to create virtual environment in R. It snapshots your local session library and locks the library versioning to make sure full-reproducibility of data pipeline.
- `Dockerfile`: This is the Dockerfile used for creating fresh container to run the data workflow. Docker is used to help deploy resources to fresh AWS instance
- `Makefile`: This file is a helper to streamline running R scripts (can be changed to .sh executable if needed)
- `R/`: This is where your Rscript lives

## Modify Existing Docker Workflow

To work/modify on existing folder, you can directly work inside the folder. Create an individual `.Rproj` on each of the folder in RStudio. 

### Reproducing Production R Environment using `Renv` in RStudio
Once you are inside the folder, you will be able to reproduce the libaries used in production using following commands:

```R
install.packages("renv")
renv::init(bare = TRUE)
renv::restore()
```

### Adding new library to `Renv` virtual environment
Adding new libraries will require you to record the changes to the `renv.lock`. Let's use installing R `Lubridate` for example:

```R
install.packages("lubridate")
renv::snapshot()
```

Running this command will snapshot the lubridate package into the `renv.lock` library index

#### Adding new R scripts
To streamline running several R scripts, edit the `Makefile`/`bash` files and add the new R Scripts

## Creating New Docker Workflow
To create a new worfklow, start by creating a new folder under this repository.

### Create `R Project` in New Folder
```
mkdir [your_new_workflow_folder]
```
Once directory is created, create RProject using the new directory

### Work like you normally would in `R/`
You are free to design how your features look like and put your Rscripts under `R/` folder
### Persist your RStudio Environment and Libraries into renv
Once you are done with the new feature, save your environment into `renv.lock` by doing
```R
library(renv)
renv::init()
```

### Create a Dockerfile
Follow guidelines from [Writing Dockerfile](docs/writing_dockerfile_guideline.md)

### Add CI/CD to Dockerhub
Follow guidelines from [Writing GitHub Action for CI/CD](docs/writing_gh_actions_guideline.md)


## Bug Reports
For any bug reports, please submit via [Github Issues](https://github.com/databrew/bohemia-kenya-data-pipeline/issues)
