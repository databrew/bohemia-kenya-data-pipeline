# Bohemia Kenya Data Pipeline
Author: atediarjo@gmail.com

## About
An end-to-end ETL tools used for Bohemia Kenya project. 

Existing Data workflows ➜
1. ODK Form Extraction
2. Data Cleaning
3. Anomaly Identification

Each data processes above is represented as folders. Each folders will be configured with continuous integration process to our [DataBrew Dockerhub](https://hub.docker.com/search?q=databrewllc) using GitHub Actions. Each images stored in Dockerhub will then be captured by our [ETL Tool](https://github.com/databrew/ecs-data-workflow/tree/main) and deployed to our workflow in AWS.

## Contributing

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

### Understanding Folder Structure
Each folder is a microservice used for data orchestration. Each folder will have these 4 components:
```
|-- your-workflow-folder
        |-- renv.lock
        |-- Dockerfile
        |-- Makefile
        |-- R/
```
- `renv.lock`: This is the file used to create virtual environment in R. It snapshots your local session library and locks the library versioning to make sure full-reproducibility of data pipeline
- `Dockerfile`: This is the Dockerfile used for creating fresh container to run the data workflow. Docker is used to help deploy resources to fresh AWS instance
- `Makefile`: This file is a helper to streamline running R scripts (can be changed to .sh executable if needed)
- `R/`: This is where your Rscript lives

### Modifying Existing Workflow Folder

To modify existing foler, you can directly work inside the folder. Ideally, you would create an `.Rproj` on that folder to work in your local RStudio.

#### a. Reproducing Production R Environment using `Renv` in RStudio
Using this setting, your local R Environment will work in an isolated environment that resembles Production.

```R
install.packages("renv")
renv::init(bare = TRUE)
renv::restore()
```

#### b. Adding new library to `Renv` virtual environment
For every new libraries used, you will be required to record the changes to the `renv.lock`. Let's use installing R Lubridate for example:

```R
install.packages("lubridate")
renv::snapshot()
```

#### c. Adding new R scripts
To streamline running several R scripts, edit the `Makefile`/`bash` files and add the new R Scripts

### Adding New Workflow Folder


## Bug Reports
Bugs reports will be done via [Github Issues](https://github.com/databrew/bohemia-kenya-data-pipeline/issues)
