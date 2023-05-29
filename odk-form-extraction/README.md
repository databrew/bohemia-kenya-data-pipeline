# ODK Form Extraction

This pipeline section covers 2 data workflow:

- Development workflow: databrew.org
- Production workflow: Kwale Servers

## Production Workflow 

### Prerequisites

- READ/WRITE AWS Access Keys and Secret Access Keys to DataBrew Prod Account
- ODK Server
- ODK Project with name `project_forms_production`
- Docker

### 1. Create a ODK Project with name `project_forms_production`
This is where the script will fetch the ODK Forms and crawl each XLS Forms

### 2. Clone Repository

```zsh
git clone https://github.com/databrew/bohemia-kenya-data-pipeline.git
```

### 3. Install Docker

```zsh
(sudo) apt-get install docker.io
```

Use this documentation to install Docker in your [Server/VMs](https://docs.docker.com/engine/install/) if you are using other Operating Machine

### 4. Build Configuration Files

Before running Doc, you will be required to manually create a hidden file containing the credentials & environment variables that will be fed into the Docker Container. You can use [vim](https://www.tutorialspoint.com/vim/vim_installation_and_configuration.html) or any text editor of choice to edit the environment variables

```zsh
vim ~/.databrew_kwale_key_vars
```

Paste the variables into the hidden files in home directory

```zsh
PIPELINE_STAGE=production
ODK_SERVER_ENDPOINT=https://yourodkserver
ODK_USERNAME=yourmail@mail.com
ODK_PASSWORD=yourodkpassword
AWS_ACCESS_KEY_ID=awsaccesskeyid
AWS_SECRET_ACCESS_KEY=awssecretaccesskey
AWS_DEFAULT_REGION=us-east-1
```

Note: 

- No need to put quotes or double quotes or spacing
- Access Key and Secret Access Key will be provided by DataBrew Team

### 5. Run bash script to enable Docker and Cron Scheduler

Go to the directory where you stored the cloned repo, and change go into the odk-form-extraction folder and run the Docker & Cron setup by running:

```zsh
cd bohemia-kenya-data-pipeline/odk-form-extraction
bash create_cron_job.sh
```

## Bugs & Feature Request

For bugs and feature requests, please post an issue to [Github Issue](https://github.com/databrew/bohemia-kenya-data-pipeline/issues) and notify atediarjo@gmail.com
