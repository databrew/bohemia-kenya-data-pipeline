# ODK Form Extraction

This pipeline section covers 2 data workflow:

- 1. Development workflow: databrew.org
- 2. Production workflow: Kwale Servers

## Production Workflow 

### Prerequisites

- READ/WRITE AWS Access Keys and Secret Access Keys (contact [atediarjo\@gmail.com](mailto:atediarjo@gmail.com){.email})
- ODK Server
- Git
- Docker

### 1. Clone Repository

```bash
git clone https://github.com/databrew/bohemia-kenya-data-pipeline.git
```

### 2. Install Docker

```bash
(sudo) apt-get install docker.io
```

Use this documentation to install Docker in your [Server/VMs](https://docs.docker.com/engine/install/) if you are using other Operating Machine

### 3. Build Configuration Files

Before running Doc, you will be required to manually create a hidden file containing the credentials & environment variables that will be fed into the Docker Container. You can use [vim](https://www.tutorialspoint.com/vim/vim_installation_and_configuration.html) or any text editor of choice to edit the environment variables

``` bash
vim ~/.databrew_kwale_key_vars
```

Paste the variables into the hidden files in home directory

``` bash
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

### 4. Run bash script to enable Docker and Cron Scheduler

Go to where you 

``` bash
cd bohemia-kenya-data-pipeline/odk-form-extraction
bash create_cron_job.sh
```

## Bugs & Feature Request

For bugs and feature requests, please post an issue to [Github Issue](https://github.com/databrew/bohemia-kenya-data-pipeline/issues) and notify atediarjo@gmail.com
