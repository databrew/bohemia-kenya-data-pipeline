# ODK Form Extraction

This pipeline section covers 2 data workflow: - Development workflow: databrew.org - Production workflow: TBD Kwale Servers

## Prerequisites

-   AWS Access Keys and Secret Access Keys (contact [atediarjo\@gmail.com](mailto:atediarjo@gmail.com){.email})
-   ODK Server Username and Password

## Steps to use in provisioned server

### 1. Install Docker

Use this documentation to install Docker in your [Server/VMs](https://docs.docker.com/engine/install/)

### 2. Docker Pull

``` bash
docker pull databrewllc/odk-form-extraction::production
```

### 3. Docker Run

Before you run the docker container, you will be required to manually create credentials & environment files in dedicated server

```bash
vim ~/.databrew_kwale_key_vars
```

Paste the variables into the hidden files in home directory

```bash
ODK_USERNAME=yourmail@mail.com
ODK_PASSWORD=yourodkpassword
AWS_ACCESS_KEY_ID=awsaccesskeyid
AWS_SECRET_ACCESS_KEY=awssecretaccesskey
AWS_DEFAULT_REGION=us-east-1
```
Note: No need to put quotes or double quotes or spacing

```bash
docker run --env-file ~/.databrew_kwale_key_vars databrewllc/odk-form-extraction:production
```

### 4. Create Scheduler via Cron

### 5. Add Docker Execute to Cron Job

## Bugs & Feature Request
