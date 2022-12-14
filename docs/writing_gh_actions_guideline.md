# Writing Github Actions for CI/CD

Github Actions are used to trigger build and deployment to Dockerhub, an example of Github Action can be seen in this template:

## Template

```yaml
name: dockerhub-ci-odk-form-extraction

on:
  push:
    branches:
      - 'main'
    paths:
      - <your-folder-name>/**

jobs:
  docker:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: ./<your-folder-name>
    steps:
      - name: Set up QEMU
        uses: docker/setup-qemu-action@v2
      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v2
      - name: Login to Docker Hub
        uses: docker/login-action@v2
        with:
          username: ${{ secrets.DOCKERHUB_USERNAME }}
          password: ${{ secrets.DOCKERHUB_TOKEN }}
      - name: Checkout Repository
        uses: actions/checkout@v3
      - name: Build Dockerfile
        run: docker build -f Dockerfile --progress=plain -t databrewllc/<your_folder_name>:latest .
      - name: Push to Dockerhub
        run: docker push databrewllc/<your_folder_name>:latest

```
1. This `yaml` file will trigger whenever CI/CD whenever you push new changes in <your_folder_name> to the `main` branch. 

2. In a fresh shell, it will log in to Dockerhub using Github Secrets set in this github repository to enable `docker push`. 

3. Then, it copies the content of your folder and builds the environment required to build and push your changes to `Dockerhub`. 

For new workflow folders, you will be required to manually create this .yaml file and save it under `.github/workflows/` folder to enable CI/CD of the new workflow. **Your action item is to change the folder name into your new folder name based on the template.**

