## Pipeline Reporting

To contribute to this pipeline:

1. Create a new branch from `main` - make sure to name it with `{your_git_username}:{task_name}`

2. Create an R project under `pipeline-reporting`

3. Restore R environment by restoring all the libraries under `renv.lock`

```r
renv::init(bare=TRUE)
renv::restore()
```

4. Create a new file called `.Renviron` under this project to have configuration set to the AWS DEV account

```yaml
PIPELINE_STAGE=develop
```

5. Create reports under `R/` - each folder under this will contain each reporting persona. You can add `.rmd` under these folders based on your use case.  

6. If you want add a new library, make sure to index the library to the lockfile

```r
renv::snapshot()
```

7. Once done, create a PR to `dev` and tag `arytontediarjo` and `joebrew` as reviewer

8. Once approved (1 approval needed) branch will be pushed to `dev` for CI / CD testing

