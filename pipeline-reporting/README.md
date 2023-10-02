## Pipeline Reporting

To contribute to this pipeline:

1. Create an R project under `pipeline-reporting`

2. Restore R environment by restoring all the libraries under `renv.lock`

```r
renv::init(bare=TRUE)
renv::restore()
```

3. Create a new file called `.Renviron` under this project to have configuration set to the AWS DEV account

```yaml
PIPELINE_STAGE=develop
```

4. If you add a new library, make sure to index the library to the lockfile

```r
renv::snapshot()
```
