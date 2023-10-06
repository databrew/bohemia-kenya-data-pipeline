## Pipeline Reporting

To contribute to this pipeline:

1. Clone repository

```bash 
git clone https://github.com/databrew/bohemia-kenya-data-pipeline.git
```

2. Create a new branch from `main` - make sure to name it with `{your_git_username}:{task_name}`

Create new branch:
```
git checkout -b {your_git_username}:{task_name}
```

Push branch to remote:
```
git push origin {your_git_username}:{task_name}
```

3. Create an R project under `pipeline-reporting`

4. Restore R environment by restoring all the libraries under `renv.lock`

```r
renv::init(bare=TRUE)
renv::restore()
```

5. Create a new file called `.Renviron` under this project to have configuration set to the `AWS DEV` account

```yaml
PIPELINE_STAGE=develop
```

If you want to take data directly from `AWS PROD` Account:

```yaml
PIPELINE_STAGE=production
```


6. Create reports under `R/` - each folder under this will contain each reporting persona. You can add `.rmd` under these folders based on your use case.  

7. If you want add a new library, make sure to index the library to the lockfile

```r
renv::snapshot()
```

8. Once done, create a [PR](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request) to `dev` and tag `arytontediarjo` and `joebrew` as reviewer

9. Once approved (1 approval needed) branch will be pushed to `dev` for CI / CD testing


## FAQs

**What if I cannot fully restore the environment using `renv.lock`?**
Fully restoring an environment can be a case by case basis, depending on the OS and your system configuration. If restoration is not possible, make sure that you are able to `knit` the report and send a PR for further inspection.

**How does the DEV and PROD account different?**
DEV account is an exact replica of PROD in terms of the stacks being deployed. However, DEV Account does not refresh data on a schedule - a manual trigger is required.

