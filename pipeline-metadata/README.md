## Pipeline Metadata

### How to contribute:

1. Create a new branch / fork this repository

2. **Open this folder as a project / or as the working directory**

3. Restore environment by running:

```r
install.packages('renv')
renv::restore()
```

Note: `renv` may not be able to fully restore environment depending on your locale configuration. 

4. Rerun all code via Terminal

```
make pipeline
```

5. (Optional) To add a new R library into the workflow, do `renv` snapshot if you are able to work under `renv` project. If not, mark it in the PR the new library you are using

```
renv::snapshot()
```
