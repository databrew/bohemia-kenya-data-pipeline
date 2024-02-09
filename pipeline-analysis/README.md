# Kenya Analyses Portfolio
Results for BOHEMIA trial in Kenya

## Setting up Local Development:

1. Fork https://github.com/databrew/bohemia-kenya-data-pipeline.git to your workspace

2. Git clone repository forked repo

3. Create a R Project under `pipeline-analysis`

**REQUIRED**: To get your local notebook development to point towards the most recent dataset, set environment variable `PIPELINE_STAGE=production`. You can also set this up once by using `.Renviron` file, and add in the environment variable you would like to have as default

### Modifying Existing Analysis
To modify each notebooks, you can go into the analysis folders in the project directory. Each folder will have a `.qmd` file and you can edit accordingly.

### Adding New Analysis
Each folder will be a section in the analyses website. Here are the steps:
- Create a new folder with new analysis name
- To append to the website, edit contents in `_quarto.yml`

```
    contents:
      - section: New Analysis Name
        contents: new_analysis_name/*
```

### Other Folders
- `/data_prep`: Used for storing data processing scripts
- `/data`: Used for storing intermediary data

### Combining Everything
Note: This will only be required for deployment

```
make pipeline
```