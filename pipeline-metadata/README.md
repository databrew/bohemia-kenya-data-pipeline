## Pipeline Metadata

### Architecture

![img](https://lucid.app/publicSegments/view/55d4c491-a697-4274-b067-246f52973fb0/image.jpeg)

1. This architecture involves the interaction between to repos. `bohemia-kenya-data-pipeline` will act as the enabler on interacting with AWS and ODK where `bk` will be the one generating metadata

2. This pipeline will clone `bk:main` for each scheduled run

3. How do we handle library addition? 
If there is a new library that is added to `bk` `generate_metadata.R` or any `rmds` attached to it - do notify @atediarjo on what library should be added to the `renv.lock` file

4. How data is stored?
Metadata will be stored as a zip file in S3 for tracking purposes under databrew.org/metadata

5. How form is versioned?
For each scheduled runs, versioning will be based on `YYMMDD01` (for manual updates, it will be adding through the last suffixes `YYMMDD02`)


### Changelog

1. 12/8/2023: [Make changes to PK Individuals](https://bohemiakenya.slack.com/archives/C059Q4RU2CA/p1702015098497179?thread_ts=1702014794.698659&cid=C059Q4RU2CA)

2. 12/14/2023: Start date for pk typecast

3. Pivot longer on pk samples fix removal of duplicates and some code chunks: https://github.com/databrew/bk/pull/23

4. Fix sample time collection: https://github.com/databrew/bk/pull/24

5. Manually Refresh metadata to triggger action
