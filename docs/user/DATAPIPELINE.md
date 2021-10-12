# Data Ingestion Pipeline

The data pipeline used on the Ingenii Data Platform goes through 6 stages: new, archived, staged, cleaned, inserted, and completed. Once each stage is complete the timestamp is added to the relevant column in `orchestration.import_file` to show that stage is complete. This means that in the rare case of a failure, restarting the pipeline for a particular file's ingestion will pick up where the previous pipeline left off, rather than restarting the entire process.

1. `New`:
    1. If there isn't one already, an entry is created on the `orchestration.import_file` table to represent this particular file.
    1. As part of the entry, a unique hash is created using the combination of the source name, table name, and the file name. Later in the `Inserted` stage this hash is added to an extra column on the final tables so that data can always be tracked back to its source file.
1. `Archived`:
    1. Move the raw file to its respective `archive` path for long-term storage and to avoid any processing in the `raw` container.
1. `Staged`:
    1. If there is a pre-processing function allocated to the specific table, then the original file is moved to `archive/<source name>/<table name>/before_pre_process`, the pre-processing function applied, and the processed file takes the original's place at `raw/<source name>/<table name>`. Please see the [Example Data Engineering repository](https://github.com/ingenii-solutions/azure-data-platform-data-engineering-example) for more details about pre-processing scripts.
    1. Create a table to host the data for this individual file called `<source name>.<table name>_<hash>`. We set the schema using the definition set in the dbt schema.yml files.
    1. Then, data from the file is loaded to this table.
1. `Cleaned`:
    1. Next, we run the tests defined in dbt against this table.
    1. If there are failing rows these are moved to a new table called `<source name>.<table name>_<hash>_<incremented number>`, and a new `orchestration.import_file` entry is created to represent this with a matching 'increment' value.
1. `Inserted`:
    1. Clean data from table `<source name>.<table name>_<hash>` is merged to main source table `<source name>.<table name>`
1. `Completed`:
    1. We delete the staged file table `<source name>.<table name>_<hash>`, as the data has been successfully moved

## Mounted containers

The data pipeline looks to move data between several containers mounted from cloud storage. The mount points follow the pattern `/dbfs/mnt/<name>`:
1. `raw`: Files from your data sources to be ingested should appear here, at the location `/dbfs/mnt/raw/<source name, e.g. Bloomberg>/<table name>/<filename>`. The filename should be unique within the source and table name to separate individual files to be ingested, for example daily files should have a date in the file name.
1. `archive`: Raw files are moved here to a matching folder path to the raw container. This for any pre-processing required, ingesting the file to a staging Delta table, and long-term storage.
1. `source`: Hosts the files for the Delta tables of the data you've ingested. 
1. `orchestration`: Hosts the Import File Delta table, which contains details about each file ingestion, both to keep a history and to drive the data pipeline code.
1. `dbt`: Contains the dbt configuration that holds source table information and tests.
1. `dbt-logs`: Long-term storage for dbt logs generated.
1. `utilities`: Holds scripts, or any files, that users want to access on both the Databricks environment and the data lake.
