# Ingenii Databricks Platform

[![Maintainer](https://img.shields.io/badge/maintainer%20-ingenii-orange?style=flat)](https://ingenii.dev/)
[![License](https://img.shields.io/badge/license%20-MPL2.0-orange?style=flat)](https://github.com/ingenii-solutions/terraform-azurerm-key-vault/blob/main/LICENSE)
[![Contributing](https://img.shields.io/badge/howto%20-contribute-blue?style=flat)](https://github.com/ingenii-solutions/data-platform-databricks-runtime/blob/main/CONTRIBUTING.md)

## Details
* Base image: [databricksruntime/standard:7.x](https://hub.docker.com/layers/databricksruntime/standard/7.x/images/sha256-0d51d36c7b927858757fdc828c6a9fd5375b98ffcb186324060d0b334f5149d3?context=explore)
* Registry: ingeniisolutions
* Repository: databricks-runtime
* Current Version: 0.4.0

## Overview

This image is used in as a Databricks runtime in our Data Platform. This contains an installation of [dbt](https://www.getdbt.com/) and Ingenii's python package for data engineering.

## Usage

This image is used with Databricks' [Container Services](https://docs.databricks.com/clusters/custom-containers.html) to customise the cluster runtime.

## Data pipeline overview

The data pipeline goes through 6 stages: new, staged, archived, cleaned, inserted, and completed. Once each stage is complete the datetime is added to the relevant column in `orchestration.import_file` to show that stage is complete.

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

## dbt

A dbt project should be in the `dbfs/mnt/dbt` folder, which is where we draw information about the data sources and the data quality tests to apply. 
All data we want the platform to know about should be recorded as [dbt sources](https://docs.getdbt.com/docs/building-a-dbt-project/using-sources), where we specify each of the tables and their schemas in one or more `schema.yml` file. An example file would be

```
version: 2

sources:
  - name: random_example
    schema: random_example
    tables:
      - name: alpha
        external:
          using: "delta"
        join:
          type: merge
          column: "date"
        file_details:
          sep: ","
          header: false
          dateFormat: dd/MM/yyyy
        columns:
          - name: date
            data_type: "date"
            tests: 
              - unique
              - not_null
          - name: price1
            data_type: "decimal(18,9)"
            tests: 
              - not_null
          - name: price2
            data_type: "decimal(18,9)"
            tests: 
              - not_null
          - name: price3
            data_type: "decimal(18,9)"
            tests: 
              - not_null
          - name: price4
            data_type: "decimal(18,9)"
            tests: 
              - not_null
```
### dbt Schema: Sources
Each source must have the following keys:
  1. `name`: The name of the source internal to dbt
  1. `schema`: The schema to load the tables to in the database. Keep this the same as the name
  1. `tables`: A list of each of the tables that we will ingest

### dbt Schema: Tables
Each table within a source must have the following keys:
  1. `name`: The name of the table
  1. `external`: This sets that this is a delta table, and is stored in a mounted container. Always include this object as it is here.
  1. `join`: object to define how we should add new data to the main source table. The main way will be as above, where we merge into the table to avoid duplicate rows, and specify the 'date' column to merge on. The 'column' entry will accept a comma-separated list of column names, if more than one forms the primary key
  1. `file_details`: Gives general information about the source file to help read it. These entries are passed to, and so must follow the conventions of, either the [pyspark.sql.DataFrameReader.csv](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.csv.html) or [pyspark.sql.DataFrameReader.json](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.json.html) function; see the 'type' entry below. 'path' and 'schema' are set separately, so do not set these or the 'inferSchema' and 'enforceSchema' parameters. Some example parameters are below:
      1. `type`: This relates to either the raw file, or if there is a pre-processing script for this table the processed file, to give you space to pre-process the raw file as needed. If this is set to `json`, then the [pyspark.sql.DataFrameReader.json](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.json.html) function is used; if set to `csv` or not set at all, then the [pyspark.sql.DataFrameReader.csv](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.csv.html) function is used.
      1. `sep`: When reading the source files, this is the field separator. For example, in comma-separated values (.csv), this is a comma ','
      1. `header`: boolean, whether the source files have a header row
      1. `dateFormat`: The format to convert from strings to date types. 
      1. `timestampFormat`: The format to convert from strings to datetimes types. 
  1. `columns`: A list of all the columns of the file. Schema is detailed in the section below

### dbt Schema: Columns
For each table all columns need to be specified, and each must have the following keys: 
  1. `name`: The name of the column
  1. `data_type`: The data type we expect the column to be, using [Databricks SQL data types](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/spark-sql/language-manual/sql-ref-datatypes#sql)
  1. `tests`: A list of any [dbt tests](https://docs.getdbt.com/docs/building-a-dbt-project/tests) that we want to apply to the column on ingestion

## Mounted containers

The data pipeline looks to move data between several containers mounted from cloud storage. The mount points follow the pattern `/dbfs/mnt/<name>`:
1. `raw`: Files from your data sources to be ingested should appear here, at the location `/dbfs/mnt/raw/<source name, e.g. Bloomberg>/<table name>/<filename>`. The filename should be unique within the source and table name to separate individual files to be ingested, for example daily files should have a date in the file name.
1. `archive`: Raw files are moved here to a matching folder path to the raw container. This for any pre-processing required, ingesting the file to a staging Delta table, and long-term storage.
1. `source`: Hosts the files for the Delta tables of the data you've ingested. 
1. `orchestration`: Hosts the Import File Delta table, which contains details about each file ingestion, both to keep a history and to drive the data pipeline code.
1. `dbt`: Contains the dbt configuration that holds source table information and tests.
1. `dbt-logs`: Long-term storage for dbt logs generated.
1. `utilities`: Holds scripts, or any files, that users want to access on both the Databricks environment and the data lake.

## dbt-spark

As part of our development, we discovered an issue in [dbt-spark](https://github.com/dbt-labs/dbt-spark) around retrying when connections fail. At time of writing we have [opened a pull request for this fix](https://github.com/dbt-labs/dbt-spark/pull/194), but it is not yet merged. In the meantime, we contain in this repository our own version of dbt-spark (version 0.19.1) in the `dbt-spark` folder, which we install as part of the container runtime creation.

If we upgrade the version of dbt used in the container while this fix is not merged we will have to upgrade our version of dbt-spark to match, which will involve updating the dbt-spark folder and re-applying our fix.

## Versions Matrix

| Image Version | Databricks Runtime | Added Packages |
| --- | --- | --- |
| 0.4.0 | 7.x | dbt_core = 0.19.1 <br> dbt_pyspark = 0.19.1 (custom) <br> ingenii_databricks = 0.3.3 <br> ingenii_data_engineering = 0.2.0 |
| 0.3.4 | 7.x | dbt_core = 0.19.1 <br> dbt_pyspark = 0.19.1 (custom) <br> ingenii_databricks = 0.3.3 <br> ingenii_data_engineering = 0.1.5 |
| 0.3.3 | 7.x | dbt_core = 0.19.1 <br> dbt_pyspark = 0.19.1 (custom) <br> ingenii_databricks = 0.3.3 <br> ingenii_data_engineering = 0.1.4 |
| 0.3.2 | 7.x | dbt_core = 0.19.1 <br> dbt_pyspark = 0.19.1 (custom) <br> ingenii_databricks = 0.3.2 <br> ingenii_data_engineering = 0.1.3 |
| 0.3.1 | 7.x | dbt_core = 0.19.1 <br> dbt_pyspark = 0.19.1 (custom) <br> ingenii_databricks = 0.3.1 <br> ingenii_data_engineering = 0.1.0 |
| 0.3.0 | 7.x | dbt_core = 0.19.1 <br> dbt_pyspark = 0.19.1 (custom) <br> ingenii_databricks = 0.3.0 |
| 0.2.0 | 7.x | dbt_core = 0.19.1 <br> dbt_pyspark = 0.19.1 (custom) <br> ingenii_databricks = 0.2.0 |
| 0.1.0 | 7.x | dbt_core = 0.19.1 <br> dbt_pyspark = 0.19.1 <br> ingenii_databricks = 0.1.0 |
| 0.0.1 | 7.x | dbt_core = 0.19.1 <br> dbt_pyspark = 0.19.1 |
