# Databricks notebook source

from os import environ

from ingenii_data_engineering.dbt_schema import get_source
from ingenii_databricks.enums import Stage
from ingenii_databricks.orchestration import ImportFileEntry
from ingenii_databricks.pipeline import archive_file, create_file_table
from ingenii_databricks.table_utils import read_file, SchemaException

# COMMAND ----------

dbt_root_folder = environ["DBT_ROOT_FOLDER"]
source = "test_data"

source_details = get_source(dbt_root_folder, source)

# COMMAND ----------
# Create normal file table

table0_name = "table0"
table0_schema = source_details["tables"][table0_name]
file0_name = "file0.csv"

import_entry = ImportFileEntry(spark, source_name=source,
                               table_name=table0_name, file_name=file0_name,
                               increment=0)

if import_entry.is_stage(Stage.NEW):
    archive_file(import_entry)
    import_entry.update_status(Stage.ARCHIVED)

n_rows = create_file_table(spark, import_entry, table0_schema)

# COMMAND ----------
# Catch when there are extra columns in the file

table_name = "table2"
table_schema = source_details["tables"][table_name]
file_name = "file2.csv"

exception = False
try:
    read_file(spark, f"/mnt/raw/{source}/{table_name}/{file_name}", table_schema)
except SchemaException:
    exception = True

assert exception
