# Databricks notebook source

from ingenii_data_engineering.dbt_schema import get_source
from ingenii_databricks.enums import Stage
from ingenii_databricks.orchestration import ImportFileEntry

# COMMAND ----------
# Create normal file table

source = "test_data"
table_name = "status_updates"
file_name = "example1.csv"

raw_folder = f"/mnt/raw/{source}/{table_name}"

dbutils.fs.mkdirs(raw_folder)
dbutils.fs.cp(f"/mnt/archive/{source}/table0/file0.csv",
              f"{raw_folder}/{file_name}")

import_entry = ImportFileEntry(
    spark,
    source_name=source, table_name=table_name, file_name=file_name,
    increment=0)

# COMMAND ----------

for curr_stage, next_stage in zip(Stage.ORDER[:-1], Stage.ORDER[1:]):
    assert import_entry.get_current_stage() == curr_stage
    assert import_entry.is_stage(curr_stage)

    import_entry.update_status(next_stage)
