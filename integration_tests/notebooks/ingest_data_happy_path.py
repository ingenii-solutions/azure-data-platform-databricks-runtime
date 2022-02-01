# Databricks notebook source

from os import environ

from ingenii_data_engineering.dbt_schema import get_source
from ingenii_databricks.orchestration import ImportFileEntry
from ingenii_databricks.pipeline import add_to_source_table, archive_file, \
    create_file_table, remove_file_table

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

# Create the individual file table
archive_file(import_entry)
create_file_table(spark, import_entry, table0_schema)

# Add to the source table
add_to_source_table(spark, import_entry, table0_schema)
remove_file_table(spark, dbutils, import_entry)

# COMMAND ----------
# Check no NULL entries
count = spark.sql(f"SELECT * FROM {source}.{table0_name} WHERE " + " OR ".join(f"{col['name']} IS NULL" for col in table0_schema["columns"])).count()
assert count == 0

# COMMAND ----------
# Check all entries have been uploaded
raw_count = 0
with open(f"/dbfs/mnt/archive/{source}/{table0_name}/{file0_name}") as raw_file:
    for _ in raw_file.readlines():
        raw_count += 1

row_count = spark.sql(f"SELECT * FROM {source}.{table0_name}").count()

assert raw_count == row_count + 1  # Add for header row

# COMMAND ----------
# Reingest file, check merging
create_file_table(spark, import_entry, table0_schema)

# Add to the source table
add_to_source_table(spark, import_entry, table0_schema)
remove_file_table(spark, dbutils, import_entry)

row_count = spark.sql(f"SELECT * FROM {source}.{table0_name}").count()

assert raw_count == row_count + 1  # Add for header row
