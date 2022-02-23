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

table_name = "table0"
table_schema = source_details["tables"][table_name]
file_name = "file0.csv"

import_entry = ImportFileEntry(spark, source_name=source,
                               table_name=table_name, file_name=file_name,
                               increment=0)
expected_table_name = f"{table_name}_{import_entry.hash}"

# COMMAND ----------
# Create the individual file table
archive_file(import_entry)
create_file_table(spark, import_entry, table_schema)

# Check file table created
found = False
for table_row in spark.sql(f"SHOW TABLES IN {source}").collect():
    if table_row.tableName == expected_table_name:
        found = True
        break
if not found:
    raise Exception(f"File table {source}.{expected_table_name} not created!")

# COMMAND ----------
# Add to the source table
add_to_source_table(spark, import_entry, table_schema)
remove_file_table(spark, dbutils, import_entry)

# Check file table removed
found = False
for table_row in spark.sql(f"SHOW TABLES IN {source}").collect():
    if table_row.tableName == expected_table_name:
        found = True
        break
if found:
    raise Exception(f"File table {source}.{expected_table_name} not removed!")

# COMMAND ----------
# Check no NULL entries
count = spark.sql(
    f"SELECT * FROM {source}.{table_name} WHERE " +
    " OR ".join(f"{col['name']} IS NULL" for col in table_schema["columns"])
).count()
assert count == 0

# COMMAND ----------
# Check all entries have been uploaded
raw_count = 0
with open(f"/dbfs/mnt/archive/{source}/{table_name}/{file_name}") as raw_file:
    for _ in raw_file.readlines():
        raw_count += 1

row_count = spark.sql(f"SELECT * FROM {source}.{table_name}").count()

assert raw_count == row_count + 1  # Add for header row
