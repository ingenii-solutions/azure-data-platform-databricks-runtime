# Databricks notebook source

from os import environ

from ingenii_data_engineering.dbt_schema import get_source
from ingenii_databricks.orchestration import ImportFileEntry
from ingenii_databricks.pipeline import archive_file, create_file_table, \
    prepare_individual_table_yml, move_rows_to_review, \
    revert_individual_table_yml, test_file_table
from ingenii_databricks.table_utils import read_file

# COMMAND ----------

dbt_root_folder = environ["DBT_ROOT_FOLDER"]
log_target_folder = environ["DBT_LOGS_FOLDER"]
source = "test_data"

source_details = get_source(dbt_root_folder, source)

# COMMAND ----------
# Create normal file table

table_name = "table1"
table_schema = source_details["tables"][table_name]
file_name = "file1.csv"
hash_str = "1767481205"

import_entry = ImportFileEntry(spark, source_name=source,
                               table_name=table_name, file_name=file_name,
                               increment=0)

# Create the individual file table
archive_file(import_entry)
create_file_table(spark, import_entry, table_schema)

# COMMAND ----------
# Test and move the problem rows
prepare_individual_table_yml(table_schema["file_name"], import_entry)

# Run tests and analyse the results
databricks_dbt_token = \
    dbutils.secrets.get(scope=environ["DBT_TOKEN_SCOPE"],
                        key=environ["DBT_TOKEN_NAME"])
testing_result = \
    test_file_table(import_entry, databricks_dbt_token,
                    dbt_root_folder, log_target_folder)

assert not testing_result["success"]

revert_individual_table_yml(table_schema["file_name"])

# If bad data, entries in the column will be NULL
print("Errors found while testing:")
for error_message in testing_result["error_messages"]:
    print(f"    - {error_message}")

assert testing_result["error_sql_files"]
move_rows_to_review(
    spark, import_entry, table_schema,
    dbt_root_folder, testing_result["error_sql_files"])

print(f"Rows with problems have been moved to review table "
      f"{import_entry.get_full_review_table_name()}")

# COMMAND ----------
# Get the row counts from files
raw_count, raw_count_clean, raw_count_problems = 0, 0, 0
file_paths = {
    "raw": f"/mnt/archive/{source}/{table_name}/{file_name}",
    "clean": f"/mnt/raw/{source}/{table_name}-other/file1-clean.csv",
    "problems": f"/mnt/raw/{source}/{table_name}-other/problems.csv",
}

with open("/dbfs" + file_paths["raw"]) as raw_file:
    for _ in raw_file.readlines():
        raw_count += 1
with open("/dbfs" + file_paths["clean"]) as raw_file:
    for _ in raw_file.readlines():
        raw_count_clean += 1
with open("/dbfs" + file_paths["problems"]) as raw_file:
    for _ in raw_file.readlines():
        raw_count_problems += 1

# COMMAND ----------
acceptable_table = spark.sql(f"SELECT * FROM {source}.{table_name}_{hash_str}")
problems_table = spark.sql(f"SELECT * FROM {source}.{table_name}_{hash_str}_1")

assert raw_count == acceptable_table.count() + raw_count_problems  # Header row on both sides, so no need to + 1
assert raw_count_clean == acceptable_table.count() + 1  # + 1 for header row
assert raw_count_problems == problems_table.count() + 1  # + 1 for header row

# COMMAND ----------
# Check problems data is the same as the raw file
problems_data = read_file(spark, file_paths["problems"], table_schema)

assert problems_data.schema == acceptable_table.schema
assert problems_data.schema == problems_table.schema

assert problems_data.collect() == problems_table.collect()

# COMMAND ----------
# Check clean data is the same as the raw file
clean_data = read_file(spark, file_paths["clean"], table_schema)

assert clean_data.schema == acceptable_table.schema

assert clean_data.collect() == acceptable_table.collect()
