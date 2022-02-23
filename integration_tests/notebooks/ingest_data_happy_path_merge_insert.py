# Databricks notebook source

from copy import deepcopy
from os import environ
from random import randint

from ingenii_data_engineering.dbt_schema import get_source
from ingenii_databricks.enums import MergeType
from ingenii_databricks.orchestration import ImportFileEntry
from ingenii_databricks.pipeline import add_to_source_table, archive_file, \
    create_file_table, remove_file_table

# COMMAND ----------

dbt_root_folder = environ["DBT_ROOT_FOLDER"]
source = "test_data"

source_details = get_source(dbt_root_folder, source)

source_table_name = "table0"
table_schema = deepcopy(source_details["tables"][source_table_name])
file_name = "file0.csv"

# COMMAND ----------
# Copy data

table_name = f"{source_table_name}_{MergeType.MERGE_INSERT}"
table_schema["join"]["type"] = MergeType.MERGE_INSERT

raw_folder = f"/mnt/raw/{source}/{table_name}"

dbutils.fs.mkdirs(raw_folder)
dbutils.fs.cp(f"/mnt/raw/{source}/{source_table_name}/{file_name}",
              f"{raw_folder}/{file_name}")


# COMMAND ----------
# Create normal file table

import_entry = ImportFileEntry(spark, source_name=source,
                               table_name=table_name, file_name=file_name,
                               increment=0)

# Create the individual file table
archive_file(import_entry)
create_file_table(spark, import_entry, table_schema)

# Add to the source table
add_to_source_table(spark, import_entry, table_schema)
remove_file_table(spark, dbutils, import_entry)

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

# COMMAND ----------
# Delete some data, change others
day_to_delete = randint(1, 31)
deleted_count = spark.sql(f"SELECT * FROM {source}.{table_name} WHERE day(date) = {day_to_delete}").count()
print(f"Deleting all entries which are the {day_to_delete} day of the month")
spark.sql(f"DELETE FROM {source}.{table_name} WHERE day(date) = {day_to_delete}")

changed_data = []
for col in table_schema["columns"][1:]:  # Don't change the date
    day_to_change = day_to_delete
    # Can't be the same
    while day_to_change == day_to_delete:
        day_to_change = randint(1, 31)

    print(f"Changing column {col['name']} for {day_to_change} day of month")
    if col["data_type"] == "boolean":
        set_clause = f"SET {col['name']} = NOT {col['name']} "
    elif col["data_type"] == "timestamp":
        set_clause = f"SET {col['name']} = {col['name']} + INTERVAL '10' MINUTE "
    else:
        set_clause = f"SET {col['name']} = {col['name']} + 10 "

    curr_data = spark.sql(f"SELECT * FROM {source}.{table_name} WHERE day(date) = {day_to_change}").collect()
    changes = {
        row["date"]: {
            "original": row[col["name"]]
        }
        for row in curr_data
    }
    spark.sql(f"UPDATE {source}.{table_name} {set_clause} WHERE day(date) = {day_to_change}")
    curr_data = spark.sql(f"SELECT * FROM {source}.{table_name} WHERE day(date) = {day_to_change}").collect()
    for row in curr_data:
        changes[row["date"]]["changed"] = row[col["name"]]

    changed_data.append({"column": col["name"], "day": day_to_change, "changes": changes})

# COMMAND ----------
# Store unchanged data

days_changed = [day_to_delete] + [data["day"] for data in changed_data]
unchanged_data = spark.sql(f"SELECT * FROM {source}.{table_name} WHERE day(date) NOT IN (" + ", ".join(days_changed) + ")").collect()

# COMMAND ----------
# Reingest file, check merging
create_file_table(spark, import_entry, table_schema)

# Add to the source table
add_to_source_table(spark, import_entry, table_schema)
remove_file_table(spark, dbutils, import_entry)

row_count = spark.sql(f"SELECT * FROM {source}.{table_name}").count()

assert raw_count == row_count + 1  # Add for header row

# COMMAND ----------
# Assert data that is supposed to be unchanged is unchanged
assert unchanged_data == spark.sql(f"SELECT * FROM {source}.{table_name} WHERE day(date) NOT IN (" + ", ".join(days_changed) + ")").collect()

# COMMAND ----------
# Explicitly check deleted lines restored

row_count = spark.sql(f"SELECT * FROM {source}.{table_name} WHERE day(date) = {day_to_delete}").count()
assert raw_count == deleted_count

# COMMAND ----------
# Insert, so changed data not updated

for change in changed_data:
    curr_data = spark.sql(f"SELECT * FROM {source}.{table_name} WHERE day(date) = {change['day']}").collect()
    for row in current_data:
        assert row[change["column"]] == change["changes"][row["date"]]["changed"]
