# Databricks notebook source

from copy import deepcopy
from os import environ
from pyspark.sql.functions import date_add
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

table_name = f"{source_table_name}_{MergeType.REPLACE}"
table_schema["join"]["type"] = MergeType.REPLACE

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
# Variables to make this easier to read
select_all = f"SELECT * FROM {source}.{table_name}"

# Store current version of the data
raw_data = spark.sql(select_all).collect()

# COMMAND ----------
# Delete some data, change others
day_to_delete = randint(1, 31)
where_delete = f"WHERE day(date) = {day_to_delete}"

deleted_count = spark.sql(f"{select_all} {where_delete}").count()
print(f"Deleting all entries for day {day_to_delete} of the month")
spark.sql(f"DELETE FROM {source}.{table_name} {where_delete}")

for col in table_schema["columns"][1:]:  # Don't change the date
    day_to_change = day_to_delete
    # Can't be the same
    while day_to_change == day_to_delete:
        day_to_change = randint(1, 31)

    where_change = f"WHERE day(date) = {day_to_change}"

    print(f"Changing column {col['name']} for day {day_to_change} of the month")
    if col["data_type"] == "boolean":
        set_clause = f"SET {col['name']} = NOT {col['name']} "
    elif col["data_type"] == "timestamp":
        set_clause = f"SET {col['name']} = {col['name']} + INTERVAL '10' MINUTE "
    else:
        set_clause = f"SET {col['name']} = {col['name']} + 10 "

    original_data = spark.sql(f"{select_all} {where_change}").collect()
    spark.sql(f"UPDATE {source}.{table_name} {set_clause} {where_change}")
    curr_data = spark.sql(f"{select_all} {where_change}").collect()

    changes = {
        row["date"]: {
            "original": row[col["name"]]
        }
        for row in original_data
    }
    for row in curr_data:
        changes[row["date"]]["changed"] = row[col["name"]]

# COMMAND ----------
# Add some data too

print("Adding 10 rows which should be removed")
some_rows = spark.sql(f"{select_all} ORDER BY DATE DESC LIMIT 10")
some_rows = some_rows.withColumn("date", date_add(some_rows.date, 100))
some_rows.write.mode("append").saveAsTable(f"{source}.{table_name}")

# COMMAND ----------
# Reingest file, check replacement
create_file_table(spark, import_entry, table_schema)

# Add to the source table
add_to_source_table(spark, import_entry, table_schema)
remove_file_table(spark, dbutils, import_entry)

row_count = spark.sql(select_all).count()

raw_count = 0
with open(f"/dbfs/mnt/archive/{source}/{table_name}/{file_name}") as raw_file:
    for _ in raw_file.readlines():
        raw_count += 1

# Check that a new copy of the data has been inserted
assert raw_count == row_count + 1  # Add for header row

# COMMAND ----------
# Assert data has been reverted

for raw, curr in zip(raw_data, spark.sql(select_all).collect()):
    for column in table_schema["columns"]:
        assert raw[column["name"]] == curr[column["name"]]