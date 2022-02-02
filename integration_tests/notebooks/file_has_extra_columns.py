# Databricks notebook source

from os import environ

from ingenii_data_engineering.dbt_schema import get_source
from ingenii_databricks.table_utils import read_file, SchemaException

# COMMAND ----------

dbt_root_folder = environ["DBT_ROOT_FOLDER"]
source = "test_data"

source_details = get_source(dbt_root_folder, source)

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
