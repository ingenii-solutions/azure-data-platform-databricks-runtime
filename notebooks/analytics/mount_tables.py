# Databricks notebook source
from pyspark.sql.functions import col

known_sources = spark.table("orchestration.import_file").select("source").distinct().collect()
mounted_sources = [db.databaseName for db in spark.sql(f"SHOW DATABASES").collect()]
for source in known_sources:
    if source.source not in mounted_sources:
        spark.sql(f"CREATE DATABASE {source.source}")
    known_tables = spark.table("orchestration.import_file").where(col("source") == source.source).select("table").distinct().collect()
    mounted_tables = [table.tableName for table in spark.sql(f"SHOW TABLES FROM {source.source}").collect()]
    for known_table in known_tables:
        if known_table.table.lower() not in mounted_tables:
            print(f"Adding table {source.source}.{known_table.table}")
            spark.sql(f"CREATE TABLE IF NOT EXISTS {source.source}.{known_table.table} USING DELTA LOCATION '/mnt/source/{source.source}/{known_table.table}'")

# COMMAND ----------
