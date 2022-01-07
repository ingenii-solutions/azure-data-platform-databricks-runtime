# Databricks notebook source

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS orchestration ;
# MAGIC CREATE TABLE IF NOT EXISTS orchestration.import_file USING DELTA LOCATION '/mnt/orchestration/import_file' ;
# MAGIC DELETE FROM orchestration.import_file WHERE source = 'test_data' ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS test_data ;

# COMMAND ----------

for known_table in spark.sql("SHOW TABLES FROM test_data").collect():
    spark.sql(f"DELETE FROM test_data.{known_table.table}")
    spark.sql(f"DROP TABLE IF EXISTS test_data.{known_table.table}")
