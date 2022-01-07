# Databricks notebook source

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS test_data ;

# COMMAND ----------

for known_table in spark.sql("SHOW TABLES FROM test_data").collect():
    spark.sql(f"DELETE FROM test_data.{known_table.tableName}")
    spark.sql(f"DROP TABLE IF EXISTS test_data.{known_table.tableName}")

# COMMAND ----------

if "orchestration" not in [db.databaseName for db in spark.sql(f"SHOW DATABASES").collect()]:
    spark.sql("CREATE DATABASE orchestration")
if "import_file" in [table.tableName for table in spark.sql(f"SHOW TABLES FROM orchestration").collect()]:
    spark.sql("DELETE FROM orchestration.import_file WHERE source = 'test_data'")
