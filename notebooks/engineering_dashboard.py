# Databricks notebook source
from ingenii_databricks.dashboard_utils import create_widgets, \
    filtered_import_table

# COMMAND ----------

create_widgets(spark, dbutils)

# COMMAND ----------

display(filtered_import_table(spark, dbutils))

# COMMAND ----------

# Run an ingestion on an exiting entry. This cell should be updated and run manually
# dbutils.notebook.run("data_pipeline", 600, {
#     "source": "random_example",
#     "table": "alpha",
#     "file_name": "20210512_RandomExample.csv",
#     "increment": 0
# })

# COMMAND ----------

# Run an ingestion on a new file. This cell should be updated and run manually
# dbutils.notebook.run("data_pipeline", 600, {
#     "file_path": "/random_example/alpha",
#     "file_name": "20210514_RandomExample.csv",
#     "increment": 0
# })
