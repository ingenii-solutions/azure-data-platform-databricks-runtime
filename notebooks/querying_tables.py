# Databricks notebook source
# MAGIC %md
# MAGIC ## On the nav bar to the left, click on 'Data' to see the tables available

# COMMAND ----------

# MAGIC %md ### Let's query a table

# COMMAND ----------

from delta.tables import DeltaTable
company_identifiers = DeltaTable.forName(spark, "reprisk.company_identifiers").toDF()

# COMMAND ----------

display(company_identifiers)

# COMMAND ----------

risk_incidents = DeltaTable.forName(spark, "reprisk.risk_incidents").toDF()

# COMMAND ----------

display(risk_incidents)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Selecting and Joining
# MAGIC ### https://docs.microsoft.com/en-us/azure/databricks/spark/latest/dataframes-datasets/introduction-to-dataframes-python
# MAGIC ### https://sparkbyexamples.com/spark/spark-sql-dataframe-join/
# MAGIC ### https://kb.databricks.com/data/join-two-dataframes-duplicated-columns.html

# COMMAND ----------

full_join = company_identifiers.join(risk_incidents, company_identifiers.RepRisk_COMPANY_ID == risk_incidents.Company_ID)
display(full_join)

# COMMAND ----------

from datetime import date
from pyspark.sql.functions import col, md5
filtered_full_join = full_join.where(col("Incident_Date") >= date(2021, 1, 12))
display(filtered_full_join)

# COMMAND ----------

%md
### Select a subset of columns
### 'filter' works the same as 'where'

# COMMAND ----------

selected_columns = full_join.select("COMPANY_Name", "Incident_Date", "Related_Countries", "Severity", "Reach", "Novelty").where(col("Sector[s]") == "Mining")
display(selected_columns)

# COMMAND ----------

%md
## Restore table to an earlier state
### https://docs.microsoft.com/en-us/azure/databricks/delta/delta-utility#--restore-a-delta-table-to-an-earlier-state

# COMMAND ----------
