# Databricks notebook source
company_df = spark.table("source.reprisk_company_identifiers")
dbutils.widgets.dropdown(
    name="company",
    defaultValue="--",
    choices=["--"] + [
        s.COMPANY_Name for s in company_df.select("COMPANY_Name").distinct().collect()
    ],
    label="Company")

# COMMAND ----------

displayHTML(f"""<font size="6"face="sans-serif">Company Dashboard: {dbutils.widgets.get("company")}</font>""")

# COMMAND ----------

display(company_df.where(col("COMPANY_Name") == dbutils.widgets.get("company")))

# COMMAND ----------

from pyspark.sql.functions import col
company_id = company_df.select("RepRisk_COMPANY_ID").where(col("COMPANY_Name") == dbutils.widgets.get("company")).first().RepRisk_COMPANY_ID
display(spark.table("source.reprisk_metrics").where(col("RepRisk_COMPANY_ID") == company_id))

# COMMAND ----------

company_id = company_df.select("RepRisk_COMPANY_ID").where(col("COMPANY_Name") == dbutils.widgets.get("company")).first().RepRisk_COMPANY_ID
display(spark.table("source.reprisk_risk_incidents").where(col("Company_ID") == company_id).sort(col("Incident_Date").desc()).head(5))

# COMMAND ----------


