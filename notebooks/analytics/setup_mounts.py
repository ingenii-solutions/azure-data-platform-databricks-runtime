# Databricks notebook source
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}
data_lake_name = "datalake"
for container in ["orchestration", "source", "utilities"]:
    dbutils.fs.mount(
        source=f"abfss://{container}@{data_lake_name}.dfs.core.windows.net/",
        mount_point=f"/mnt/{container}",
        extra_configs=configs)

# COMMAND ----------
