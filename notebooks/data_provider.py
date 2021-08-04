# Databricks notebook source

from ingenii_databricks.orchestration import DataProvider, DataProviderTable

# COMMAND ----------

# List data providers
DataProvider(spark).list_providers()

# COMMAND ----------

# Creating a new data provider
data_provider = "example_sftp"
connection = "sftp"
authentication = "basic"
config = {
    "url": "https://example_api.com",
    "username": "sftp-username",
    "key_vault_secret_name": "sftp-password"
}

data_provider_obj = DataProvider(spark, data_provider)


# COMMAND ----------

if data_provider_obj.exists():
    # If the provider already exists, get the details
    data_provider_obj.get_provider()
else:
    # If the provider does not exist, create it
    data_provider_obj.create_provider(
        connection=connection,
        authentication=authentication
    )

# COMMAND ----------

# Set the config for this data provider
data_provider_obj.set_config(config)

# COMMAND ----------

# Definitions of what tables/file should be brought across and ingested
tables = [
    {
        "data_provider": "testsftp",
        "table": "random_data_1",
        "config": {
            "path": "random_data_1"
        }
    },
    {
        "data_provider": "testsftp",
        "table": "random_data_2",
        "config": {
            "path": "random_data_2",
            "zipped": "true"
        }
    }
]

# COMMAND ----------

# Add definitions to the database
for table_dict in tables:
    table = DataProviderTable(
        spark, table_dict["data_provider"], table_dict["table"])

    if table.exists():
        table.get_table()
    else:
        table.create_table()

    table.set_config(table_dict["config"])
