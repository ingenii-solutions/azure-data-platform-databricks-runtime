from datetime import datetime
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import BooleanType, StructField, StructType, \
    StringType, TimestampType
from pyspark.sql.session import SparkSession
from typing import List

from .base import OrchestrationTable
from .connection import ConnectionTypes
from ingenii_databricks.table_utils import delete_table_entries, \
    merge_dataframe_into_table, MergeType


class DataProviderConfigurationException(Exception):
    ...


class DataProviderInterface(OrchestrationTable):
    table_schema = [
        StructField("name", StringType(), nullable=False),
        StructField("connection", StringType(), nullable=False),
        StructField("authentication", StringType(), nullable=False),
        StructField("enabled", BooleanType(), nullable=False),
        StructField("_date_row_inserted", TimestampType(), nullable=False),
        StructField("_date_row_updated", TimestampType(), nullable=True)
    ]
    primary_keys = ["name"]
    orch_table = "data_provider"

    def __init__(self, spark: SparkSession, name: str) -> None:
        super(DataProviderInterface, self).__init__(spark)

        self.name = name

    def get_entry(self) -> dict:
        data_provider = self.get_orch_table_df().where(
            col("name") == self.name
        )
        if data_provider.rdd.isEmpty():
            raise DataProviderConfigurationException(
                f"Data provider '{self.name}' does not yet exist in the "
                f"orchestration.{self.orch_table} table! Use the "
                f"'create_provider' function to create it."
            )

        return data_provider.first().asDict()

    def exists(self) -> bool:
        try:
            self.get_entry()
            return True
        except DataProviderConfigurationException:
            return False

    def check_exists(self) -> None:
        if not self.exists():
            raise DataProviderConfigurationException(
                f"Data provider '{self.name}' doesn't exist in the "
                f"orchestration.{self.orch_table} table! Perhaps you meant "
                f"'create_provider'?"
            )

    def check_doesnt_exist(self) -> None:
        if self.exists():
            raise DataProviderConfigurationException(
                f"Data provider '{self.name}' already exists in the "
                f"orchestration.{self.orch_table} table, "
                f"so we don't need to create it! Perhaps you meant "
                f"'update_provider'?"
            )

    def add_entry(self, connection: str, authentication: str,
                  enabled: bool) -> None:
        # Add new entry
        new_entry = self.spark.createDataFrame(
            data=[(
                    self.name, connection, authentication, enabled,
                    datetime.utcnow()
                )],
            schema=StructType([
                f for f in self.table_schema
                if f.name in (
                    "name", "connection", "authentication", "enabled",
                    "_date_row_inserted"
                    )
            ])
        )
        new_entry.write.format("delta").mode("append").saveAsTable(
            self.get_full_orch_table_name())

    def update_entry(self, connection: str, authentication: str,
                     enabled: bool) -> None:
        self.get_orch_table().update(
            (col("name") == self.name),
            {
                "connection": connection,
                "authentication": authentication,
                "enabled": enabled,
                "_date_row_updated": lit(datetime.utcnow())
            })

    def _change_status(self, enabled: bool) -> None:
        self.check_exists()
        self.get_orch_table().update(
            (col("name") == self.name),
            {
                "enabled": lit(enabled),
                "_date_row_updated": lit(datetime.utcnow())
            })

    def delete_entry(self) -> None:
        self.get_orch_table().delete(col("name") == self.name)


class DataProviderConfigInterface(OrchestrationTable):
    table_schema = [
        StructField("data_provider", StringType(), nullable=False),
        StructField("key", StringType(), nullable=False),
        StructField("value", StringType(), nullable=False),
        StructField("_date_row_inserted", TimestampType(), nullable=False),
        StructField("_date_row_updated", TimestampType(), nullable=True)
    ]
    primary_keys = ["data_provider", "key"]
    orch_table = "data_provider_config"

    def __init__(self, spark: SparkSession, name: str) -> None:
        super(DataProviderConfigInterface, self).__init__(spark)

        self.name = name

    def get_config_df(self) -> DataFrame:
        return self.get_orch_table_df().where(
            col("data_provider") == self.name)

    def get_config(self) -> dict:
        return {
            row.key: row.value
            for row in self.get_config_df().collect()
        }

    def add_config(self, config: dict) -> None:
        datetime_now = datetime.utcnow()
        config_df = self.spark.createDataFrame(
            data=[
                (self.name, key, value,
                 datetime_now, datetime_now)
                for key, value in config.items()
            ],
            schema=StructType([
                f for f in self.table_schema
                if f.name in (
                    "data_provider", "key", "value",
                    "_date_row_inserted", "_date_row_updated"
                    )
            ])
        )

        merge_dataframe_into_table(
            self.get_orch_table(), config_df, self.primary_keys,
            MergeType.MERGE_DATE_ROWS
        )

        # Remove entries that are not a part of this new config
        rows_to_delete = self.get_config_df().join(
            config_df, on=self.primary_keys, how="leftanti")
        delete_table_entries(self.get_orch_table(), rows_to_delete,
                             self.primary_keys)

    def delete_config(self) -> None:
        self.get_orch_table().delete(col("data_provider") == self.name)


class DataProvider:
    _details = {}

    @property
    def name(self):
        return self._details["name"]

    @name.setter
    def name(self, value):
        self._details["name"] = value

    @property
    def connection(self):
        return self._details.get("connection")

    @connection.setter
    def connection(self, value):
        self._details["connection"] = value

    @property
    def authentication(self):
        return self._details.get("authentication")

    @authentication.setter
    def authentication(self, value):
        self._details["authentication"] = value

    def __str__(self):
        return self.name

    def __init__(self, spark: SparkSession, name: str = None) -> None:
        self.data_provider_interface = DataProviderInterface(spark, name)
        self.data_provider_config_interface = \
            DataProviderConfigInterface(spark, name)

        if name:
            self.name = name

    def list_providers(self) -> List[dict]:
        return sorted(
            self.data_provider_interface._convert_to_dictionaries(
                self.data_provider_interface.get_orch_table_df()),
            key=lambda p: p["name"])

    def _check_specific_data_provider(func):
        def wrapper(self, *args, **kwargs):
            if self.name is None:
                raise Exception(
                    "This object isn't related to a specific data provider, "
                    "but can be used for listing ones currently in the "
                    "database. Create an object specific to a provider by "
                    "passing the name when initialising, "
                    "e.g. DataProvider(spark, 'name')"
                    )
            func(self, *args, **kwargs)
        return wrapper

    def _publish_data_providers(func):
        def wrapper(self, *args, **kwargs):
            func(self, *args, **kwargs)
            self.data_provider_interface._publish_current_table()
        return wrapper

    # General configuration, orchestration.data_provider table

    def exists(self) -> bool:
        return self.data_provider_interface.exists()

    @_check_specific_data_provider
    def get_provider(self) -> None:
        self._details = self.data_provider_interface.get_entry()
        return self._details

    def _validate_data(self, connection: str, authentication: str,
                       ) -> None:
        errors = []
        if not ConnectionTypes.check_connection(connection):
            errors.append(
                f"Connection type {connection} is not valid. "
                f"Possible types: {ConnectionTypes.all_connections()}"
                )
        elif not ConnectionTypes.check_authentication(connection,
                                                      authentication):
            errors.append(
                f"Authentication type {authentication} is not valid for "
                f"connection {connection}. Possible types: "
                f"{ConnectionTypes.all_authentications(connection)}"
                )
        if errors:
            raise DataProviderConfigurationException(errors)

    @_publish_data_providers
    @_check_specific_data_provider
    def create_provider(self, connection: str, authentication: str,
                        enabled: bool = True) -> None:
        self._validate_data(connection, authentication)

        self.data_provider_interface.check_doesnt_exist()
        self.data_provider_interface.add_entry(
            connection, authentication, enabled)

        return self.get_provider()

    @_publish_data_providers
    @_check_specific_data_provider
    def update_provider(self, connection: str, authentication: str,
                        enabled: bool = True) -> None:
        self._validate_data(connection, authentication)

        self.data_provider_interface.check_exists()
        self.data_provider_interface.update_entry(
            connection, authentication, enabled
        )

        return self.get_provider()

    @_publish_data_providers
    @_check_specific_data_provider
    def enable_provider(self) -> None:
        self.data_provider_interface._change_status(True)
        return self.get_provider()

    @_publish_data_providers
    @_check_specific_data_provider
    def disable_provider(self) -> None:
        self.data_provider_interface._change_status(False)
        return self.get_provider()

    @_publish_data_providers
    @_check_specific_data_provider
    def delete_provider(self) -> None:
        self.data_provider_interface.check_exists()
        self.data_provider_interface.delete_entry()

    # Specific configuration, orchestration.data_provider_config table

    def _publish_data_providers_configs(func):
        def wrapper(self, *args, **kwargs):
            func(self, *args, **kwargs)
            self.data_provider_config_interface._publish_current_table()
        return wrapper

    def get_config(self) -> dict:
        return self.data_provider_config_interface.get_config()

    @_publish_data_providers_configs
    def set_config(self, config: dict) -> None:
        if self.connection is None:
            self.get_provider()

        connection_obj = ConnectionTypes.connection_types[
            self.connection]
        connection_obj.validate_config(config)

        self.data_provider_config_interface.add_config(config)

    @_publish_data_providers_configs
    def delete_config(self) -> None:
        self.data_provider_config_interface.delete_config()
