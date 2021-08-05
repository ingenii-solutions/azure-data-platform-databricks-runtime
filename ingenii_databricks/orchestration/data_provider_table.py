from datetime import datetime
from os import makedirs, path
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import BooleanType, StructField, StructType, \
    StringType, TimestampType
from pyspark.sql.session import SparkSession
from typing import List, Union

from .base import OrchestrationTable
from .connection import ConnectionTypes
from .data_provider import DataProvider
from ingenii_databricks.table_utils import delete_table_entries, \
    merge_dataframe_into_table, MergeType


class DataProviderTableConfigurationException(Exception):
    ...


class DataProviderTableInterface(OrchestrationTable):
    """
    Interface to interact with the data_provider_table table, giving the
    high-level configuration for each table to do with a data provider. Not to
    be initialised directly, and only used by the DataProviderTable class
    """
    table_schema = [
        StructField("data_provider", StringType(), nullable=False),
        StructField("table", StringType(), nullable=False),
        StructField("enabled", BooleanType(), nullable=False),
        StructField("_date_row_inserted", TimestampType(), nullable=False),
        StructField("_date_row_updated", TimestampType(), nullable=True)
    ]
    primary_keys = ["data_provider", "table"]
    orch_table = "data_provider_table"

    def __init__(self, spark: SparkSession, data_provider: str, table: str
                 ) -> None:
        super(DataProviderTableInterface, self).__init__(spark)

        self.data_provider = data_provider
        self.table = table

    def list_tables(self) -> List[dict]:
        """
        List all the known tables for this data_provider
        """
        return sorted(
            self._convert_to_dictionaries(self.get_orch_table_df().where(
                col("data_provider") == self.data_provider)),
            key=lambda p: p["table"])

    def get_entry(self) -> dict:
        data_provider_table = self.get_orch_table_df().where(
            (col("data_provider") == self.data_provider) &
            (col("table") == self.table)
        )
        if data_provider_table.rdd.isEmpty():
            raise DataProviderTableConfigurationException(
                f"Data provider '{self.data_provider}', table '{self.table}' "
                f"does not yet exist in the orchestration.{self.orch_table} "
                f"table! Use the 'create_table' function to create it."
            )

        return data_provider_table.first().asDict()

    def exists(self) -> bool:
        try:
            self.get_entry()
            return True
        except DataProviderTableConfigurationException:
            return False

    def check_exists(self) -> None:
        if not self.exists():
            raise DataProviderTableConfigurationException(
                f"Data provider '{self.data_provider}', table {self.table} "
                f"doesn't exist in the orchestration.{self.orch_table} table!"
            )

    def check_doesnt_exist(self) -> None:
        if self.exists():
            raise DataProviderTableConfigurationException(
                f"Data provider '{self.data_provider}', table '{self.table}' "
                f"already exists in the orchestration.{self.orch_table} table!"
            )

    def create_entry(self, enabled: bool) -> None:
        self.check_doesnt_exist()

        new_entry = self.spark.createDataFrame(
            data=[(
                    self.data_provider, self.table, enabled,
                    datetime.utcnow()
                )],
            schema=StructType([
                f for f in self.table_schema
                if f.name in (
                    "data_provider", "table", "enabled",
                    "_date_row_inserted"
                    )
            ])
        )
        new_entry.write.format("delta").mode("append").saveAsTable(
            self.get_full_orch_table_name())

    def _change_status(self, enabled: bool) -> None:
        self.check_exists()
        self.get_orch_table().update(
            (col("data_provider") == self.data_provider) &
            (col("table") == self.table),
            {
                "enabled": lit(enabled),
                "_date_row_updated": lit(datetime.utcnow())
            })

    def delete_entry(self) -> None:
        self.check_exists()

        self.get_orch_table().delete(
            (col("data_provider") == self.data_provider) &
            (col("table") == self.table))


class DataProviderTableConfigInterface(OrchestrationTable):
    """
    Interface to interact with the data_provider_table_config table, giving the
    variable configuration for each table to do with a data provider. Not to
    be initialised directly, and only used by the DataProviderTable class
    """
    table_schema = [
        StructField("data_provider", StringType(), nullable=False),
        StructField("table", StringType(), nullable=False),
        StructField("key", StringType(), nullable=False),
        StructField("value", StringType(), nullable=False),
        StructField("_date_row_inserted", TimestampType(), nullable=False),
        StructField("_date_row_updated", TimestampType(), nullable=True)
    ]
    primary_keys = ["data_provider", "table", "key"]
    orch_table = "data_provider_table_config"

    def __init__(self, spark: SparkSession, data_provider: str, table: str
                 ) -> None:
        super(DataProviderTableConfigInterface, self).__init__(spark)

        self.data_provider = data_provider
        self.table = table

    def get_config_df(self) -> DataFrame:
        return self.get_orch_table_df().where(
            (col("data_provider") == self.data_provider) &
            (col("table") == self.table))

    def get_config(self) -> dict:
        return {
            row.key: row.value
            for row in self.get_config_df().collect()
        }

    def set_config(self, config: dict) -> None:
        datetime_now = datetime.utcnow()
        config_df = self.spark.createDataFrame(
            data=[
                (self.data_provider, self.table, key, value,
                 datetime_now, datetime_now)
                for key, value in config.items()
            ],
            schema=StructType([
                f for f in self.table_schema
                if f.name in (
                    "data_provider", "table", "key", "value",
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
        self.get_orch_table().delete(
            (col("data_provider") == self.data_provider) &
            (col("table") == self.table))


class DataProviderTable:
    """
    Class for interacting with the configuration of each table of data being
    transferred for a particular data provider. One of these should be
    initialised per table.
    """
    _details = {}

    @property
    def data_provider(self):
        return self._details["data_provider"]

    @data_provider.setter
    def data_provider(self, value):
        self._details["data_provider"] = value

    @property
    def table(self):
        return self._details.get("table")

    @table.setter
    def table(self, value):
        self._details["table"] = value

    def __str__(self):
        return f"{self.data_provider} {self.table}"

    def __init__(self, spark: SparkSession,
                 data_provider: Union[DataProvider, str], table: str = None
                 ) -> None:

        if isinstance(data_provider, DataProvider):
            self.data_provider_obj = data_provider
            self.data_provider = data_provider.name
        else:
            self.data_provider_obj = DataProvider(spark, data_provider)
            self.data_provider_obj.get_provider()
            self.data_provider = data_provider
        if table:
            self.table = table

        self.data_provider_table_interface = \
            DataProviderTableInterface(spark, self.data_provider, table)
        self.data_provider_table_config_interface = \
            DataProviderTableConfigInterface(spark, self.data_provider, table)

        for container in ("raw", "archive"):
            for folder_path in (
                f"/dbfs/mnt/{container}/{self.data_provider}",
                f"/dbfs/mnt/{container}/{self.data_provider}/{self.table}"
            ):
                if not path.exists(folder_path):
                    makedirs(folder_path)

    def list_tables(self) -> List[dict]:
        """
        Lists all of the tables currently configured for this data provider
        """
        return self.data_provider_table_interface.list_tables()

    def _check_specific_table(func):
        """
        This object can be initialised without a table name to facilitate the
        usage of the list_tables funtion. This decorator is used to check that
        the object is initialised for a specific table before continuing with
        a table-specific operation
        """
        def wrapper(self, *args, **kwargs):
            if self.table is None:
                raise Exception(
                    "This object isn't related to a specific data provider "
                    "table, but can be used for listing ones currently in the "
                    "database. Create an object specific to a provider by "
                    "passing the name when initialising, e.g. "
                    "DataProviderTable(spark, 'data_provider_name', "
                    "'table_name')"
                    )
            func(self, *args, **kwargs)
        return wrapper

    def _publish_data_providers_tables(func):
        """
        For the Data Factory integration, rather than read the Delta Lake
        parquet files directly, which contain the whole history of the table,
        we publish the current table values so Data Factory reads the
        up-to-date version.
        """
        def wrapper(self, *args, **kwargs):
            func(self, *args, **kwargs)
            self.data_provider_table_interface._publish_current_table()
        return wrapper

    # General table config, orchestration.data_provider_table table

    def exists(self) -> bool:
        return self.data_provider_table_interface.exists()

    @_check_specific_table
    def get_table(self) -> None:
        self._details = self.data_provider_table_interface.get_entry()
        return self._details

    @_publish_data_providers_tables
    @_check_specific_table
    def create_table(self, enabled: bool = True) -> None:
        self.data_provider_table_interface.create_entry(enabled)
        return self.get_table()

    @_publish_data_providers_tables
    @_check_specific_table
    def enable_table(self) -> None:
        self.data_provider_table_interface.check_exists()
        self.data_provider_table_interface._change_status(True)
        return self.get_table()

    @_publish_data_providers_tables
    @_check_specific_table
    def disable_table(self) -> None:
        self.data_provider_table_interface.check_exists()
        self.data_provider_table_interface._change_status(False)
        return self.get_table()

    @_publish_data_providers_tables
    @_check_specific_table
    def delete_table(self) -> None:
        self.data_provider_table_interface.delete_entry()

    # Specific table config, orchestration.data_provider_table_config table

    def _publish_data_providers_tables_configs(func):
        def wrapper(self, *args, **kwargs):
            func(self, *args, **kwargs)
            self.data_provider_table_config_interface._publish_current_table()
        return wrapper

    @_check_specific_table
    def get_config(self) -> dict:
        self.data_provider_table_interface.check_exists()

        self.data_provider_table_config_interface.get_config()

    @_publish_data_providers_tables_configs
    @_check_specific_table
    def set_config(self, config: dict) -> None:
        self.data_provider_table_interface.check_exists()

        ConnectionTypes \
            .get_connection(self.data_provider_obj.connection) \
            .validate_table_config(config)

        self.data_provider_table_config_interface.set_config(config)

    @_publish_data_providers_tables_configs
    @_check_specific_table
    def delete_config(self) -> None:
        self.data_provider_table_config_interface.delete_config()
