from datetime import date, datetime
from delta.tables import DeltaTable
from pyspark.sql.dataframe import DataFrame
from typing import List

from ingenii_databricks.table_utils import create_database, create_table, \
    get_table, is_table


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


class OrchestrationTable:
    """
    Base class with utilities to help interact with any orchestration table
    """
    database = "orchestration"
    orch_table = None
    table_schema = []
    primary_keys = []

    def __init__(self, spark):
        self.spark = spark

    def get_orch_table_folder(self):
        return f"/mnt/{self.database}/{self.orch_table}"

    def create_orch_table(self):
        create_database(self.spark, self.database)
        return create_table(
            self.spark, self.database, self.orch_table,
            self.schema_as_dict(self.table_schema),
            self.get_orch_table_folder())

    def get_orch_table(self) -> DeltaTable:
        """
        Get the orchestration table. If it doesn't exist yet, create it

        Returns
        -------
        DeltaTable
            A representation of the table that can be queried
        """
        if is_table(self.spark, self.get_orch_table_folder()):
            return get_table(self.spark, self.get_orch_table_folder())
        else:
            return self.create_orch_table()

    def get_orch_table_df(self) -> DataFrame:
        """
        Get the orchestration, but return as a DataFrame

        Returns
        -------
        DataFrame
            The orchestration table as a DataFrame
        """
        return self.get_orch_table().toDF()

    def get_full_orch_table_name(self):
        return f"{self.database}.{self.orch_table}"

    @staticmethod
    def schema_as_dict(schema_list):
        return [
            {"name": f.name, "data_type": f.dataType.simpleString()}
            for f in schema_list
        ]

    @staticmethod
    def _convert_to_dictionaries(dataframe) -> List[dict]:
        return list(map(lambda row: row.asDict(), dataframe.collect()))
