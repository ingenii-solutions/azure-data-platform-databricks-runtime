from delta.tables import DeltaTable
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from typing import List, Union


def get_folder_path(stage: str, source_name: str, table_name: str,
                    hash_identifier=None) -> str:
    """
    Produce the folder path for a particular table. Does not start with
    '/dbfs', and so is not the 'full' path

    Parameters
    ----------
    stage : str
        The stage that the table is in e.g. 'raw', 'source'
    source_name : str
        The name of the source
    table_name : str
        The name of the table
    hash_identifier : [type], optional
        [description], by default None

    Returns
    -------
    str
        The folder path that contains the table's files
    """

    return "/" + "/".join([
        "mnt", stage, source_name,
        f"{table_name}{hash_identifier if hash_identifier else ''}"
        ])


def is_table(spark: SparkSession, table_folder_path: str) -> bool:
    """
    Determine if there is a Delta table at this folder path

    Parameters
    ----------
    spark : pyspark.sql.session.SparkSession
        Object for interacting with Delta tables
    table_folder_path : str
        The folder path to check

    Returns
    -------
    bool
        Whether this is a table or not
    """
    return DeltaTable.isDeltaTable(spark, table_folder_path)


def get_table(spark: SparkSession, table_folder_path: str) -> DeltaTable:
    """
    Get the representation of a Delta table

    Parameters
    ----------
    spark : pyspark.sql.session.SparkSession
        Object for interacting with Delta tables
    table_folder_path : str
        The folder path to access

    Returns
    -------
    DeltaTable
        The Delta table at this location
    """
    return DeltaTable.forPath(spark, table_folder_path)


def handle_name(raw_name: str) -> str:
    """
    Remove or replace Databricks forbidden characters: ' ,;{}()\n\t='

    Parameters
    ----------
    raw_name : str
        The raw name

    Returns
    -------
    str
        An acceptable version of the name
    """
    return raw_name.replace(" ", "_").replace(",", "").replace(";", "") \
                   .replace("{", "[").replace("}", "]") \
                   .replace("(", "[").replace(")", "]") \
                   .replace("\n", "").replace("\t", "_").replace("=", "-")


def schema_as_string(schema_list: list) -> str:
    """
    Takes a dictionary object of a schema, and turns it into string form to be
    used in SQL commands

    Parameters
    ----------
    schema_list : list
        The table schema, which requires both 'name' and 'data_type' keys

    Returns
    -------
    str
        The schema in SQL form
    """
    return ", ".join([
        f"{handle_name(s['name'])} {s['data_type']}"
        for s in schema_list
    ])


def read_file(spark: SparkSession, file_path: str,
              table_schema: dict) -> DataFrame:
    """
    Read a character separated file and create a dataframe
    Reference: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.csv.html#pyspark.sql.DataFrameReader.csv)

    Parameters
    ----------
    spark : SparkSession
        Object for interacting with Delta tables
    file_path : str
        Path of the file to read
    table_schema : dict
        The schema of the table with information necessary to read the file

    Returns
    -------
    DataFrame
        Spark DataFrame of the data
    """
    return spark.read.csv(
        **table_schema.get("file_details", {}),
        path=file_path,
        schema=schema_as_string(table_schema["columns"])
        )


def create_database(spark: SparkSession, database_name: str) -> None:
    """
    Create a database if it does not already exist

    Parameters
    ----------
    spark : SparkSession
        Object for interacting with Delta tables
    database_name : str
        The database name
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {handle_name(database_name)}")


def create_table(spark: SparkSession, database_name: str, table_name: str,
                 schema_list: list, folder_path: str) -> DeltaTable:
    """
    Create an unmanaged Delta table at a defined location

    Parameters
    ----------
    spark : SparkSession
        Object for interacting with Delta tables
    database_name : str
        The database to create the table in. Must already exist
    table_name : str
        The name of the table
    schema_list : list[dict]
        The schema of the table to create
    folder_path : str
        The folder path to create the table files in

    Returns
    -------
    DeltaTable
        The Delta table that has been created
    """
    spark.sql(" ".join([
        f"CREATE TABLE IF NOT EXISTS",
        f"{handle_name(database_name)}.{handle_name(table_name)}",
        f"({schema_as_string(schema_list)})",
        f"USING DELTA LOCATION '{folder_path}'"
    ]))
    return get_table(spark, folder_path)


def insert_dataframe_into_table(folder_path: str, dataframe: DataFrame
                                ) -> None:
    """
    Given a DataFrame, insert it into a Delta table

    Parameters
    ----------
    folder_path : str
        Folder path of the Delta table
    dataframe : DataFrame
        DataFrame of data to insert
    """
    dataframe.write.format("delta").mode("append").save(folder_path)


def _match_condition_string(merge_columns: Union[str, List[str]]) -> str:
    """
    Given the columns to match on, generate a spark SQL string for matching
    two dataframes

    Parameters
    ----------
    merge_columns : Union[str, List[str]]
        The columns to match on. Can be comma-separated string

    Returns
    -------
    str
        Full string to pass to 'merge' function
    """
    if isinstance(merge_columns, str):
        merge_columns = merge_columns.split(",")
    return " AND ".join([
        f"deltatable.{handle_name(mc)} = dataframe.{handle_name(mc)}"
        for mc in merge_columns
    ])


def _difference_condition_string(all_columns: List[str],
                                 merge_columns: Union[str, List[str]]) -> str:
    """
    Generate a spark SQL string for finding where data has changed and so
    needs updating. Ignore primary keys and internal columns

    Parameters
    ----------
    all_columns : List[str]
        The names of all the columns
    merge_columns : Union[str, List[str]]
        The names of columns that are in the primary key

    Returns
    -------
    str
        Full string to pass to the 'condition' field
    """
    if isinstance(merge_columns, str):
        merge_columns = merge_columns.split(",")
    return " OR ".join([
        f"deltatable.{handle_name(column)} <> dataframe.{handle_name(column)}"
        for column in all_columns
        if column not in merge_columns and not column.startswith("_")
    ])


class MergeType:
    """
    Class to ensure that the correct merge types are used in functions. When
    we pass a merge type to a function such as merge_dataframe_into_table, we
    can use this class to ensure no unintended consequences
    """
    MERGE_DATE_ROWS = "merge_date_rows"
    MERGE_UPDATE = "merge_update"
    MERGE_INSERT = "merge_insert"
    INSERT = "insert"

    @classmethod
    def all_types(cls):
        return [
            cls.MERGE_DATE_ROWS, cls.MERGE_UPDATE,
            cls.MERGE_INSERT, cls.INSERT
        ]

    @classmethod
    def check_type(cls, type_to_check):
        return type_to_check in cls.all_types()


def merge_dataframe_into_table(merge_table: DeltaTable, dataframe: DataFrame,
                               merge_columns: Union[str, List[str]],
                               merge_type: str) -> None:
    """
    Merge a dataframe into a Delta table matching on one or more columns

    Parameters
    ----------
    merge_table : DeltaTable
        The DeltaTable to merge data into
    dataframe : DataFrame
        The data to merge in
    merge_columns : Union[str, List[str]]
        The columns to base the merge on. Can be comma-separated string
    merge_type: str
        The action to take when merging e.g. updating, or only inserting
    """

    if not MergeType.check_type(merge_type):
        raise Exception(
            f"{merge_type} not a recognised merge type! "
            f"Possible types: {MergeType.all_types()}")

    # https://github.com/delta-io/delta/blob/master/python/delta/tables.py
    updated_table = \
        merge_table.alias("deltatable") \
                   .merge(dataframe.alias("dataframe"),
                          _match_condition_string(merge_columns))

    if merge_type == MergeType.MERGE_DATE_ROWS:
        # Merge, but be careful with _date_row_inserted and _date_row_updated
        updated_table = updated_table \
            .whenMatchedUpdate(
                condition=_difference_condition_string(
                    dataframe.columns, merge_columns),
                set={
                    col_name: f"dataframe.{col_name}"
                    for col_name in dataframe.columns
                    if col_name != "_date_row_inserted"  # Remains the same
                }
            ) \
            .whenNotMatchedInsert(values={
                col_name: f"dataframe.{col_name}"
                for col_name in dataframe.columns
                if col_name != "_date_row_updated"  # _date_row_updated is null
            })
    elif merge_type == MergeType.MERGE_UPDATE:
        # Insert, or update if any of the data columns change
        updated_table = updated_table \
            .whenNotMatchedInsertAll() \
            .whenMatchedUpdateAll(
                condition=_difference_condition_string(
                    dataframe.columns, merge_columns)
            )
    elif merge_type == MergeType.MERGE_INSERT:
        # Only insert
        updated_table = updated_table \
            .whenNotMatchedInsertAll()
    else:
        raise Exception(
            f"{merge_type} not a recognised merge type! "
            f"Possible types: {MergeType.all_types()}")

    updated_table.execute()


def delete_table_entries(deltatable: DeltaTable, dataframe: DataFrame,
                         merge_columns: Union[str, List[str]]) -> None:
    """
    Delete from the table the matching entries in the dataframe

    Parameters
    ----------
    merge_table : DeltaTable
        The DeltaTable to delete data from
    dataframe : DataFrame
        The data to delete
    merge_columns : Union[str, List[str]]
        A comma separated list of columns to base the merge on
    """

    deltatable.alias("deltatable").merge(
        source=dataframe.alias("dataframe"),
        condition=_match_condition_string(merge_columns)) \
        .whenMatchedDelete()


def delete_table(spark: SparkSession, database_name: str, table_name: str
                 ) -> None:
    """
    Delete a table from a database. Note that if this is an unmanaged table
    then this will only delete the metadata, and not the table files itself.
    This will not delete the database.

    Parameters
    ----------
    spark : SparkSession
        Object for interacting with Delta tables
    database_name : str
        The database to delete the table from
    table_name : str
        The name of the table to delete
    """
    # https://docs.microsoft.com/en-us/azure/databricks/kb/delta/drop-delta-table
    # https://docs.microsoft.com/en-us/azure/databricks/spark/latest/spark-sql/language-manual/delta-delete-from
    full_name = f"{handle_name(database_name)}.{handle_name(table_name)}"
    spark.sql(f"DELETE FROM {full_name}")
    spark.sql(f"DROP TABLE IF EXISTS {full_name}")
