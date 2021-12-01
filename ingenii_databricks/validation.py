from pyspark.sql.session import SparkSession
from pyspark.sql.utils import AnalysisException
from typing import Union

from ingenii_databricks.orchestration import ImportFileEntry
from ingenii_databricks.table_utils import add_columns_to_table


class ParameterException(Exception):
    ...


def check_parameters(source: Union[str, None], table_name: Union[str, None],
                     file_path: Union[str, None], file_name: Union[str, None],
                     increment: Union[str, None]) -> None:
    """
    Check the parameters that we have obtained from widgets

    Parameters
    ----------
    source : Union[str, None]
        The source name. Either file_path, or this and table_name is required
    table_name : Union[str, None]
        The table name. Either file_path, or this and source is required
    file_path : Union[str, None]
        The path to the folder holding the raw file. Either this, or source
        and table_name are required
    file_name : Union[str, None]
        The name of the file. Required
    increment : Union[str, None]
        The increment of the ingestion we want to create. New files start at 0

    Raises
    ------
    ParameterException
        If there are any issues, raise an exception
    """
    errors = []
    if file_name is None or file_name == "":
        errors.append("Must pass the 'file_name' argument!")
    if increment is None or increment == "":
        errors.append("Must pass the 'increment' argument!")

    if source is None or table_name is None:
        if file_path is None or file_path == "":
            errors.append(
                "'source' or 'table' is Null! These must either be passed "
                "directly, or can be drawn from the 'file_path' but this is "
                "Null as well!"
            )
        else:
            errors.append(
                "'source' or 'table' is Null! These must either be passed "
                "directly, or the file path passed as the 'file_path' "
                "argument from which these can be determined."
                )

    if errors:
        raise ParameterException("\n".join(errors))


def compare_schema_and_table(spark: SparkSession,
                             import_entry: ImportFileEntry,
                             table_schema: dict) -> None:
    """
    Check that the source table has all the necessary columns for the
    individual file table, and if not, add those columns in. Also, check that
    the types match.

    Parameters
    ----------
    spark : SparkSession
        Object for interacting with Delta tables
    import_entry : ImportFileEntry
        Import entry for the specific file
    table_schema : dict
        Schema of the table for this specific file
    """
    try:
        table_information = spark.sql(
            f"DESCRIBE TABLE {import_entry.source}.{import_entry.table}"
        ).collect()
    except AnalysisException as e:
        # Catch case when we haven't created the table yet
        if "Table or view not found" in e.desc:
            return
        else:
            raise e

    table_columns = [
        col.col_name for col in table_information if col.data_type
    ]
    missing_table_schema = [
        col for col in table_schema["columns"]
        if col["name"].strip("`") not in table_columns
    ]
    if missing_table_schema:
        add_columns_to_table(spark, import_entry.source, import_entry.table,
                             missing_table_schema)
