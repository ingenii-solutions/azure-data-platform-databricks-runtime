from pyspark.sql.session import SparkSession
from pyspark.sql.utils import AnalysisException
from typing import List, Union

from ingenii_databricks.enums import MergeType
from ingenii_databricks.orchestration import ImportFileEntry
from ingenii_databricks.table_utils import add_columns_to_table, handle_name, \
    handle_major_name


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


class SchemaException(Exception):
    ...


def check_source_schema(source_dict: dict) -> List[str]:
    """
    Check the names in the schema to see if we can use them in Databricks

    Parameters
    source_dict
    proposed_source : dict
        The schema loaded from the dbt folder structure for a particular source

    Raises
    ------
    SchemaException
        If there are any issues, raise an exception
    """
    errors = []

    if source_dict["schema"] != handle_major_name(source_dict["schema"]):
        errors.append(
            f"Source {source_dict['name']}: "
            f"Schema named '{source_dict['schema']}' is not possible in "
            f"Databricks. Suggested name: "
            f"{handle_major_name(source_dict['schema'])}"
        )

    for _, table in source_dict.get("tables", {}).items():
        if table["name"] != handle_major_name(table["name"]):
            errors.append(
                f"Source {source_dict['name']}: "
                f"Table named '{table['name']}' is not possible in "
                f"Databricks. Suggested name: "
                f"{handle_major_name(table['name'])}"
            )

        # Check that the join type is understood
        if table.get("join", {}).get("type") is not None:
            if not MergeType.check_type(table["join"]["type"]):
                errors.append(
                    f"Source {source_dict['name']}, "
                    f"table {table['name']}: "
                    f"Trying to join using the type {table['join']['type']}, "
                    f"but this isn't one of the possible options: "
                    f"{MergeType.all_types()}"
                )

        # Check that the columns we want to join on make sense
        column_names = [c["name"] for c in table["columns"]]
        if table.get("join", {}).get("column") is not None:
            join_columns = table["join"]["column"].split(",")
            for col in join_columns:
                if col not in column_names and f"`{col}`" not in column_names:
                    errors.append(
                        f"Source {source_dict['name']}, "
                        f"table {table['name']}: "
                        f"Trying to join on column {join_columns}, "
                        f"but {col} isn't one of the columns on the table: "
                        f"{column_names}"
                    )

        all_suggested_names = []
        for column in table.get("columns", []):
            sub_errors = []
            has_backticks = \
                column["name"].startswith("`") and column["name"].endswith("`")
            no_backticks = column["name"].strip("`")

            # Produce one suggested name, regardless of the number of errors
            suggested_name, wrapped_name = \
                handle_name(no_backticks), handle_name(no_backticks)

            if no_backticks != suggested_name:
                sub_errors.append(
                    f"Source {source_dict['name']}, table {table['name']}: "
                    f"Column named '{column['name']}' is not possible in "
                    f"Databricks as it has illegal characters."
                    )

            # [ and ] causes dbt testing SQL to not render correctly
            if ("[" in column["name"] or "]" in column["name"]) \
                    and not has_backticks:
                sub_errors.append(
                    f"Source {source_dict['name']}, table {table['name']}: "
                    f"Column named '{column['name']}' has '[' and/or ']' "
                    f"characters, which causes issues with DBT testing. "
                    f"Remove those, or add quotes and backticks to the name "
                    f"in the schema yml file to resolve this."
                )
                suggested_name = \
                    suggested_name.replace("[", "").replace("]", "")

            # Columns starting with _ are reserved for engineering columns
            if column["name"].startswith("_") \
                    or column["name"].startswith("`_") \
                    or wrapped_name.startswith("_") \
                    or wrapped_name.startswith("`_"):
                sub_errors.append(
                    f"Source {source_dict['name']}, table {table['name']}: "
                    f"Column named '{column['name']}' starts with '_', "
                    f"which is reserved for Ingenii Engineering columns."
                )
                suggested_name = suggested_name.replace("_", "", 1)
                wrapped_name = wrapped_name.replace("_", "", 1)

            all_suggested_names.append(suggested_name)

            if sub_errors:
                errors.extend([
                    se +
                    f" Suggested name '`{wrapped_name}`' or '{suggested_name}'"
                    for se in sub_errors
                ])

        # Check no duplicate column names
        unique_column_names = set(all_suggested_names)
        if len(unique_column_names) != len(all_suggested_names):
            counts = {name: 0 for name in unique_column_names}
            for name in all_suggested_names:
                counts[name] += 1
            errors.append(
                "Duplicate columns in schema! " + ", ".join([
                    f"{k} appears {v} times"
                    for k, v in counts.items() if v > 1
                ])
            )

    if errors:
        raise SchemaException("\n".join(errors))


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
