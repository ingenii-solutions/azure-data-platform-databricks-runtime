from typing import List, Union

from .table_utils import handle_name, MergeType


class ParameterException(Exception):
    ...


class SchemaException(Exception):
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
    if source_dict["schema"] != handle_name(source_dict["schema"]):
        errors.append(
            f"Source {source_dict['name']}: "
            f"Schema named '{source_dict['schema']}' is not possible in "
            f"Databricks. Suggested name: {handle_name(source_dict['schema'])}"
            )

    for _, table in source_dict.get("tables", {}).items():
        if table["name"] != handle_name(table["name"]):
            errors.append(
                f"Source {source_dict['name']}: "
                f"Table named '{table['name']}' is not possible in "
                f"Databricks. Suggested name: {handle_name(table['name'])}"
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

            # - causes the SQL to create the columns to have issues
            if "-" in column["name"] and not has_backticks:
                sub_errors.append(
                    f"Source {source_dict['name']}, table {table['name']}: "
                    f"Column named '{column['name']}' has '-' "
                    f"characters, which causes issues with defining table "
                    f"columns in Databricks. Remove those, or add quotes and "
                    f"backticks to the column name in the schema yml file to "
                    f"resolve this."
                )
                suggested_name = suggested_name.replace("-", "_")

            # / causes the SQL to create the columns to have issues
            if "/" in column["name"] and not has_backticks:
                sub_errors.append(
                    f"Source {source_dict['name']}, table {table['name']}: "
                    f"Column named '{column['name']}' has '/' "
                    f"characters, which causes issues with defining table "
                    f"columns in Databricks. Remove those, or add quotes and "
                    f"backticks to the column name in the schema yml file to "
                    f"resolve this."
                )
                suggested_name = suggested_name.replace("/", "_or_")

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

            if sub_errors:
                errors.extend([
                    se +
                    f" Suggested name '`{wrapped_name}`' including the " +
                    f"quotes, or {suggested_name}"
                    for se in sub_errors
                ])

    if errors:
        raise SchemaException("\n".join(errors))
