from datetime import datetime
import logging
from os import mkdir, path
from pyspark.dbutils import DBUtils
from pyspark.sql.functions import lit
from pyspark.sql.session import SparkSession
from shutil import move
from typing import List

from ingenii_data_engineering.dbt_schema import add_individual_table, \
    get_table_def, revert_yml
from ingenii_data_engineering.pre_process import PreProcess

from ingenii_databricks.dbt_utils import clear_dbt_log_file, \
    create_unique_id, find_node_order, get_errors_from_stdout, \
    get_nodes_and_dependents, MockDBTError, move_dbt_log_file, run_dbt_command
from ingenii_databricks.enums import MergeType
from ingenii_databricks.orchestration import ImportFileEntry
from ingenii_databricks.table_utils import create_database, create_table, \
    delete_table, handle_name, insert_dataframe_into_table, is_table, \
    is_table_metadata, merge_dataframe_into_table, \
    overwrite_dataframe_into_table, read_file

from pre_process.root import find_pre_process_function


def pre_process_file(import_entry: ImportFileEntry):
    """
    If the file reqires it, pre-process it to create an ingestible version

    Parameters
    ----------
    import_entry : ImportFileEntry
        Import entry for the specific file
    """

    # Find if there is a function linked to this source and table
    pre_process_function = \
        find_pre_process_function(import_entry.source, import_entry.table)
    if pre_process_function:
        pre_process_obj = PreProcess(import_entry.source,
                                     import_entry.table,
                                     import_entry.file_name)
        proceed, new_name = pre_process_function(pre_process_obj)

        if proceed:
            # If the pre-process function says it's safe to proceed
            import_entry.add_processed_file_name(new_name)
        else:
            # If the pre-process function says the data is bad
            import_entry.delete_entry()


def create_file_table(spark: SparkSession, import_entry: ImportFileEntry,
                      table_schema: dict) -> int:
    """
    Create the table related to a specific file

    Parameters
    ----------
    spark : SparkSession
        Object for interacting with Delta tables
    import_entry : ImportFileEntry
        Import entry for the specific file
    table_schema : dict
        Schema of the table for this specific file

    Returns
    -------
    int
        The number of rows read from the file
    """

    create_database(spark, import_entry.source)
    file_table = create_table(
        spark,
        import_entry.source,
        import_entry.get_file_table_name(),
        table_schema["columns"],
        import_entry.get_file_table_folder_path(),
        all_null=True)

    # File has been moved to the archive container to avoid processing in the
    # raw container
    file_data = read_file(spark, import_entry.get_archive_path(), table_schema)

    # Check the merge columns are present
    merge_columns = table_schema["join"]["column"].split(",")
    missing_merge_columns = [
        handle_name(col)
        for col in merge_columns
        if handle_name(col) not in file_data.columns
    ]
    if missing_merge_columns:
        raise Exception(
            f"Columns set as primary keys, but not present in raw file: "
            f"{','.join(missing_merge_columns)}"
        )

    merge_dataframe_into_table(
        file_table, file_data, merge_columns, MergeType.MERGE_UPDATE)

    return file_data.count()


def archive_file(import_entry: ImportFileEntry) -> None:
    """
    Move the file to the archive location

    Parameters
    ----------
    import_entry : ImportFileEntry
        Import entry for the specific file
    """

    source_folder = "/" + "/".join([
        "dbfs", "mnt", "archive", import_entry.source])
    if not path.exists(source_folder):
        mkdir(source_folder)
    source_folder += f"/{import_entry.table}"
    if not path.exists(source_folder):
        mkdir(source_folder)

    if not path.exists("/dbfs" + import_entry.get_archive_path()):
        move("/dbfs" + import_entry.get_file_path(),
             "/dbfs" + import_entry.get_archive_path())


def prepare_individual_table_yml(source_yml_path: str,
                                 import_entry: ImportFileEntry) -> None:
    """
    Prepare the schema .yml by adding an extra table corresponding to the
    individual file, so DBT can run tests against it

    Parameters
    ----------
    source_yml_path : str
        Path to the schema .yml file
    import_entry : ImportFileEntry
        Import entry for the specific file
    """
    table_def = get_table_def(
        source_yml_path, import_entry.source, import_entry.table)

    table_def["name"] = import_entry.get_file_table_name()

    add_individual_table(source_yml_path, import_entry.source, table_def)


def revert_individual_table_yml(source_yml_path: str) -> None:
    """
    Revert a schema .yml that has been edited

    Parameters
    ----------
    source_yml_path : str
        The schema .yml to revert
    """
    revert_yml(source_yml_path)


def create_source_table(spark: SparkSession, import_entry: ImportFileEntry,
                        table_schema: dict) -> None:
    """
    Create the Delta table corresponding to a particular source table. We also
    add the '_hash' column to relate data rows back to the original file, and
    the _datetime_added column to record when the data was added or updated

    Parameters
    ----------
    spark : SparkSession
        Object for interacting with Delta tables
    import_entry : ImportFileEntry
        Import entry for a specific file, though we are creating an entry for
        the overall table
    table_schema : dict
        The schema of this new table
    """
    create_database(spark, import_entry.source)

    full_table_columns = \
        table_schema["columns"] + [
            {"name": "_hash", "data_type": "int"},
            {"name": "_datetime_added", "data_type": "timestamp"},
        ]

    create_table(spark,
                 import_entry.source,
                 import_entry.get_source_table_name(),
                 full_table_columns,
                 import_entry.get_source_table_folder_path())


def test_file_table(import_entry: ImportFileEntry, databricks_dbt_token: str,
                    dbt_root_folder: str, log_target_folder: str) -> bool:
    """
    Run the DBT tests against the individual file table

    Parameters
    ----------
    import_entry : ImportFileEntry
        Import entry for the specific file
    databricks_dbt_token : str
        Token to authenticate to the Databricks cluster
    dbt_root_folder : str
        Folder path to the root of the DBT project
    log_target_folder : str
        Target folder for DBT logs to be moved to

    Returns
    -------
    dict
        Testing result
    """

    clear_dbt_log_file(dbt_root_folder)

    result = run_dbt_command(
        databricks_dbt_token, "test",
        "--models", f"source:{import_entry.get_full_file_table_name()}"
    )

    move_dbt_log_file(import_entry, dbt_root_folder, log_target_folder)

    # If all tests have passed
    if result.returncode == 0:
        return {
            "success": True,
            "stdout": result.stdout,
            "stderr": result.stderr
            }

    # If there were test errors
    error_messages, error_sql_files = get_errors_from_stdout(result.stdout)
    return {
        "success": False,
        "error_messages": error_messages,
        "error_sql_files": error_sql_files,
        "stdout": result.stdout,
        "stderr": result.stderr
        }


def get_error_where_clauses(dbt_root_folder: str, error_sql_paths: List[str]
                            ) -> List[str]:
    """
    From the list of SQL files, return the clauses needed to identify rows
    with issues

    Parameters
    ----------
    dbt_root_folder : str
        Folder path to the root of the DBT project
    error_sql_paths : List[str]
        List of paths to the SQL files that will identify rows with issues

    Returns
    -------
    List[str]
        The clauses (after the 'where' keyword in a SQL query) that identify
        rows with issues
    """

    where_clauses = []
    for file_path in error_sql_paths:
        with open(dbt_root_folder + "/" + file_path) as sql_file:
            sql_string = sql_file.read()

        where_clauses.append(sql_string.split("where")[-1].strip())

    return where_clauses


def move_rows_to_review(spark: SparkSession, import_entry: ImportFileEntry,
                        table_schema: dict, dbt_root_folder: str,
                        error_sql_paths: List[str]) -> None:
    """
    Find the rows that have issues and move them to the review table

    Parameters
    ----------
    spark : SparkSession
        Object for interacting with Delta tables
    import_entry : ImportFileEntry
        Import entry for the specific file
    table_schema : dict
        The schema of the table so we can identify the columns to merge on
    dbt_root_folder : str
        Folder path to the root of the DBT project
    error_sql_paths : List[str]
        Paths, relative to the DBT root folder, to SQL files which contain
        queries which identify data rows with issues

    Raises
    ------
    Exception
        If no paths to SQL files are passed, then no data needs to be moved
    """

    if len(error_sql_paths) == 0:
        raise Exception("No error SQL files to process!")

    review_import_entry = import_entry.create_review_table_entry()

    # Create the review table
    create_table(spark,
                 import_entry.source,
                 import_entry.get_review_table_name(),
                 table_schema["columns"],
                 import_entry.get_review_table_folder_path(),
                 all_null=True)

    where_clause = "WHERE " + " OR ".join([
        f"({wc})" for wc in get_error_where_clauses(
            dbt_root_folder, error_sql_paths)
        ])

    # Merge into review version of the file table
    review_table = import_entry.get_review_table()
    error_dataframe = spark.sql(" ".join([
        "SELECT * FROM",
        import_entry.get_full_file_table_name(),
        where_clause
    ]))

    merge_dataframe_into_table(review_table, error_dataframe,
                               table_schema["join"]["column"],
                               table_schema["join"]["type"])

    # Delete from the file table
    spark.sql(" ".join([
        "DELETE FROM",
        import_entry.get_full_file_table_name(),
        where_clause
    ]))

    review_import_entry.update_rows_read(error_dataframe.count())


def add_to_source_table(spark: SparkSession, import_entry: ImportFileEntry,
                        table_schema: dict) -> None:
    """
    Move data from an individual file table to the overall source table

    Parameters
    ----------
    spark : SparkSession
        Object for interacting with Delta tables
    import_entry : ImportFileEntry
        Import entry for the specific file
    table_schema : dict
        Schema for the overall source table
    """
    if not (
        is_table(spark, import_entry.get_source_table_folder_path()) and
        is_table_metadata(spark, database_name=import_entry.source,
                          table_name=import_entry.table)
    ):
        create_source_table(spark, import_entry, table_schema)

    file_dataframe = \
        import_entry.get_file_table().toDF() \
                    .withColumn("_hash", lit(import_entry.hash)) \
                    .withColumn("_datetime_added", lit(datetime.utcnow()))

    if file_dataframe.rdd.isEmpty():
        return

    join_obj = table_schema.get("join", {})
    join_type = join_obj.get("type", "")
    if join_type.startswith("merge"):

        source_table = import_entry.get_source_table()

        merge_dataframe_into_table(
            source_table, file_dataframe, join_obj["column"], join_type)
    else:
        # MergeType.INSERT or MergeType.REPLACE
        if join_type == MergeType.REPLACE:
            overwrite_dataframe_into_table(
                import_entry.get_source_table_folder_path(), file_dataframe)
        else:
            insert_dataframe_into_table(
                import_entry.get_source_table_folder_path(), file_dataframe)


def remove_file_table(spark: SparkSession, dbutils: DBUtils,
                      import_entry: ImportFileEntry) -> None:
    """
    Remove a file table. This deletes both the metadata and the table files

    Parameters
    ----------
    spark : SparkSession
        Object for interacting with Delta tables
    dbutils : DBUtils
        Object for interacting with many things, in this case the file system
    import_entry : ImportFileEntry
        Import entry for the specific file
    """
    delete_table(spark,
                 import_entry.source,
                 import_entry.get_file_table_name())
    # Unmanaged table, so must delete the files separately
    dbutils.fs.rm(import_entry.get_file_table_folder_path(), True)


def propagate_source_data(databricks_dbt_token: str, project_name: str,
                          schema: str, table: str) -> None:
    """
    Propagate the source data to all relevant nodes

    Parameters
    ----------
    databricks_dbt_token : str
        the token to interact with dbt
    project_name : str
        The project the source data is in
    schema : str
        The schema the source data is in
    table : str
        The source data table name

    Raises
    ------
    Exception
        If any propagation fails, raise an error
    """

    nodes, dependents = get_nodes_and_dependents(databricks_dbt_token)
    starting_id = create_unique_id(project_name, "source", schema, table)

    logging.info(f"Running dependent models for {schema}.{table}")
    print(f"Running dependent models for {schema}.{table}")

    # Find the nodes we need to process based on the source completed
    node_order = find_node_order(nodes, dependents, starting_id)
    logging.info(f"Nodes to process for {starting_id}: {node_order}")
    print(f"Nodes to process for {starting_id}: {node_order}")

    complete_nodes = {starting_id}
    errors = []

    for node_id in node_order:
        logging.info(f"Running {node_id}")
        print(f"Running {node_id}")

        # Check all the nodes this depends on have been processed
        missing_dependencies = [
            dep
            for dep in nodes[node_id]["depends_on"]
            if dep not in complete_nodes and dep in node_order
        ]
        if missing_dependencies:
            errors.append(MockDBTError(
                returncode=1,
                stderr=f"Node {node_id}, dependencies not complete: " +
                str(missing_dependencies)
            ))
            continue

        # Different command for snapshots
        if nodes[node_id]["resource_type"] == "snapshot":
            command = "snapshot"
        else:
            command = "run"

        # Create the model
        result = run_dbt_command(
            databricks_dbt_token, "--warn-error",
            command, "--select", nodes[node_id]["name"]
        )

        # Handle success or failure
        if result.returncode == 0:
            complete_nodes.add(node_id)
            logging.info(result.stdout)
            print(result.stdout)
        else:
            errors.append(result)

    if errors:
        raise Exception(
            f"Completed nodes: {complete_nodes}. "
            f"Errors when running models! " +
            str([
                {"stdout": error.stdout, "stderr": error.stderr}
                for error in errors
            ])
        )
