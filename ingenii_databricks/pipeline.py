from datetime import datetime
from os import environ, mkdir, path
from pyspark.dbutils import DBUtils
from pyspark.sql.functions import lit
from pyspark.sql.session import SparkSession
from shutil import move
from subprocess import run
from typing import List

from ingenii_data_engineering.dbt_schema import add_individual_table, \
    get_table_def, revert_yml

from ingenii_databricks.dbt_utils import clear_dbt_log_file, \
    get_errors_from_stdout, move_dbt_log_file
from ingenii_databricks.orchestration import ImportFileEntry
from ingenii_databricks.table_utils import create_database, create_table, \
    delete_table, insert_dataframe_into_table, is_table, \
    merge_dataframe_into_table, MergeType, read_file


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

    create_database(spark, import_entry.get_source_name())
    file_table = create_table(
        spark,
        import_entry.get_source_name(),
        import_entry.get_file_table_name(),
        table_schema["columns"],
        import_entry.get_file_table_folder_path())

    file_data = read_file(spark, import_entry.get_file_path(), table_schema)

    merge_dataframe_into_table(
        file_table, file_data, table_schema["join"]["column"],
        MergeType.MERGE_UPDATE)

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
        "dbfs", "mnt", "archive", import_entry.get_source_name()])
    if not path.exists(source_folder):
        mkdir(source_folder)
    source_folder += f"/{import_entry.details['table']}"
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
    table_def = get_table_def(source_yml_path,
                              import_entry.details["source"],
                              import_entry.details["table"], )

    table_def["name"] = import_entry.get_file_table_name()

    add_individual_table(source_yml_path,
                         import_entry.details["source"],
                         table_def)


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
    create_database(spark, import_entry.get_source_name())

    full_table_columns = \
        table_schema["columns"] + [
            {"name": "_hash", "data_type": "int"},
            {"name": "_datetime_added", "data_type": "timestamp"},
        ]

    create_table(spark,
                 import_entry.get_source_name(),
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

    res = run(
        f"dbt test --profiles-dir . --models "
        f"source:{import_entry.get_full_file_table_name()}".split(),
        cwd=dbt_root_folder, capture_output=True, text=True,
        env={**environ, "DATABRICKS_DBT_TOKEN": databricks_dbt_token}
        )

    move_dbt_log_file(import_entry, dbt_root_folder, log_target_folder)

    # If all tests have passed
    if res.returncode == 0:
        return {
            "success": True,
            "stdout": res.stdout,
            "stderr": res.stderr
            }

    # If there were test errors
    error_messages, error_sql_files = get_errors_from_stdout(res.stdout)
    return {
        "success": False,
        "error_messages": error_messages,
        "error_sql_files": error_sql_files,
        "stdout": res.stdout,
        "stderr": res.stderr
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
    create_table(
        spark,
        import_entry.get_source_name(),
        import_entry.get_review_table_name(),
        table_schema["columns"],
        import_entry.get_review_table_folder_path())

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
    if not is_table(spark, import_entry.get_source_table_folder_path()):
        create_source_table(spark, import_entry, table_schema)

    file_dataframe = \
        import_entry.get_file_table().toDF() \
                    .withColumn("_hash", lit(import_entry.hash)) \
                    .withColumn("_datetime_added", lit(datetime.utcnow()))

    if file_dataframe.rdd.isEmpty():
        return

    join_obj = table_schema.get("join", {})
    if join_obj.get("type", "").startswith("merge"):

        source_table = import_entry.get_source_table()

        merge_dataframe_into_table(
            source_table, file_dataframe, join_obj["column"], join_obj["type"])
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
                 import_entry.get_source_name(),
                 import_entry.get_file_table_name())
    # Unmanaged table, so must delete the files separately
    dbutils.fs.rm(import_entry.get_file_table_folder_path(), True)
