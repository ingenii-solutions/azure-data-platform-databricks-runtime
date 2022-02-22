from datetime import datetime
import logging
from os import environ, mkdir, path
from py4j.protocol import Py4JJavaError
from pyspark.dbutils import DBUtils
from pyspark.sql.functions import lit
from pyspark.sql.session import SparkSession
from shutil import move
from typing import List, Union

from ingenii_data_engineering.dbt_schema import add_individual_table, \
    get_project_config, get_source, get_table_def, revert_yml
from ingenii_data_engineering.pre_process import PreProcess

from ingenii_databricks.dbt_utils import clear_dbt_log_file, \
    create_unique_id, find_node_order, get_errors_from_stdout, \
    get_nodes_and_dependents, MockDBTError, move_dbt_log_file, run_dbt_command
from ingenii_databricks.enums import MergeType, Stage
from ingenii_databricks.orchestration import ImportFileEntry
from ingenii_databricks.table_utils import create_database, create_table, \
    delete_table, delete_table_data, insert_dataframe_into_table, is_table, \
    is_table_metadata, merge_dataframe_into_table, read_file
from ingenii_databricks.validation import check_parameters, \
    check_source_schema, compare_schema_and_table

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
            delete_table_data(spark, import_entry.source, import_entry.table)
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


class PipelineExecution:
    """ Class of the individual stages to stage, test, and ingest a file """

    def __init__(self, spark, dbutils):
        self.spark = spark
        self.dbutils = dbutils

        self.databricks_dbt_token = None
        self.dbt_root_folder = environ["DBT_ROOT_FOLDER"]
        self.log_target_folder = environ["DBT_LOGS_FOLDER"]

    def get_parameter(self, parameter_name: str) -> Union[str, None]:
        """
        Obtain a parameter of the pipeline. If it hasn't been passed, then
        return None

        Parameters
        ----------
        parameter_name : str
            The name of the parameter to get

        Returns
        -------
        Union[str, None]
            Either the parameter value, or None
        """

        try:
            return self.dbutils.widgets.get(parameter_name)
        except Py4JJavaError:
            return

    def initialise(self):
        """
        Initialise the data load:
            Obtain and parse the parameters
            Get the table schema
            Find or create the import_file entry
        """

        # Obtain parameters
        file_name = self.get_parameter("file_name")
        increment = self.get_parameter("increment")
        source = self.get_parameter("source")
        table_name = self.get_parameter("table")
        file_path = self.get_parameter("file_path")

        if (source is None or table_name is None) and file_path is not None:
            source, table_name = \
                file_path.replace("raw/", "").strip("/").split("/")

        check_parameters(source, table_name, file_path, file_name, increment)

        # Passed from widget as a string
        increment = int(increment)

        source_details = get_source(self.dbt_root_folder, source)

        # Check that the schema for this particular source is acceptable
        check_source_schema(source_details)

        self.table_schema = source_details["tables"][table_name]

        # Find or create the orchestration entry
        self.import_entry = ImportFileEntry(
            self.spark, source_name=source, table_name=table_name,
            file_name=file_name, increment=increment)

        # Check that the current table schema will accept this new data
        compare_schema_and_table(
            self.spark, self.import_entry, self.table_schema)

    def archive_file(self):
        """ Archive the new file """
        if not self.import_entry.is_stage(Stage.NEW):
            return
        archive_file(self.import_entry)
        self.import_entry.update_status(Stage.ARCHIVED)

    def stage_file(self):
        """ Pre-process and stage the file """
        if self.import_entry.is_stage(Stage.ARCHIVED):
            return
        pre_process_file(self.import_entry)

        # Create individual table in the source database
        n_rows = create_file_table(
            self.spark, self.import_entry, self.table_schema)
        self.import_entry.update_rows_read(n_rows)
        self.import_entry.update_status(Stage.STAGED)

    def get_dbt_token(self):
        """ Get the dbt token from Databricks secrets """
        return self.dbutils.secrets.get(
            scope=environ["DBT_TOKEN_SCOPE"], key=environ["DBT_TOKEN_NAME"])

    def test_data(self):
        """ Test the data, moving any problem data to a separate table """
        if self.import_entry.is_stage(Stage.STAGED):
            return

        # Create temporary .yml to identify this as a source, including tests
        # Run the tests
        # Move any failed rows: https://docs.getdbt.com/faqs/failed-tests
        # Run cleaning checks, moving offending entries to a review table
        prepare_individual_table_yml(
            self.table_schema["file_name"], self.import_entry)

        # Run tests and analyse the results
        self.databricks_dbt_token = self.get_dbt_token()

        testing_result = \
            test_file_table(self.import_entry, self.databricks_dbt_token,
                            self.dbt_root_folder, self.log_target_folder)

        revert_individual_table_yml(self.table_schema["file_name"])

        # If bad data, entries in the column will be NULL
        if not testing_result["success"]:
            print("Errors found while testing:")
            for error_message in testing_result["error_messages"]:
                print(f"    - {error_message}")

            if testing_result["error_sql_files"]:
                move_rows_to_review(
                    self.spark, self.import_entry, self.table_schema,
                    self.dbt_root_folder, testing_result["error_sql_files"])

                print(f"Rows with problems have been moved to review table "
                      f"{self.import_entry.get_full_review_table_name()}")
            else:
                raise Exception("\n".join([
                    "stdout:", testing_result["stdout"],
                    "stderr:", testing_result["stderr"]
                    ]))
        else:
            self.import_entry.update_status(Stage.CLEANED)

    def insert_data(self):
        """ Append / Merge into main table """
        if self.import_entry.is_stage(Stage.CLEANED):
            return

        add_to_source_table(self.spark, self.import_entry, self.table_schema)
        self.import_entry.update_status(Stage.INSERTED)

    def complete_ingestion(self):
        """ Tidying """
        if self.import_entry.is_stage(Stage.INSERTED):
            return

        remove_file_table(self.spark, self.dbutils, self.import_entry)
        self.import_entry.update_status(Stage.COMPLETED)

        # Optimize table to keep it performant
        self.spark.sql(
            "OPTIMIZE orchestration.import_file ZORDER BY (source, table)")

    def check_completion(self):
        """ Check pipeline did complete as expected """
        final_stage = self.import_entry.get_current_stage()
        if final_stage != Stage.COMPLETED:
            raise Exception(
                f"Pipeline didn't make it to completion! "
                f"Only made it to the '{final_stage}' stage!"
            )

    def propagate_data(self):
        """ Propagate this source data to downstream models and snapshots """
        # Get the DBT token if we haven't already
        databricks_dbt_token = \
            self.databricks_dbt_token or self.get_dbt_token()

        project_name = get_project_config(self.dbt_root_folder)["name"]
        propagate_source_data(
            databricks_dbt_token, project_name,
            self.import_entry.source, self.import_entry.table)
