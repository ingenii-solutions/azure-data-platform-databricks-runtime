from datetime import datetime
from json import loads as jload
from os import environ, path, mkdir, remove, rename
from re import compile
from shutil import move
from subprocess import run
from typing import List, Tuple

from ingenii_data_engineering.dbt_schema import get_project_config

from ingenii_databricks.orchestration import ImportFileEntry

ansi_escape = compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')


def get_log_folder(dbt_root_folder: str) -> str:
    """
    Read the DBT project configuration and return the folder logs should be
    stored in

    Parameters
    ----------
    dbt_root_folder : str
        Path top the root of the DBT project

    Returns
    -------
    str
        Path to the log folder
    """

    return get_project_config(dbt_root_folder).get(
        "log-path", dbt_root_folder + "/logs")


def get_log_file_path(dbt_log_folder: str) -> str:
    """
    Return the full path to the log file

    Parameters
    ----------
    dbt_root_folder : str
        Path to the root of the DBT project

    Returns
    -------
    str
        Path to the log file
    """

    return dbt_log_folder + "/dbt.log"


def clear_dbt_log_file(dbt_root_folder: str) -> None:
    """
    If there is a log file already, remove it so the only logs are for this
    particular run

    Parameters
    ----------
    dbt_root_folder : str
        Path to the root of the DBT project
    """

    # https://docs.getdbt.com/reference/project-configs/log-path
    dbt_log_folder = get_log_folder(dbt_root_folder)
    if not path.isdir(dbt_log_folder):
        mkdir(dbt_log_folder)

    dbt_log_file = get_log_file_path(dbt_log_folder)
    if path.exists(dbt_log_file):
        remove(dbt_log_file)


def move_dbt_log_file(import_entry: ImportFileEntry, dbt_root_folder: str,
                      log_target_folder: str) -> None:
    """
    After a test, rename the log file to associate it with the run and move to
    our given location

    Parameters
    ----------
    import_entry : ImportFileEntry
        Details of the specific file we're testing
    dbt_root_folder : str
        Path to the root of the DBT project
    log_target_folder : str
        Path to the folder we want to move logs to
    """

    dbt_log_folder = get_log_folder(dbt_root_folder)
    dbt_log_file = get_log_file_path(dbt_log_folder)

    if not path.exists(dbt_log_file):
        return

    # Rename and move log file
    new_file_name = "_".join([
        "dbt", "test",
        str(import_entry.hash),
        f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.log"
    ])

    rename(dbt_log_file, path.join(dbt_log_folder, new_file_name))

    if not path.isdir(log_target_folder):
        mkdir(log_target_folder)

    move(path.join(dbt_log_folder, new_file_name),
         path.join(log_target_folder, new_file_name))


def get_errors_from_stdout(dbt_stdout: str) -> Tuple[list, list]:
    """
    Given the stdout from the 'dbt test' run, extract the error messages and
    SQL locations to identify the rows with issues

    Parameters
    ----------
    dbt_stdout : str
        The stdout of the 'dbt test' run

    Returns
    -------
    list
        List of all the error messages
    list
        List of paths to the SQL files that will identify rows with issues
    """

    error_messages, error_sql_paths = [], []
    for line in dbt_stdout.split("\n"):
        decoded_line = ansi_escape.sub('', line).strip()
        if decoded_line.startswith("Failure in test "):
            error_messages.append(decoded_line)
        if decoded_line.startswith("compiled SQL at "):
            error_sql_paths.append(
                decoded_line.replace("compiled SQL at ", ""))

    return error_messages, error_sql_paths


def run_dbt_command(databricks_dbt_token, *args):
    return run(
        ["dbt", *args, "--profiles-dir", "."],
        cwd=environ["DBT_ROOT_FOLDER"], capture_output=True, text=True,
        env={**environ, "DATABRICKS_DBT_TOKEN": databricks_dbt_token}
    )


def get_dependency_tree(databricks_dbt_token: str) -> Tuple[dict, dict]:
    """
    Find the all the links between the nodes

    Parameters
    ----------
    databricks_dbt_token : str
        The token to interact with dbt

    Returns
    -------
    dependencies: dict
        All the nodes this node depends on
    dependents: dict
        All the nodes that depend on this node
    """
    result = run_dbt_command(databricks_dbt_token, "ls", "--output", "json")

    dependencies, dependents = {}, {}

    for node_str in result.stdout.split("\n"):
        if not node_str:
            continue

        node_json = jload(node_str)

        if node_json["resource_type"] not in ["model", "snapshot", "source"]:
            continue

        dependencies[node_json["unique_id"]] = \
            node_json.get("depends_on", {}).get("nodes", [])

        for node in node_json.get("depends_on", {}).get("nodes", []):
            if node not in dependents:
                dependents[node] = []
            dependents[node].append({
                "unique_id": node_json["unique_id"],
                "name": node_json["name"],
                "schema": node_json["config"]["schema"],
                "package_name": node_json["package_name"],
            })

    return dependencies, dependents


def find_forward_nodes(dependents_tree: dict, starting_id: str) -> set:
    """
    From a starting point, find all the nodes that depend on this node

    Parameters
    ----------
    dependents_tree : dict
        All the nodes that depend on the node
    starting_id : str
        The id of the node to start from

    Returns
    -------
    set
        All the node in the tree related to this node
    """
    all_nodes = set()
    dependent_nodes = [starting_id]

    while dependent_nodes:
        all_nodes.update(dependent_nodes)
        dependent_nodes = [
            dep["unique_id"]
            for node_id in dependent_nodes
            for dep in dependents_tree.get(node_id, [])
        ]

    return all_nodes


def find_node_order(dependencies: dict, dependents: dict, starting_id: str
                    ) -> List[str]:
    """
    Given the relationships and a starting point, find the correct order to
    traverse the relevant nodes

    Parameters
    ----------
    dependencies: dict
        All the nodes this node depends on
    dependents: dict
        All the nodes that depend on this node
    starting_id : str
        The id of the node to start from

    Returns
    -------
    List[str]
        The list of nodes to traverse, in order
    """
    forward_nodes = find_forward_nodes(dependents, starting_id)

    node_order = [starting_id]

    # Until all nodes added
    while set(node_order) != forward_nodes:
        for node in node_order:

            # For all the nodes that depend on 'node'
            for depend in dependents.get(node, []):
                dep_id = depend["unique_id"]

                # Ignore if this is not in the forward tree
                if dep_id not in forward_nodes:
                    continue

                # Check if the relevant nodes this depends on have been added
                missing_dependencies = [
                    dep not in node_order
                    for dep in dependencies.get(dep_id, [])
                    if dep in forward_nodes
                ]

                # If all dependencies met
                if not any(missing_dependencies) and dep_id not in node_order:
                    node_order.append(dep_id)

    return node_order


def create_source_unique_id(project_name, schema_name, table_name):
    return f"source.{project_name}.{schema_name}.{table_name}"


def run_model(databricks_dbt_token, model_name, package_name=None):
    if package_name:
        full_name = f"{package_name}.{model_name}"
    else:
        full_name = model_name

    return run_dbt_command(databricks_dbt_token, "run", "--select", full_name)


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

    dependencies, dependents = get_dependency_tree(databricks_dbt_token)
    starting_id = create_source_unique_id(project_name, schema, table)

    errors = []

    for node in find_node_order(dependencies, dependents, starting_id):
        # Multiple package names?
        result = run_model(node)

        if result.returncode != 0:
            errors.append(result)

    if errors:
        raise Exception(
            f"Errors when running models! " +
            str([
                {"stdout": error.stdout, "stderr": error.stderr}
                for error in errors
            ])
        )
