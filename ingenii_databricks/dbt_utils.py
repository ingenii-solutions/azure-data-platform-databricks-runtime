from datetime import datetime
from json import loads as jload
import logging
from os import environ, path, mkdir, remove, rename
from re import compile
from shutil import move
from subprocess import CompletedProcess, run
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


def run_dbt_command(databricks_dbt_token: str, *args) -> CompletedProcess:
    """
    Run a dbt command. Fields to pass to 'dbt' are passed to the function as
    arguments

    Parameters
    ----------
    databricks_dbt_token : str
        The token to authenticate to the cluster we are running dbt on

    Returns
    -------
    CompletedProcess
        The result of the command, including stdout and stderr
    """
    logging.info(
        f"Running dbt command 'dbt {' '.join(args)} --profiles-dir .'"
    )
    return run(
        ["dbt", *args, "--profiles-dir", "."],
        cwd=environ["DBT_ROOT_FOLDER"], capture_output=True, text=True,
        env={**environ, "DATABRICKS_DBT_TOKEN": databricks_dbt_token}
    )


def get_nodes_and_dependents(databricks_dbt_token: str) -> Tuple[dict, dict]:
    """
    Find the all the links between the nodes

    Parameters
    ----------
    databricks_dbt_token : str
        The token to interact with dbt

    Returns
    -------
    nodes: dict
        The mode details, with the unique_id as the key
    dependents: dict
        All the nodes that depend on this node
    """
    result = run_dbt_command(databricks_dbt_token, "ls", "--output", "json")

    nodes, dependents = {}, {}

    for node_str in result.stdout.split("\n"):
        if not node_str:
            continue

        node_json = jload(node_str)

        if node_json["resource_type"] not in ["model", "snapshot", "source"]:
            continue

        nodes[node_json["unique_id"]] = {
            "unique_id": node_json["unique_id"],
            "resource_type": node_json["resource_type"],
            "name": node_json["name"],
            "package_name": node_json["package_name"],
            "depends_on": node_json.get("depends_on", {}).get("nodes", [])
        }
        if node_json["resource_type"] == "model":
            nodes[node_json["unique_id"]]["schema"] = \
                node_json["config"]["schema"]
        elif node_json["resource_type"] == "snapshot":
            nodes[node_json["unique_id"]]["schema"] = \
                node_json["config"]["target_schema"]
        else:
            nodes[node_json["unique_id"]]["schema"] = \
                node_json["source_name"]

        for node in node_json.get("depends_on", {}).get("nodes", []):
            if node not in dependents:
                dependents[node] = []
            dependents[node].append(node_json["unique_id"]),

    return nodes, dependents


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
            dep
            for node_id in dependent_nodes
            for dep in dependents_tree.get(node_id, [])
        ]

    return all_nodes


def find_node_order(nodes: dict, dependents: dict, starting_id: str
                    ) -> List[str]:
    """
    Given the relationships and a starting point, find the correct order to
    traverse the relevant nodes. Does not include the starting node

    Parameters
    ----------
    nodes: dict
        All the nodes and their details, including what they depend on
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
        for node_id in node_order:

            # For all the nodes that depend on 'node'
            for dep_id in dependents.get(node_id, []):

                # Ignore if this is not in the forward tree
                if dep_id not in forward_nodes:
                    continue

                # Check if the relevant nodes this depends on have been added
                missing_dependencies = [
                    dep not in node_order
                    for dep in nodes.get(dep_id, {}).get("depends_on", [])
                    if dep in forward_nodes
                ]

                # If all dependencies met
                if not any(missing_dependencies) and dep_id not in node_order:
                    node_order.append(dep_id)

    return node_order[1:]


def create_unique_id(project_name: str, node_type: str, schema_name: str,
                     table_name: str) -> str:
    """
    Generate the unique ID for a node

    Parameters
    ----------
    project_name : str
        The name of the project
    node_type : str
        The type of the node, e.g. 'source' or 'model'
    schema_name : str
        The name of the schema, mainly relevant for source nodes
    table_name : str
        The name of the table

    Returns
    -------
    str
        The node unique ID

    Raises
    ------
    Exception
        If the node_type is not recognised
    """
    if node_type == "source":
        return f"source.{project_name}.{schema_name}.{table_name}"
    elif node_type == "model":
        return f"model.{project_name}.{table_name}"
    else:
        raise Exception(f"Can't recognise data type {node_type}")


class MockDBTError:
    returncode = 0
    stdout = ""
    stderr = ""

    def __init__(self, **kwargs) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)
