from datetime import datetime
from os import path, mkdir, remove, rename
from re import compile
from shutil import move
from typing import Tuple

from ingenii_databricks.orchestration import ImportFileEntry
from ingenii_databricks.schema_yml import get_project_config

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
