from copy import deepcopy
from os import path, walk
from shutil import copyfile, move
import yaml


def read_yml(yml_path: str) -> dict:
    """
    Read a .yml file and return a dictionary of its contents

    Parameters
    ----------
    yml_path : str
        Path to the .yml file to read

    Returns
    -------
    dict
        Object of the file's contents
    """
    with open(yml_path) as file:
        return yaml.load(file, Loader=yaml.FullLoader)


def write_yml(yml_path: str, yml_data: dict) -> None:
    """
    Write a dictionary in a .yml format

    Parameters
    ----------
    yml_path : str
        The path to write to, including the file name
    yml_data : dict
        The dictionar to write
    """
    with open(yml_path, "w") as file:
        yaml.dump(yml_data, file)


def backup_path(yml_path: str) -> str:
    """
    Given a path to a .yml file, return a path is can be backed up to

    Parameters
    ----------
    yml_path : str
        The path to the existing .yml

    Returns
    -------
    str
        The path the backup can be written to
    """
    return yml_path + "-original"


def backup_yml(yml_path: str) -> None:
    """
    Create a backup of the .yml so we can safely edit the original

    Parameters
    ----------
    yml_path : str
        The path of the .yml to backup
    """
    if not path.exists(backup_path(yml_path)):
        copyfile(yml_path, backup_path(yml_path))


def get_project_config(root_path: str) -> dict:
    """
    Get the configuration of thei DBT project

    Parameters
    ----------
    root_path : str
        The root path to the DBT project

    Returns
    -------
    dict
        The configuration for this DBT project
    """
    return read_yml(path.join(root_path, "dbt_project.yml"))


def find_source_ymls(root_path: str) -> list:
    """
    Find all of the .yml files that may contain source definitions

    Parameters
    ----------
    root_path : str
        Root of the DBT project

    Returns
    -------
    list[str]
        List of the full paths to each model .yml
    """
    project_config = get_project_config(root_path)

    return [
        path.join(dirpath, f)
        for source_path in project_config.get("source-paths", [])
        for (dirpath, _, file_names) in walk(path.join(root_path, source_path))
        for f in file_names
        if f.endswith(".yml") or f.endswith(".yaml")
    ]


def get_all_sources(root_path: str) -> dict:
    """
    Get all the source definitions in a dictionary form

    Parameters
    ----------
    root_path : str
        Root of the DBT project

    Returns
    -------
    dict
        Dictionary of the source and table definitions
    """
    return {
        s["name"]: {
            **s,
            "tables": {
                t["name"]: {**t, "file_name": s_yml}
                for t in s["tables"]
            }
        }
        for s_yml in find_source_ymls(root_path)
        for s in read_yml(s_yml).get("sources", [])
    }


def get_source(root_path: str, source_name: str) -> dict:
    """
    Get a specific source definition in a dictionary form

    Parameters
    ----------
    root_path : str
        Root of the DBT project
    source_name : str
        The name of the source we want to obtain

    Returns
    -------
    dict
        Dictionary of the source and table definitions
    """
    return get_all_sources(root_path)[source_name]


def get_table_schema(all_sources: dict, source_name: str, table_name: str
                     ) -> None:
    """
    Get a specific table's schema

    Parameters
    ----------
    all_sources : dict
        Dictionary that contains all the sources
    source_name : str
        The name of the source
    table_name : str
        The name of the table

    Returns
    -------
    dict
        The individual table's schema
    """
    return all_sources.get(source_name, {}) \
                      .get("tables", {}) \
                      .get(table_name, {})


def get_table_def(yml_path: str, source_name: str, table_name: str) -> dict:
    """
    Get the definition of a particular source table

    Parameters
    ----------
    yml_path : str
        Path to the .yml that contains the table definition
    source_name : str
        The name of the source
    table_name : str
        The name of the table

    Returns
    -------
    dict
        The definition for the particular table
    """

    backup_yml(yml_path)
    full_schema = read_yml(backup_path(yml_path))

    source_def, table_def = None, None
    for s in full_schema["sources"]:
        if s["name"] == source_name:
            source_def = s
            break
    for t in source_def["tables"]:
        if t["name"] == table_name:
            table_def = deepcopy(t)
            break
    return table_def


def add_individual_table(yml_path: str, source_name: str, table_def: dict
                         ) -> None:
    """
    Update the schema .yml with an entry for the file table, so that DBT
    recognises this as a table and can run tests against it

    Parameters
    ----------
    yml_path : str
        Path to the schema .yml
    source_name : str
        The source name
    table_def : dict
        The table definition to add
    """

    backup_yml(yml_path)
    full_schema = read_yml(backup_path(yml_path))

    source_def = None
    for s in full_schema["sources"]:
        if s["name"] == source_name:
            source_def = deepcopy(s)
            break

    source_def["tables"] += [table_def]

    full_schema["sources"] = [
        s for s in full_schema["sources"]
        if s["name"] != source_name
    ] + [source_def]
    write_yml(yml_path, full_schema)


def revert_yml(yml_path: str) -> None:
    """
    Overwrite the possibly edited schema with the backed up version,
    reverting any changes

    Parameters
    ----------
    yml_path : str
        Path to the .yml file we want to revert
    """
    if path.exists(backup_path(yml_path)):
        move(backup_path(yml_path), yml_path)
