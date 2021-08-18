from typing import Union


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
