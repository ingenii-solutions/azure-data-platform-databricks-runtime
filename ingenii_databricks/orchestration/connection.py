from abc import ABC
from typing import List


class BaseConnection(ABC):
    """
    Abstract class to base the definitions of each connection type from
    """

    # The general name of the connection type
    name = None

    # Dictionary where the keys are the names of the possible authentication
    # options e.g. 'basic', 'token'. The values are a sub-dictionary with 2
    # keys, 'required', and 'optional', and those values are a list of the
    # corresponding parameters required or optional for this connection type
    authentications = {}

    # A list of strings giving the names of the required and optional
    # parameters to define for each table
    required_table_parameters = []
    optional_table_parameters = []

    @classmethod
    def is_valid_authentication(cls, authentication_method: str) -> bool:
        return authentication_method in cls.authentications

    @classmethod
    def all_authentications(cls) -> List[str]:
        return list(cls.authentications.keys())

    @staticmethod
    def _validate_config(config: dict, required_parameters: List[str],
                         optional_parameters: List[str]) -> None:
        """
        Validate any config given the lists of parameters
        """
        errors = []

        missing_parameters = [
            rp for rp in required_parameters
            if rp not in config
        ]
        if missing_parameters:
            errors.append(f"Fields missing from config: {missing_parameters}")

        unknown_parameters = [
            c for c in config
            if c not in required_parameters + optional_parameters
        ]
        if unknown_parameters:
            errors.append(f"Unknown fields in config: {unknown_parameters}")

        if errors:
            raise Exception(errors)

    @classmethod
    def validate_config(cls, authentication: str, config: dict) -> None:
        """
        Validate the config for the data provider
        """
        cls._validate_config(
            config,
            cls.authentications[authentication].get("required", []),
            cls.authentications[authentication].get("optional", []))

    @classmethod
    def validate_table_config(cls, config: dict) -> None:
        """
        Validate the config for a specific table for a data provider
        """
        cls._validate_config(config,
                             cls.required_table_parameters,
                             cls.optional_table_parameters)


class APIConnection(BaseConnection):
    name = "api"
    authentications = {
        "token_header": {  # Token passed in an 'Authorization' header
            "required": ["base_url", "prefix", "key_vault_secret_name"]
        },
        "token_parameter": {  # Token passed as the value of parameter
            "required": ["base_url", "parameter", "key_vault_secret_name"]
        }
    }

    required_table_parameters = ["path"]
    optional_table_parameters = ["date_format", "date_from_parameter"]

    @classmethod
    def validate_table_config(cls, config) -> None:
        # Anything starting with "parameter_" is free text and does not need
        # to be checked
        check_config = {k: v
                        for k, v in config.items()
                        if not k.startswith("parameter_")}

        super(APIConnection, cls).validate_table_config(check_config)


class SFTPConnection(BaseConnection):
    name = "sftp"
    authentications = {
        "basic": ["url", "username", "key_vault_secret_name"]
    }

    required_table_parameters = ["path"]
    # TODO: implement prefix and suffix checks
    optional_table_parameters = ["zipped", "prefix", "suffix"]


class ConnectionTypes:
    """
    Overall class used to validate the configuration of the connection details
    for a data provider
    """
    connection_types = {
        conn.name: conn
        for conn in (
            APIConnection, SFTPConnection
        )
    }

    @classmethod
    def all_connections(cls):
        return [c for c in cls.connection_types]

    @classmethod
    def check_connection(cls, connection):
        return connection in cls.connection_types

    @classmethod
    def get_connection(cls, connection):
        return cls.connection_types[connection]

    @classmethod
    def all_authentications(cls, connection):
        return cls.connection_types[connection].all_authentications()

    @classmethod
    def check_authentication(cls, connection, authentication):
        return cls.connection_types[connection] \
            .is_valid_authentication(authentication)
