from datetime import datetime
from delta.tables import DeltaTable
from os import path
from pyspark.sql.functions import col, hash, lit
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, \
    StringType, TimestampType
from typing import List

from .base import OrchestrationTable
from ingenii_databricks.enums import ImportColumns, Stage
from ingenii_databricks.table_utils import get_folder_path, get_table, \
    handle_major_name, sql_table_name


class MissingEntryException(Exception):
    ...


class MissingFileException(Exception):
    ...


class ImportFileEntry(OrchestrationTable):
    """
    Object to interact with the status of an individual file's status
    """
    orch_table = "import_file"
    table_schema = [
        StructField(ImportColumns.HASH, IntegerType(), nullable=False),
        StructField(ImportColumns.SOURCE, StringType(), nullable=False),
        StructField(ImportColumns.TABLE, StringType(), nullable=False),
        StructField(ImportColumns.FILE_NAME, StringType(), nullable=False),
        StructField(
            ImportColumns.PROCESSED_FILE_NAME, StringType(), nullable=True
        ),
        StructField(ImportColumns.INCREMENT, IntegerType(), nullable=False)
    ] + [
        StructField(
            ImportColumns.date_stage(s), TimestampType(),
            nullable=bool(s != Stage.NEW)
        )
        for s in Stage.ORDER
    ] + [
        StructField(ImportColumns.ROWS_READ, IntegerType(), nullable=True),
        StructField(
            ImportColumns.DATE_ROW_INSERTED, TimestampType(), nullable=False
        ),
        StructField(
            ImportColumns.DATE_ROW_UPDATED, TimestampType(), nullable=True
        )
    ]
    primary_keys = (
        ImportColumns.SOURCE, ImportColumns.TABLE, ImportColumns.FILE_NAME
    )

    _details = {}
    deleted = False

    @property
    def source(self):
        return self._details[ImportColumns.SOURCE]

    @property
    def table(self):
        return self._details[ImportColumns.TABLE]

    @property
    def file_name(self):
        return self._details[ImportColumns.FILE_NAME]

    @property
    def processed_file_name(self):
        return self._details[ImportColumns.PROCESSED_FILE_NAME]

    @property
    def hash(self):
        return self._details[ImportColumns.HASH]

    @hash.setter
    def hash(self, value):
        self._details[ImportColumns.HASH] = value

    @property
    def increment(self):
        return self._details[ImportColumns.INCREMENT]

    @increment.setter
    def increment(self, value):
        self._details[ImportColumns.INCREMENT] = value

    def __init__(self, spark: SparkSession, row_hash: int = None,
                 source_name: str = None, table_name: str = None,
                 file_name: str = None, processed_file_name: str = None,
                 increment: int = 0, extra_stages: List[str] = [],
                 create_if_missing: bool = True):
        """
        Create an ImportFileEntry instance for a specific file to be ingested

        Parameters
        ----------
        spark : SparkSession
            Object for interacting with Delta tables
        row_hash : int, optional
            The hash for the unique combination of source name, table name,
            and file name, by default None
        source_name : str, optional
            The name of the source the data is coming from, by default None
        table_name : str, optional
            The name of the table the data belongs to, by default None
        file_name : str, optional
            The name of the file the data is contained in, by default None
        processed_file_name : str, optional
            If the file needs pre-processing, the name of the file that's
            produced, by default None
        increment : int, optional
            The increment of the 'attempt' to ingest the date, by default 0.
            For each cleaning stage, if there are errors a new entry is added
            with an incremented value
        extra_stages : list, optional
            List of names of any extra stages that should be intialised, by
            default []. Where we are creating 'review' entries the data is
            already ingested, so we can use this to populate the 'stage' and
            'archive' stages
        create_if_missing : bool, optional
            Whether to create the entry if it can't be found, by default True

        Raises
        ------
        Exception
            If not enough information is passed to initialise
        """
        super(ImportFileEntry, self).__init__(spark)

        self.increment = increment

        if row_hash is not None:
            # Passing the hash assumes the entry already exists
            self.hash = row_hash
        elif source_name is None or table_name is None or file_name is None:
            raise Exception(" ".join([
                "Not enough information to find or create an entry!",
                "Hash is not passed; we therefore need source_name, "
                "table_name, and file_name, but we are missing",
                " and ".join([s for s in [
                    "source_name" if source_name else None,
                    "table_name" if table_name else None,
                    "file_name" if file_name else None
                    ] if s])
                ]))
        else:
            import_entry = self.get_import_table_df().where(
                (col(ImportColumns.SOURCE) == source_name) &
                (col(ImportColumns.TABLE) == table_name) &
                (col(ImportColumns.FILE_NAME) == file_name) &
                (col(ImportColumns.INCREMENT) == increment))
            if import_entry.rdd.isEmpty():

                if not create_if_missing:
                    raise MissingEntryException(
                        f"Unable to find entry with details "
                        f"{ImportColumns.SOURCE} = '{source_name}', "
                        f"{ImportColumns.TABLE} = '{table_name}', "
                        f"{ImportColumns.FILE_NAME} = '{file_name}', "
                        f"and {ImportColumns.INCREMENT} = {increment}"
                    )

                # Create new entry
                self.create_import_entry(
                    source_name, table_name, file_name, processed_file_name,
                    increment, extra_stages=extra_stages)
            else:
                self.hash = import_entry.first().hash

        self.get_import_entry()

    # Orchestration table

    def get_import_table(self):
        """
        Get the table

        Returns
        -------
        DeltaTable
            A representation of the table that can be queried
        """
        return self.get_orch_table()

    def get_import_table_df(self):
        """
        Get or create the orchestration.import_file table, but return it as a
        DataFrame

        Returns
        -------
        DataFrame
            A representation of the table that can be queried
        """
        return self.get_orch_table_df()

    def create_import_entry(self, source_name: str, table_name: str,
                            file_name: str, processed_file_name: str,
                            increment: int, extra_stages: List[str]) -> None:
        """
        Given the details of a file, create an entry on the import table

        Parameters
        ----------
        source_name : str
            The name of the source the data is coming from
        table_name : str
            The name of the table the data belongs to
        file_name : str
            The name of the file the data is contained in
        increment : int
            The increment of the 'attempt' to ingest the date. For each
            cleaning stage, if there are errors a new entry is added with an
            incremented value
        extra_stages : List[str]
            List of names of any extra stages that should be intialised.
            Where we are creating 'review' entries the data is already
            ingested, so we can use this to populate the 'stage' and 'archive'
            stages
        """
        # Catch if wrong 'increment' value used
        if increment > 0 and self.get_import_table_df().where(
                (col(ImportColumns.SOURCE) == source_name) &
                (col(ImportColumns.TABLE) == table_name) &
                (col(ImportColumns.FILE_NAME) == file_name) &
                (col(ImportColumns.INCREMENT) == increment - 1)
                ).rdd.isEmpty():
            raise MissingEntryException(
                f"Trying to create entry with increment {increment}, "
                f"but entry with increment {increment - 1} does not "
                f"exist yet!")

        # Check that the file we want to ingest exists
        def mnt_path(*args):
            return "/".join(["", "mnt", *args])

        expected_paths = [
            mnt_path("raw", source_name, table_name, file_name),
            mnt_path("archive", source_name, table_name, file_name),
        ]
        if processed_file_name:
            expected_paths.extend([
                mnt_path("archive",
                         source_name, table_name, processed_file_name),
                mnt_path("archive", "before_pre_processing",
                         source_name, table_name, file_name),
            ])
        if not any([
            path.exists("/dbfs" + e_path)
            for e_path in expected_paths
                ]):
            raise MissingFileException(
                f"Trying to create a new orchestration.import_file "
                f"entry, but file does not exist! Can't see a file "
                f"at any of {expected_paths}"
            )

        # Create the entry
        datetime_now = datetime.utcnow()
        new_entry = self.spark.createDataFrame(
            data=[(
                    source_name, table_name, file_name, processed_file_name,
                    increment,
                    datetime_now, datetime_now
                ) + tuple(datetime_now for _ in extra_stages)
            ],
            schema=StructType([
                f for f in self.table_schema
                if f.name in (
                    ImportColumns.SOURCE, ImportColumns.TABLE,
                    ImportColumns.FILE_NAME, ImportColumns.PROCESSED_FILE_NAME,
                    ImportColumns.INCREMENT,
                    ImportColumns.date_stage(Stage.NEW),
                    ImportColumns.DATE_ROW_INSERTED
                    ) + tuple(
                        ImportColumns.date_stage(s) for s in extra_stages
                    )
            ])).withColumn(ImportColumns.HASH, hash(*self.primary_keys))

        self.get_import_table().alias("target").merge(
                new_entry.alias("new_data"),
                " AND ".join([
                    f"target.{col} = new_data.{col}"
                    for col in (ImportColumns.HASH, ImportColumns.INCREMENT)
                ])
            ) \
            .whenNotMatchedInsert(values={
                c: f"new_data.{c}" for c in new_entry.columns}) \
            .execute()

        self.hash = new_entry.first().hash

    def get_import_entry(self) -> None:
        """
        Given the hash and increment, find the entry
        """
        result = self.get_import_table_df().where(
            (col(ImportColumns.HASH) == self.hash) &
            (col(ImportColumns.INCREMENT) == self.increment)
        )
        if result.rdd.isEmpty():
            raise MissingEntryException(
                f"Unable to find orchestration.import_file entry with "
                f"{ImportColumns.HASH} = {self.hash} and "
                f"{ImportColumns.INCREMENT} = {self.increment}"
            )

        self._details = result.first().asDict()

    # Utilities

    def get_current_stage(self) -> Stage:
        """
        Get the name of the stage the entry is currently at

        Returns
        -------
        Stage
            The name of the stage (new, staged, cleaned, etc.)
        """
        if self.deleted:
            return "deleted"
        return [
            s for s in Stage.ORDER
            if self._details[ImportColumns.date_stage(s)] is not None
        ][-1]

    def is_stage(self, stage_to_compare: Stage) -> bool:
        """
        Check if the entry is at the stage passed as a parameter

        Parameters
        ----------
        stage_to_compare : Stage
            Name of the stage to check

        Returns
        -------
        bool
            Whether the entry is at this stage
        """
        return self.get_current_stage() == stage_to_compare

    def update_status(self, status_name: Stage) -> None:
        """
        Update this entry's status

        Parameters
        ----------
        status_name : Stage
            The name of the status to update to
        """
        self.get_import_entry()

        if self._details[ImportColumns.date_stage(status_name)] is None:
            dt = lit(datetime.utcnow())
            self.get_import_table().update(
                (col(ImportColumns.HASH) == self.hash) &
                (col(ImportColumns.INCREMENT) == self.increment),
                {
                    ImportColumns.date_stage(status_name): dt,
                    ImportColumns.DATE_ROW_UPDATED: dt
                })

            self.get_import_entry()

    def update_rows_read(self, n_rows: int) -> None:
        """
        Once we have read the source file, add to the import_file table the
        number of rows it has

        Parameters
        ----------
        n_rows : int
            The number of rows to update the entry with
        """
        self.get_import_table().update(
            (col(ImportColumns.HASH) == self.hash) &
            (col(ImportColumns.INCREMENT) == self.increment),
            {
                ImportColumns.ROWS_READ: lit(n_rows),
                ImportColumns.DATE_ROW_UPDATED: lit(datetime.utcnow())
            })

        self.get_import_entry()

    def get_full_table_name(self, table_name: str) -> str:
        """
        Get the name of a table including the database name

        Parameters
        ----------
        table_name : str
            The name of the table

        Returns
        -------
        str
            The full name
        """
        return sql_table_name(self.source, table_name)

    def get_base_table_name(self) -> str:
        """
        Give the base table name, which increment numbers can be built from

        Returns
        -------
        str
            The name of the table
        """
        return handle_major_name(self.table) + "_" + \
            str(self.hash).replace("-", "m")

    # Pre-processing

    def add_processed_file_name(self, processed_file_name: str) -> None:
        """
        If the file has been pre-processed, add the resultant name to the entry

        Parameters
        ----------
        processed_file_name : str
            The file name to add to the entry
        """
        self.get_import_table().update(
            (col(ImportColumns.HASH) == self.hash) &
            (col(ImportColumns.INCREMENT) == self.increment),
            {
                ImportColumns.PROCESSED_FILE_NAME: lit(processed_file_name),
                ImportColumns.DATE_ROW_UPDATED: lit(datetime.utcnow())
            })

        self.get_import_entry()

    def delete_entry(self):
        """
        Delete the entry and don't continue with the file
        """
        self.deleted = True
        self.get_import_table().delete(f"{ImportColumns.HASH} = {self.hash}")

    # File table

    def add_current_increment(self, string_to_add: str) -> str:
        """
        Given a folder path, name, or similar, add the current increment

        Parameters
        ----------
        string_to_add : str
            The string to add the increment to (e.g. table name)

        Returns
        -------
        str
            The enriched string
        """
        if self.increment > 0:
            return string_to_add + "_" + str(self.increment)
        else:
            return string_to_add

    def get_file_path(self) -> str:
        """
        Get the full path to the file to ingest

        Returns
        -------
        str
            The full file path
        """
        return get_folder_path("raw", self.source, self.table) + \
            "/" + self.file_name

    def get_file_table_name(self) -> str:
        """
        Get the name of the staged file table

        Returns
        -------
        str
            The name of the table
        """
        return self.add_current_increment(self.get_base_table_name())

    def get_full_file_table_name(self) -> str:
        """
        Get the full name of the staged file table, including the database
        name in the form <database name>.<table name>

        Returns
        -------
        str
            The name of the database and table
        """
        return self.get_full_table_name(self.get_file_table_name())

    def get_file_table_folder_path(self) -> str:
        """
        Get the folder path for the staged file table

        Returns
        -------
        str
            The folder path
        """
        return get_folder_path(
            "source", self.source, self.get_file_table_name())

    def get_file_table(self) -> DeltaTable:
        """
        Get a representation of the staged file table that can be queried

        Returns
        -------
        DeltaTable
            The representation of the table
        """
        return get_table(self.spark, self.get_file_table_folder_path())

    # Review table

    def get_review_table_name(self) -> str:
        """
        Get the name of the review table, that any rows with errors can be
        moved to

        Returns
        -------
        str
            The name of the table
        """
        return f"{self.get_base_table_name()}_{str(self.increment + 1)}"

    def get_full_review_table_name(self) -> str:
        """
        Get the full name of the review table, including the database name in
        the form <database name>.<table name>, that any rows with errors can be
        moved to

        Returns
        -------
        str
            The name of the database and table
        """
        return self.get_full_table_name(self.get_review_table_name())

    def get_review_table_folder_path(self) -> str:
        """
        Get the folder path for the review file table

        Returns
        -------
        str
            The folder path
        """
        return get_folder_path(
            "source", self.source, self.get_review_table_name()
        )

    def get_review_table(self) -> DeltaTable:
        """
        Get a representation of the review table that can be queried

        Returns
        -------
        DeltaTable
            The representation of the table
        """
        return get_table(self.spark, self.get_review_table_folder_path())

    def create_review_table_entry(self) -> 'ImportFileEntry':
        """
        Create an entry for a review table based on the current details

        Returns
        -------
        ImportFileEntry
            The entry for the review table
        """
        return ImportFileEntry(
            self.spark,
            source_name=self.source,
            table_name=self.table,
            file_name=self.file_name,
            processed_file_name=self.processed_file_name,
            increment=self.increment + 1,
            extra_stages=[Stage.ARCHIVED, Stage.STAGED])

    # Archive file

    def get_archive_path(self) -> str:
        """
        Get the file path we should move the file to for archive purposes

        Returns
        -------
        str
            The file path
        """
        if self.processed_file_name:
            return get_folder_path("archive", self.source, self.table) + \
                "/" + self.processed_file_name
        else:
            return get_folder_path("archive", self.source, self.table) + \
                "/" + self.file_name

    # Source table

    def get_source_table_name(self) -> str:
        """
        Get the name of the source table, that the data will eventually be
        moved to once cleaned

        Returns
        -------
        str
            The name of the table
        """
        return handle_major_name(self.table)

    def get_full_source_table_name(self) -> str:
        """
        Get the full name of the source table, including the database name in
        the form <database name>.<table name>, that the data will eventually be
        moved to once cleaned

        Returns
        -------
        str
            The name of the database and table
        """
        return self.get_full_table_name(self.get_source_table_name())

    def get_source_table_folder_path(self) -> str:
        """
        Get the folder path for the source table

        Returns
        -------
        str
            The folder path
        """
        return get_folder_path("source", self.source, self.table)

    def get_source_table(self) -> DeltaTable:
        """
        Get a representation of the source table that can be queried

        Returns
        -------
        DeltaTable
            The representation of the table
        """
        return get_table(self.spark, self.get_source_table_folder_path())
