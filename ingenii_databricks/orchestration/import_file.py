from datetime import datetime
from delta.tables import DeltaTable
from os import path
from pyspark.sql.functions import col, hash, lit
from pyspark.sql.types import StructField, StructType, IntegerType, \
    StringType, TimestampType
from typing import List

from .base import OrchestrationTable
from ingenii_databricks.table_utils import get_folder_path, get_table, \
    handle_name


class ImportFileEntry(OrchestrationTable):
    """
    Object to interact with the status of an individual file's status
    """
    hash = None
    increment = None
    details = {}
    orch_table = "import_file"
    stages = ["new", "staged", "archived", "cleaned", "inserted", "completed"]
    table_schema = [
        StructField("hash", IntegerType(), nullable=False),
        StructField("source", StringType(), nullable=False),
        StructField("table", StringType(), nullable=False),
        StructField("file_name", StringType(), nullable=False),
        StructField("processed_file_name", StringType(), nullable=True),
        StructField("increment", IntegerType(), nullable=False)
    ] + [
        StructField("date_" + s, TimestampType(), nullable=bool(s == "new"))
        for s in stages
    ] + [
        StructField("rows_read", IntegerType(), nullable=True),
        StructField("_date_row_inserted", TimestampType(), nullable=False),
        StructField("_date_row_updated", TimestampType(), nullable=True)
    ]
    primary_keys = ["source", "table", "file_name"]

    processed_file_name = None
    deleted = False

    @property
    def source(self):
        return self.details["source"]

    @property
    def table(self):
        return self.details["table"]

    @property
    def file_name(self):
        return self.details["file_name"]

    @property
    def processed_file_name(self):
        return self.details["processed_file_name"]

    def __init__(self, spark, row_hash=None, source_name=None,
                 table_name=None, file_name=None, processed_file_name=None,
                 increment=0, extra_stages=[]):
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
                (col("source") == source_name) &
                (col("table") == table_name) &
                (col("file_name") == file_name) &
                (col("increment") == increment))
            if import_entry.rdd.isEmpty():
                # Create new entry

                # Catch if wrong 'increment' value used
                if increment > 0 and self.get_import_table_df().where(
                        (col("source") == source_name) &
                        (col("table") == table_name) &
                        (col("file_name") == file_name) &
                        (col("increment") == increment - 1)
                        ).rdd.IsEmpty():
                    raise Exception(
                        f"Trying to create entry with increment {increment}, "
                        f"but entry with increment {increment - 1} does not "
                        f"exist yet!")

                expected_path = "/" + "/".join([
                    "mnt", "raw", source_name, table_name, file_name
                    ])
                if not path.exists("/dbfs" + expected_path):
                    raise Exception(
                        f"Trying to create a new orchestration.import_file "
                        f"entry, but file does not exist! Can't see a file "
                        f"at dbfs:{expected_path}")

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
                    "source", "table", "file_name", "processed_file_name",
                    "increment",
                    "date_new", "_date_row_inserted"
                    ) + tuple("date_" + stage for stage in extra_stages)
            ])).withColumn("hash", hash("source", "table", "file_name"))

        self.get_import_table().alias("target").merge(
                new_entry.alias("new_data"),
                "target.hash = new_data.hash AND "
                "target.increment = new_data.increment"
                ) \
            .whenNotMatchedInsert(values={
                c: f"new_data.{c}" for c in new_entry.columns}) \
            .execute()

        self.hash = new_entry.first().hash

    def get_import_entry(self) -> None:
        """
        Update this entry with the latest details
        """
        self.details = self.get_import_table_df().where(
            (col("hash") == self.hash) & (col("increment") == self.increment)
        ).first().asDict()

    # Utilities

    def get_current_stage(self) -> str:
        """
        Get the name of the stage the entry is currently at

        Returns
        -------
        str
            The name of the stage (new, staged, cleaned, etc.)
        """
        if self.deleted:
            return "deleted"
        return [
            s for s in self.stages
            if self.details["date_" + s] is not None
        ][-1]

    def is_stage(self, stage_to_compare: str) -> bool:
        """
        Check if the entry is at the stage passed as a parameter

        Parameters
        ----------
        stage_to_compare : str
            Name of the stage to check

        Returns
        -------
        bool
            Whether the entry is at this stage
        """
        return self.get_current_stage() == stage_to_compare

    def update_status(self, status_name: str) -> None:
        """
        Update this entry's status

        Parameters
        ----------
        status_name : str
            The name of the status to update to
        """
        self.get_import_entry()

        if self.details["date_" + status_name] is None:
            dt = lit(datetime.utcnow())
            self.get_import_table().update(
                (col("hash") == self.hash) &
                (col("increment") == self.increment),
                {
                    "date_" + status_name: dt,
                    "_date_row_updated": dt
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
            (col("hash") == self.hash) & (col("increment") == self.increment),
            {
                "rows_read": lit(n_rows),
                "_date_row_updated": lit(datetime.utcnow())
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
        return self.source + "." + handle_name(table_name)

    def get_base_table_name(self) -> str:
        """
        Give the base table name, which increment numbers can be built from

        Returns
        -------
        str
            The name of the table
        """
        return handle_name(self.details["table"]) + "_" + \
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
            (col("hash") == self.hash) & (col("increment") == self.increment),
            {
                "processed_file_name": lit(processed_file_name),
                "_date_row_updated": lit(datetime.utcnow())
            })

        self.get_import_entry()

    def delete_entry(self):
        """
        Delete the entry and don't continue with the file
        """
        self.deleted = True
        self.get_import_table().delete(f"hash = {self.hash}")

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
        if self.processed_file_name:
            return get_folder_path(
                "raw", self.details["source"], self.details["table"]
                ) + "/" + self.processed_file_name
        else:
            return get_folder_path(
                "raw", self.details["source"], self.details["table"]
                ) + "/" + self.details["file_name"]

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
        return self.add_current_increment(get_folder_path(
            "source", self.details["source"], self.details["table"],
            hash_identifier=self.details["hash"]))

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
        return f"{self.get_file_table_folder_path()}_{str(self.increment + 1)}"

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
            increment=self.increment + 1, extra_stages=["staged", "archived"])

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
        return handle_name(self.table)

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
