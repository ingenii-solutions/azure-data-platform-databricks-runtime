from os import remove
from pyspark.dbutils import DBUtils
from pyspark.sql.session import SparkSession

from ingenii_databricks.enums import Stages
from ingenii_databricks.orchestration import ImportFileEntry
from ingenii_databricks.pipeline import remove_file_table


def abandon_file(spark: SparkSession, dbutils: DBUtils,
                 hash: int = None,
                 source_name: str = None, table_name: str = None,
                 file_name: str = None, increment: int = 0):
    import_entry = ImportFileEntry(
        hash=hash,
        source_name=source_name, table_name=table_name, file_name=file_name,
        increment=increment
    )

    current_stage = import_entry.get_current_stage()

    # Check if file has been ingested to the main table
    if current_stage >= Stages.INSERTED:
        raise Exception("File has already been ingested to main table!")

    # If individual table, remove individual table data and metadata
    if current_stage >= Stages.STAGED:
        remove_file_table(spark, dbutils, import_entry)

    # If increment > 0, no file to remove
    if increment > 0:
        return

    if current_stage >= Stages.ARCHIVED:
        # If archived, remove file from there
        remove("/dbfs" + import_entry.get_archive_path())
    else:
        # If raw, remove file from there
        remove("/dbfs" + import_entry.get_file_path())

    # Remove orchestration.import_file entry
    import_entry.delete_entry()
