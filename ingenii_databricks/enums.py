class Stages:
    """ The stages each file ingestion goes through """

    NEW = "new"
    ARCHIVED = "archived"
    STAGED = "staged"
    CLEANED = "cleaned"
    INSERTED = "inserted"
    COMPLETED = "completed"

    ORDER = [NEW, ARCHIVED, STAGED, CLEANED, INSERTED, COMPLETED]


class ImportColumns:
    """ The columns of the import_file table """

    HASH = "hash"
    SOURCE = "source"
    TABLE = "table"
    FILE_NAME = "file_name"
    PROCESSED_FILE_NAME = "processed_file_name"
    INCREMENT = "increment"
    ROWS_READ = "rows_read"
    DATE_ROW_INSERTED = "_date_row_inserted"
    DATE_ROW_UPDATED = "_date_row_updated"

    @staticmethod
    def date_stage(stage_name):
        if stage_name not in Stages.ORDER:
            raise Exception(
                f"Stage not recognised: {stage_name}. "
                f"Possible stages: {Stages.ORDER}"
            )

        return f"date_{stage_name}"
