class StageObj:
    """ Base object to enable comparisons """

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    @staticmethod
    def _check_type(other):
        if not isinstance(other, (StageObj, str)):
            raise Exception(
                f"Comparison must be to another StageObj object or string, "
                f"not {type(other)}")

    def __eq__(self, other):
        self._check_type(other)
        return self.name == other

    def __ne__(self, other):
        self._check_type(other)
        return not self.__eq__(other)

    def __lt__(self, other):
        self._check_type(other)
        return Stage.ORDER.index(self) < Stage.ORDER.index(other)

    def __le__(self, other):
        self._check_type(other)
        return Stage.ORDER.index(self) <= Stage.ORDER.index(other)

    def __gt__(self, other):
        self._check_type(other)
        return Stage.ORDER.index(self) > Stage.ORDER.index(other)

    def __ge__(self, other):
        self._check_type(other)
        return Stage.ORDER.index(self) >= Stage.ORDER.index(other)


class Stage:
    """ The stages each file ingestion goes through """

    NEW = StageObj("new")
    ARCHIVED = StageObj("archived")
    STAGED = StageObj("staged")
    CLEANED = StageObj("cleaned")
    INSERTED = StageObj("inserted")
    COMPLETED = StageObj("completed")

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

    @classmethod
    def date_stage(cls, stage_name):
        if stage_name not in Stage.ORDER:
            raise Exception(
                f"Stage not recognised: {stage_name}. "
                f"Possible stages: {Stage.ORDER}"
            )

        return f"date_{stage_name}"
