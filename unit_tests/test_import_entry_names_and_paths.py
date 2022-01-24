from unittest import TestCase
from unittest.mock import Mock, patch

from ingenii_databricks.orchestration import ImportFileEntry  # noqa: E402
from ingenii_databricks.orchestration.base import OrchestrationTable  # noqa: E402
from ingenii_databricks.enums import ImportColumns  # noqa: E402

file_str = "ingenii_databricks.orchestration"
class_str = f"{file_str}.ImportFileEntry"


class TestFileFolderPaths(TestCase):

    row_hash = 123465789
    source_name = "source_name"
    table_name = "table_name"
    file_name = "file_name"
    processed_file_name = "processed_file_name"
    increment = 0

    existing_entry_mock = Mock(
        return_value=Mock(
            where=Mock(
                return_value=Mock(
                    rdd=Mock(isEmpty=Mock(return_value=False)),
                    first=Mock(return_value=Mock(hash=row_hash))
                )
            )
        )
    )
    functions_mock = Mock()
    spark_mock = Mock()

    with \
            patch(f"{class_str}.get_import_table_df", existing_entry_mock), \
            patch(f"{class_str}.create_import_entry", Mock()), \
            patch(f"{class_str}.get_import_entry", Mock()):
        import_entry = ImportFileEntry(
            spark=spark_mock,
            source_name=source_name, table_name=table_name,
            file_name=file_name, increment=increment,
        )
    import_entry._details = {
        ImportColumns.SOURCE: source_name,
        ImportColumns.TABLE: table_name,
        ImportColumns.FILE_NAME: file_name,
        ImportColumns.PROCESSED_FILE_NAME: None,
        ImportColumns.INCREMENT: increment,
        ImportColumns.HASH: row_hash,
    }

    ########
    # General
    ########

    def test_full_table_name(self):
        self.assertEqual(
            self.import_entry.get_full_table_name(self.table_name),
            f"{self.source_name}.{self.table_name}"
        )

    def test_base_table_name(self):
        self.assertEqual(
            self.import_entry.get_base_table_name(),
            f"{self.table_name}_{self.row_hash}"
        )

    def test_add_current_increment(self):
        self.assertEqual(
            self.import_entry.add_current_increment("123456"),
            "123456"
        )

        self.import_entry.increment = 1
        self.assertEqual(
            self.import_entry.add_current_increment("123456"),
            "123456_1"
        )
        self.import_entry.increment = 3
        self.assertEqual(
            self.import_entry.add_current_increment("123456"),
            "123456_3"
        )
        self.import_entry.increment = 0

    ########
    # Individual File Table
    ########

    def test_get_file_path(self):
        self.assertEqual(
            self.import_entry.get_file_path(),
            f"/mnt/raw/{self.source_name}/{self.table_name}/{self.file_name}"
        )

    source_table_folder_path = f"/mnt/source/{source_name}/{table_name}"

    def file_table_name_and_path(self, path_suffix):
        get_table_mock = Mock()
        with patch(
                "ingenii_databricks.orchestration.import_file.get_table",
                get_table_mock):
            self.import_entry.get_file_table()
        get_table_mock.assert_called_once_with(
            self.spark_mock,
            self.source_table_folder_path + path_suffix
        )
        self.assertEqual(
            self.import_entry.get_file_table_folder_path(),
            self.source_table_folder_path + path_suffix
        )
        self.assertEqual(
            self.import_entry.get_file_table_name(),
            self.table_name + path_suffix
        )
        self.assertEqual(
            self.import_entry.get_full_file_table_name(),
            f"{self.source_name}.{self.table_name}" + path_suffix
        )

    def test_get_file_table(self):
        self.file_table_name_and_path(f"_{self.row_hash}")

        self.import_entry.increment = 1
        self.file_table_name_and_path(f"_{self.row_hash}_1")

        self.import_entry.increment = 3
        self.file_table_name_and_path(f"_{self.row_hash}_3")

        self.import_entry.increment = 0

        self.import_entry._details[ImportColumns.HASH] *= -1
        self.file_table_name_and_path(f"_m{self.row_hash}")

        self.import_entry.increment = 1
        self.file_table_name_and_path(f"_m{self.row_hash}_1")

        self.import_entry.increment = 3
        self.file_table_name_and_path(f"_m{self.row_hash}_3")

        self.import_entry.increment = 0
        self.import_entry._details[ImportColumns.HASH] *= -1

    ########
    # Review Table
    ########

    def review_table_name_and_path(self, path_suffix):
        get_table_mock = Mock()
        with patch(
                "ingenii_databricks.orchestration.import_file.get_table",
                get_table_mock):
            self.import_entry.get_review_table()
        get_table_mock.assert_called_once_with(
            self.spark_mock,
            self.source_table_folder_path + path_suffix
        )
        self.assertEqual(
            self.import_entry.get_review_table_folder_path(),
            self.source_table_folder_path + path_suffix
        )
        self.assertEqual(
            self.import_entry.get_review_table_name(),
            self.table_name + path_suffix
        )
        self.assertEqual(
            self.import_entry.get_full_review_table_name(),
            f"{self.source_name}.{self.table_name}" + path_suffix
        )

    def test_get_review_table(self):
        self.review_table_name_and_path(f"_{self.row_hash}_1")

        self.import_entry.increment = 1
        self.review_table_name_and_path(f"_{self.row_hash}_2")

        self.import_entry.increment = 3
        self.review_table_name_and_path(f"_{self.row_hash}_4")

        self.import_entry.increment = 0

        self.import_entry._details[ImportColumns.HASH] *= -1
        self.review_table_name_and_path(f"_m{self.row_hash}_1")

        self.import_entry.increment = 1
        self.review_table_name_and_path(f"_m{self.row_hash}_2")

        self.import_entry.increment = 3
        self.review_table_name_and_path(f"_m{self.row_hash}_4")

        self.import_entry.increment = 0
        self.import_entry._details[ImportColumns.HASH] *= -1

    def create_review_table_entry(self, expected_increment):
        import_file_entry_mock = Mock()
        with patch(
                "ingenii_databricks.orchestration.import_file.ImportFileEntry",
                import_file_entry_mock):
            self.import_entry.create_review_table_entry()
        import_file_entry_mock.assert_called_once_with(
            self.spark_mock,
            source_name=self.source_name,
            table_name=self.table_name,
            file_name=self.file_name,
            processed_file_name=None,
            increment=expected_increment,
            extra_stages=["archived", "staged"]
        )

    def test_create_review_table_entry(self):
        self.create_review_table_entry(1)

        self.import_entry.increment = 1
        self.create_review_table_entry(2)

        self.import_entry.increment = 3
        self.create_review_table_entry(4)

        self.import_entry.increment = 0

    ########
    # Archive File
    ########

    archive_table_folder_path = f"/mnt/archive/{source_name}/{table_name}"

    def test_get_archive_path(self):
        self.assertEqual(
            self.import_entry.get_archive_path(),
            f"{self.archive_table_folder_path}/{self.file_name}"
        )

        self.import_entry._details[ImportColumns.PROCESSED_FILE_NAME] = \
            self.processed_file_name
        self.assertEqual(
            self.import_entry.get_archive_path(),
            f"{self.archive_table_folder_path}/{self.processed_file_name}"
        )
        self.import_entry._details[ImportColumns.PROCESSED_FILE_NAME] = None

    ########
    # Source Table
    ########

    def test_get_source_table_name(self):
        self.assertEqual(
            self.import_entry.get_source_table_name(),
            self.table_name
        )

    def test_get_full_source_table_name(self):
        self.assertEqual(
            self.import_entry.get_full_source_table_name(),
            f"{self.source_name}.{self.table_name}"
        )

    def test_get_source_table_folder_path(self):
        self.assertEqual(
            self.import_entry.get_source_table_folder_path(),
            self.source_table_folder_path
        )

    def test_get_source_table(self):
        get_table_mock = Mock()
        with patch(
                "ingenii_databricks.orchestration.import_file.get_table",
                get_table_mock):
            self.import_entry.get_source_table()
        get_table_mock.assert_called_once_with(
            self.spark_mock,
            self.source_table_folder_path
        )


class TestBaseOrchestration(TestCase):

    def test_schema_as_dict(self):

        table_schema = \
            OrchestrationTable.schema_as_dict(ImportFileEntry.table_schema)
        expected_schema = [
            {"name": "hash", "data_type": "int", "nullable": False},
            {"name": "source", "data_type": "string", "nullable": False},
            {"name": "table", "data_type": "string", "nullable": False},
            {"name": "file_name", "data_type": "string", "nullable": False},
            {"name": "processed_file_name", "data_type": "string", "nullable": True},
            {"name": "increment", "data_type": "int", "nullable": False},
            {"name": "date_new", "data_type": "timestamp", "nullable": False},
            {"name": "date_archived", "data_type": "timestamp", "nullable": True},
            {"name": "date_staged", "data_type": "timestamp", "nullable": True},
            {"name": "date_cleaned", "data_type": "timestamp", "nullable": True},
            {"name": "date_inserted", "data_type": "timestamp", "nullable": True},
            {"name": "date_completed", "data_type": "timestamp", "nullable": True},
            {"name": "rows_read", "data_type": "int", "nullable": True},
            {"name": "_date_row_inserted", "data_type": "timestamp", "nullable": False},
            {"name": "_date_row_updated", "data_type": "timestamp", "nullable": True},
        ]
        self.assertListEqual(table_schema, expected_schema)
