from ast import Import
from datetime import datetime
from pyspark.sql.types import StructField, StructType, IntegerType, \
    StringType, TimestampType
import sys
from unittest import TestCase
from unittest.mock import Mock, patch

sys.modules["delta.tables"] = Mock()
# sys.modules["pyspark"] = Mock()
# sys.modules["pyspark.sql"] = Mock()
# sys.modules["pyspark.sql.dataframe"] = Mock()
sys.modules["pyspark.sql.functions"] = functions_mock = Mock(
    hash=Mock(return_value="mock hash")
)
# sys.modules["pyspark.sql.session"] = Mock()
# sys.modules["pyspark.sql.types"] = pyspark_sql_types_mock = Mock()

from ingenii_databricks.orchestration import ImportFileEntry  # noqa: E402
from ingenii_databricks.enums import ImportColumns

file_str = "ingenii_databricks.orchestration"
class_str = f"{file_str}.ImportFileEntry"


class TestFileFolderPaths(TestCase):

    row_hash = 123465789
    source_name = "source_name"
    table_name = "table_name"
    file_name = "file_name"
    processed_file_name = "processed_file_name"
    increment = 0

    existing_import_entry_mock = Mock(
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

    with \
            patch(f"{class_str}.get_import_table_df", existing_import_entry_mock), \
            patch(f"{class_str}.create_import_entry", Mock()), \
            patch(f"{class_str}.get_import_entry", Mock()):
        import_entry = ImportFileEntry(
            spark=Mock(),
            source_name=source_name, table_name=table_name,
            file_name=file_name, increment=increment,
        )
    import_entry._details = {
        ImportColumns.SOURCE: source_name,
        ImportColumns.TABLE: table_name,
        ImportColumns.FILE_NAME: file_name,
        ImportColumns.INCREMENT: increment,
        ImportColumns.HASH: row_hash,
    }

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

    def test_get_file_path(self):
        self.assertEqual(
            self.import_entry.get_file_path(),
            f"/mnt/raw/{self.source_name}/{self.table_name}/{self.file_name}"
        )

    def test_get_file_table_name(self):
        self.assertEqual(
            self.import_entry.get_file_table_name(),
            f"{self.table_name}_{self.row_hash}"
        )

        self.import_entry.increment = 1
        self.assertEqual(
            self.import_entry.get_file_table_name(),
            f"{self.table_name}_{self.row_hash}_1"
        )
        self.import_entry.increment = 3
        self.assertEqual(
            self.import_entry.get_file_table_name(),
            f"{self.table_name}_{self.row_hash}_3"
        )
        self.import_entry.increment = 0

        self.import_entry._details[ImportColumns.HASH] *= -1
        self.assertEqual(
            self.import_entry.get_file_table_name(),
            f"{self.table_name}_m{self.row_hash}"
        )

        self.import_entry.increment = 1
        self.assertEqual(
            self.import_entry.get_file_table_name(),
            f"{self.table_name}_m{self.row_hash}_1"
        )
        self.import_entry.increment = 3
        self.assertEqual(
            self.import_entry.get_file_table_name(),
            f"{self.table_name}_m{self.row_hash}_3"
        )
        self.import_entry.increment = 0
        self.import_entry._details[ImportColumns.HASH] *= -1

    def test_full_file_table_name(self):
        self.assertEqual(
            self.import_entry.get_full_file_table_name(),
            f"{self.source_name}.{self.table_name}_{self.row_hash}"
        )

        self.import_entry._details[ImportColumns.HASH] *= -1
        self.assertEqual(
            self.import_entry.get_full_file_table_name(),
            f"{self.source_name}.{self.table_name}_m{str(self.row_hash)}"
        )
        self.import_entry._details[ImportColumns.HASH] *= -1

    def test_get_file_table_folder_path(self):
        self.assertEqual(
            self.import_entry.get_file_table_folder_path(),
            f"/mnt/source/{self.source_name}/{self.table_name}{self.row_hash}"
        )

        self.import_entry._details[ImportColumns.HASH] *= -1
        self.assertEqual(
            self.import_entry.get_file_table_folder_path(),
            f"/mnt/source/{self.source_name}/{self.table_name}-{self.row_hash}"
        )
        self.import_entry._details[ImportColumns.HASH] *= -1
