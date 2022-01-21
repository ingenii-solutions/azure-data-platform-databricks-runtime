import sys
from unittest import TestCase
from unittest.mock import Mock, patch

sys.modules["delta.tables"] = Mock()
sys.modules["pyspark"] = Mock()
sys.modules["pyspark.sql"] = Mock()
sys.modules["pyspark.sql.dataframe"] = Mock()
sys.modules["pyspark.sql.functions"] = Mock()
sys.modules["pyspark.sql.session"] = Mock()
sys.modules["pyspark.sql.types"] = Mock()

from ingenii_databricks.orchestration import ImportFileEntry  # noqa: E402
from ingenii_databricks.orchestration.import_file import \
    MissingEntryException, MissingFileException  # noqa: E402

file_str = "ingenii_databricks.orchestration"
class_str = f"{file_str}.ImportFileEntry"


class TestInitialisation(TestCase):

    row_hash = 123465789
    source_name = "source_name"
    table_name = "table_name"
    file_name = "file_name"
    processed_file_name = "processed_file_name"
    increment = 0

    no_import_entry_mock = Mock(
        return_value=Mock(
            where=Mock(
                return_value=Mock(
                    rdd=Mock(isEmpty=Mock(return_value=True))
                )
            )
        )
    )
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

    def test_entry_doesnt_exist_create(self):
        """ Creates new entry if one doesn't exist """

        create_import_entry_mock = Mock()
        get_import_entry_mock = Mock()

        with \
                patch(f"{class_str}.get_import_table_df", self.no_import_entry_mock), \
                patch(f"{class_str}.create_import_entry", create_import_entry_mock), \
                patch(f"{class_str}.get_import_entry", get_import_entry_mock):
            ImportFileEntry(
                spark=Mock(),
                source_name=self.source_name, table_name=self.table_name,
                file_name=self.file_name, increment=self.increment,
            )

        create_import_entry_mock.assert_called_once_with(
            self.source_name, self.table_name, self.file_name,
            None, self.increment, extra_stages=[]
        )
        get_import_entry_mock.assert_called_once()

    def test_entry_doesnt_exist_create_wrong_increment(self):
        """ Increment is 1, entry with increment 0 doesn't exist """

        with patch(f"{class_str}.get_import_table_df", self.no_import_entry_mock):
            self.assertRaises(
                MissingEntryException, ImportFileEntry,
                spark=Mock(),
                source_name=self.source_name, table_name=self.table_name,
                file_name=self.file_name, increment=self.increment + 1,
            )

    def test_entry_doesnt_exist_dont_create(self):
        """ Entry doesn't exist, and set to not create one """

        with patch(f"{class_str}.get_import_table_df", self.no_import_entry_mock):
            self.assertRaises(
                MissingEntryException, ImportFileEntry,
                spark=Mock(),
                source_name=self.source_name, table_name=self.table_name,
                file_name=self.file_name, increment=self.increment,
                create_if_missing=False
            )

    def test_entry_exists(self):
        """ Entry exists, and successfully retrieved """

        create_import_entry_mock = Mock()
        get_import_entry_mock = Mock()

        with \
                patch(f"{class_str}.get_import_table_df", self.existing_import_entry_mock), \
                patch(f"{class_str}.create_import_entry", create_import_entry_mock), \
                patch(f"{class_str}.get_import_entry", get_import_entry_mock):
            import_entry = ImportFileEntry(
                spark=Mock(),
                source_name=self.source_name, table_name=self.table_name,
                file_name=self.file_name, increment=self.increment,
            )

        create_import_entry_mock.assert_not_called()
        get_import_entry_mock.assert_called_once()

        self.assertEqual(self.row_hash, import_entry.hash)

    def test_entry_initialised_with_hash(self):

        get_import_table_df_mock = Mock()
        create_import_entry_mock = Mock()
        get_import_entry_mock = Mock()

        with \
                patch(f"{class_str}.get_import_table_df", get_import_table_df_mock), \
                patch(f"{class_str}.create_import_entry", create_import_entry_mock), \
                patch(f"{class_str}.get_import_entry", get_import_entry_mock):
            import_entry = ImportFileEntry(
                spark=Mock(),
                row_hash=self.row_hash
            )

        get_import_table_df_mock.assert_not_called()
        create_import_entry_mock.assert_not_called()
        get_import_entry_mock.assert_called_once()

        self.assertEqual(self.row_hash, import_entry.hash)

    def test_create_import_entry_paths_checked(self):
        """ Creates new entry if one doesn't exist """

        path_exists_mock = Mock(return_value=True)
        get_import_entry_mock = Mock()
        spark_mock = Mock(
            createDataFrame=Mock(
                return_value=Mock(
                    withColumn=Mock(
                        return_value=Mock(
                            columns=["col1", "col2"]
                        )
                    )
                )
            )
        )

        with \
                patch(f"{class_str}.get_import_table_df", self.no_import_entry_mock), \
                patch(f"{class_str}.get_import_entry", get_import_entry_mock), \
                patch("os.path.exists", path_exists_mock):
            ImportFileEntry(
                spark=spark_mock,
                source_name=self.source_name, table_name=self.table_name,
                file_name=self.file_name, increment=self.increment,
            )

        sub_path = f"{self.source_name}/{self.table_name}/{self.file_name}"
        paths_to_check = [
            f"/dbfs/mnt/raw/{sub_path}",
            f"/dbfs/mnt/archive/{sub_path}",
        ]

        paths_checked = [call[0][0] for call in path_exists_mock.call_args_list]
        self.assertEqual(len(paths_to_check), len(paths_checked))
        self.assertEqual(set(paths_to_check), set(paths_checked))

        # Finally
        get_import_entry_mock.assert_called_once()

    def test_create_import_entry_paths_checked_preprocess(self):
        """ Creates new entry if one doesn't exist """

        path_exists_mock = Mock(return_value=True)
        get_import_entry_mock = Mock()
        spark_mock = Mock(
            createDataFrame=Mock(
                return_value=Mock(
                    withColumn=Mock(
                        return_value=Mock(
                            columns=["col1", "col2"]
                        )
                    )
                )
            )
        )

        with \
                patch(f"{class_str}.get_import_table_df", self.no_import_entry_mock), \
                patch(f"{class_str}.get_import_entry", get_import_entry_mock), \
                patch("os.path.exists", path_exists_mock):
            ImportFileEntry(
                spark=spark_mock,
                source_name=self.source_name, table_name=self.table_name,
                file_name=self.file_name, increment=self.increment,
                processed_file_name=self.processed_file_name
            )

        sub_path = f"{self.source_name}/{self.table_name}/{self.file_name}"
        sub_path_2 = f"{self.source_name}/{self.table_name}/{self.processed_file_name}"
        paths_to_check = [
            f"/dbfs/mnt/raw/{sub_path}",
            f"/dbfs/mnt/archive/{sub_path}",
            f"/dbfs/mnt/archive/{sub_path_2}",
            f"/dbfs/mnt/archive/before_pre_processing/{sub_path}",
        ]

        paths_checked = [call[0][0] for call in path_exists_mock.call_args_list]
        self.assertEqual(len(paths_to_check), len(paths_checked))
        self.assertEqual(set(paths_to_check), set(paths_checked))

        # Finally
        get_import_entry_mock.assert_called_once()

    def test_create_import_entry_file_missing(self):
        """ Creates new entry if one doesn't exist """

        path_exists_mock = Mock(return_value=False)
        get_import_entry_mock = Mock()
        with \
                patch(f"{class_str}.get_import_table_df", self.no_import_entry_mock), \
                patch(f"{class_str}.get_import_entry", get_import_entry_mock), \
                patch("os.path.exists", path_exists_mock):

            self.assertRaises(
                MissingFileException, ImportFileEntry,
                spark=Mock(),
                source_name=self.source_name, table_name=self.table_name,
                file_name=self.file_name, increment=self.increment,
            )

        get_import_entry_mock.assert_not_called()
