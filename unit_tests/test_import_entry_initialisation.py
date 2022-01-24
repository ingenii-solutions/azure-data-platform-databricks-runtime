from datetime import datetime
from pyspark.sql.types import StructField, StructType, IntegerType, \
    StringType, TimestampType
import sys
from unittest import TestCase
from unittest.mock import Mock, patch

from ingenii_databricks.orchestration import ImportFileEntry  # noqa: E402
from ingenii_databricks.orchestration.import_file import \
    MissingEntryException, MissingFileException  # noqa: E402

from unit_tests.test__mocks import functions_mock

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
    functions_mock = Mock()

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
        """ ImportEntry initialised with just the row hash """

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
        """ All is well if file in an expected path """

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
        """ Preprocessed file not in expected paths """

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
        """ File not in expected paths """

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

    def test_create_import_entry_data(self):
        """ Creates the row and inserts correctly """

        path_exists_mock = Mock(return_value=True)
        get_import_entry_mock = Mock()
        get_import_table_mock = Mock()
        new_entry_mock = Mock(
            columns=["col1", "col2"],
            first=Mock(return_value=Mock(hash=self.row_hash))
        )
        spark_mock = Mock(
            createDataFrame=Mock(
                return_value=Mock(
                    withColumn=Mock(
                        return_value=new_entry_mock
                    )
                )
            )
        )

        with \
                patch(f"{class_str}.get_import_table_df", self.no_import_entry_mock), \
                patch(f"{class_str}.get_import_entry", get_import_entry_mock), \
                patch(f"{class_str}.get_import_table", get_import_table_mock), \
                patch("os.path.exists", path_exists_mock):
            import_entry = ImportFileEntry(
                spark=spark_mock,
                source_name=self.source_name, table_name=self.table_name,
                file_name=self.file_name, increment=self.increment,
            )

        # Check import entry creation
        spark_mock.createDataFrame.assert_called_once()
        df_args, df_kwargs = spark_mock.createDataFrame.call_args_list[0]
        self.assertTupleEqual(df_args, ())
        self.assertEqual(len(df_kwargs["data"]), 1)
        self.assertTrue(isinstance(df_kwargs["schema"], StructType))

        for data, col in zip(df_kwargs["data"][0], df_kwargs["schema"]):
            self.assertTrue(isinstance(col, StructField))
            if col.name == "source":
                self.assertEqual(data, self.source_name)
                self.assertEqual(col.nullable, False)
                self.assertTrue(isinstance(col.dataType, StringType))
            elif col.name == "table":
                self.assertEqual(data, self.table_name)
                self.assertEqual(col.nullable, False)
                self.assertTrue(isinstance(col.dataType, StringType))
            elif col.name == "file_name":
                self.assertEqual(data, self.file_name)
                self.assertEqual(col.nullable, False)
                self.assertTrue(isinstance(col.dataType, StringType))
            elif col.name == "processed_file_name":
                self.assertEqual(data, None)
                self.assertEqual(col.nullable, True)
                self.assertTrue(isinstance(col.dataType, StringType))
            elif col.name == "increment":
                self.assertEqual(data, self.increment)
                self.assertEqual(col.nullable, False)
                self.assertTrue(isinstance(col.dataType, IntegerType))
            elif col.name == "date_new" or col.name == "_date_row_inserted":
                self.assertTrue(isinstance(data, datetime))
                self.assertEqual(col.nullable, False)
                self.assertTrue(isinstance(col.dataType, TimestampType))
            else:
                raise Exception(f"Unexpected column: {col.name}, {data}")

        # Check hash column addition
        spark_mock.createDataFrame.return_value.withColumn.assert_called_once()
        wc_args, wc_kwargs = \
            spark_mock.createDataFrame.return_value.withColumn.call_args_list[0]
        self.assertTupleEqual(wc_args, ("hash", functions_mock.hash.return_value))
        self.assertDictEqual(wc_kwargs, {})

        print(functions_mock.hash.call_args_list)
        hash_args, hash_kwargs = functions_mock.hash.call_args_list[-1]
        self.assertTupleEqual(hash_args, ("source", "table", "file_name"))
        self.assertDictEqual(hash_kwargs, {})

        # Check merge
        get_import_table_mock.assert_called_once_with()

        get_import_table_mock.return_value.alias.assert_called_once_with("target")
        new_entry_mock.alias.assert_called_once_with("new_data")

        merge_call = get_import_table_mock.return_value.alias.return_value.merge
        merge_call.assert_called_once_with(
            new_entry_mock.alias.return_value,
            "target.hash = new_data.hash AND target.increment = new_data.increment"
        )

        when_not_matched_insert_call = merge_call.return_value.whenNotMatchedInsert
        when_not_matched_insert_call.assert_called_once_with(values={
            col: f"new_data.{col}" for col in new_entry_mock.columns
        })

        when_not_matched_insert_call.return_value.execute.assert_called_once_with()

        # Finally
        get_import_entry_mock.assert_called_once()
        self.assertEqual(import_entry.hash, self.row_hash)
