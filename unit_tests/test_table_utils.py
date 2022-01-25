from unittest import TestCase
from unittest.mock import Mock

from ingenii_databricks.enums import MergeType
from ingenii_databricks.orchestration import ImportFileEntry
from ingenii_databricks.table_utils import _difference_condition_string, \
    _match_condition_string, create_database, delete_table, \
    delete_table_entries, delete_table_data, get_folder_path, handle_name, \
    handle_major_name, is_table_metadata, merge_dataframe_into_table, \
    schema_as_dict, schema_as_string, sql_table_name


class TestTableUtils(TestCase):

    def test_get_folder_path(self):
        self.assertEqual(
            get_folder_path("123", "456", "789"),
            "/mnt/123/456/789"
        )
        self.assertEqual(
            get_folder_path("123", "456", "789", hash_identifier="012"),
            "/mnt/123/456/789012"
        )

    def test_is_table_metadata(self):
        """ Check if the table exists based on Databricks metadata """

        spark_session = Mock(sql=Mock(return_value=Mock(collect=Mock(
            return_value=[
                Mock(tableName="table"),
                Mock(tableName="table2")
            ]
        ))))
        self.assertTrue(is_table_metadata(
            spark_session, database_name="source",  table_name="table"))

        spark_session.sql.assert_called_once_with("SHOW TABLES IN source")
        spark_session.sql().collect.assert_called_once()

        spark_session = Mock(sql=Mock(return_value=Mock(collect=Mock(
            return_value=[]
        ))))
        self.assertFalse(is_table_metadata(
            spark_session, database_name="source",  table_name="table"))

    table_names = {
        "SoUrCe.TaBle": [
            ("SoUrCe", "TaBle"),
            ("So,UrCe", "Ta;Ble"),
        ],
        "source_1.table_2": [
            ("source 1", "table 2"),
            ("source-1", "table 2"),
            ("source=1", "ta\nble\t2"),
        ],
        "source[1].table[2]": [
            ("source{1]", "table[2}"),
            ("source{1)", "table(2}"),
        ],
    }

    def test_handle_name(self):
        for res, opts in self.table_names.items():
            for opt in opts:
                self.assertEqual(
                    res,
                    f"{handle_major_name(opt[0])}.{handle_name(opt[1])}"
                )
                self.assertEqual(sql_table_name(opt[0], opt[1]), res)

    table_schema = ImportFileEntry.table_schema
    dictionary_schema = [
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
    string_schema = \
        "`hash` int NOT NULL, " \
        "`source` string NOT NULL, " \
        "`table` string NOT NULL, " \
        "`file_name` string NOT NULL, " \
        "`processed_file_name` string, " \
        "`increment` int NOT NULL, " \
        "`date_new` timestamp NOT NULL, " \
        "`date_archived` timestamp, " \
        "`date_staged` timestamp, " \
        "`date_cleaned` timestamp, " \
        "`date_inserted` timestamp, " \
        "`date_completed` timestamp, " \
        "`rows_read` int, " \
        "`_date_row_inserted` timestamp NOT NULL, " \
        "`_date_row_updated` timestamp"
    string_schema_null = \
        "`hash` int, " \
        "`source` string, " \
        "`table` string, " \
        "`file_name` string, " \
        "`processed_file_name` string, " \
        "`increment` int, " \
        "`date_new` timestamp, " \
        "`date_archived` timestamp, " \
        "`date_staged` timestamp, " \
        "`date_cleaned` timestamp, " \
        "`date_inserted` timestamp, " \
        "`date_completed` timestamp, " \
        "`rows_read` int, " \
        "`_date_row_inserted` timestamp, " \
        "`_date_row_updated` timestamp"

    def test_schema_as_dict(self):
        self.assertListEqual(
            schema_as_dict(self.table_schema), self.dictionary_schema
        )

    def test_schema_as_string(self):
        self.assertEqual(
            schema_as_string(self.dictionary_schema), self.string_schema
        )
        self.assertEqual(
            schema_as_string(self.dictionary_schema, all_null=True),
            self.string_schema_null
        )

    def test_create_database(self):
        spark_mock = Mock()
        create_database(spark_mock, "database1")

        spark_mock.sql.assert_called_once_with(
            "CREATE DATABASE IF NOT EXISTS database1")

    def test_match_condition_string(self):
        self.assertEqual(
            _match_condition_string(["col1", "col{2}", "col 4"]),
            "deltatable.col1 = dataframe.col1 AND "
            "deltatable.col[2] = dataframe.col[2] AND "
            "deltatable.col_4 = dataframe.col_4"
        )
        self.assertEqual(
            _match_condition_string("col1,col{2},col 4"),
            "deltatable.col1 = dataframe.col1 AND "
            "deltatable.col[2] = dataframe.col[2] AND "
            "deltatable.col_4 = dataframe.col_4"
        )

    def test_difference_condition_string(self):
        all_columns = ["col1", "col{2}", "col3", "col 4", "col 5", "_col_6"]
        merge_columns_1 = ["col1", "col{2}", "col_4"]
        merge_columns_2 = "col1,col[2],col 4"
        self.assertEqual(
            _difference_condition_string(all_columns, merge_columns_1),
            "deltatable.`col3` <> dataframe.`col3` OR "
            "deltatable.`col_5` <> dataframe.`col_5`"
        )
        self.assertEqual(
            _difference_condition_string(all_columns, merge_columns_2),
            "deltatable.`col3` <> dataframe.`col3` OR "
            "deltatable.`col_5` <> dataframe.`col_5`"
        )

    def test_merge_dataframe_wrong_type(self):
        self.assertRaises(
            Exception, merge_dataframe_into_table,
            Mock(), Mock(), [], MergeType.INSERT
        )
        self.assertRaises(
            Exception, merge_dataframe_into_table,
            Mock(), Mock(), [], MergeType.REPLACE
        )
        self.assertRaises(
            Exception, merge_dataframe_into_table,
            Mock(), Mock(), [], "1234567890"
        )

    all_columns = [
        "col1", "col2", "col3", "col4", "col5",
        "_date_row_inserted", "_date_row_updated"
    ]
    merge_columns = ["col1", "col2"]
    match_string = \
        "deltatable.col1 = dataframe.col1 AND deltatable.col2 = dataframe.col2"
    difference_string = \
        "deltatable.`col3` <> dataframe.`col3` OR " \
        "deltatable.`col4` <> dataframe.`col4` OR " \
        "deltatable.`col5` <> dataframe.`col5`"

    def test_merge_dataframe_merge_date_rows(self):
        merge_table_mock = Mock()
        dataframe_mock = Mock(columns=self.all_columns)
        merge_dataframe_into_table(
            merge_table_mock, dataframe_mock,
            self.merge_columns, "merge_date_rows")

        merge_table_mock.alias.assert_called_once_with("deltatable")
        dataframe_mock.alias.assert_called_once_with("dataframe")

        merge_table_mock.alias.return_value.merge.assert_called_once_with(
            dataframe_mock.alias.return_value, self.match_string
        )

        updated_table = merge_table_mock.alias.return_value.merge.return_value

        insert_call = updated_table.whenNotMatchedInsert
        insert_call.assert_called_once()
        args, kwargs = insert_call.call_args_list[0]
        self.assertTupleEqual(args, ())
        self.assertSetEqual(set(kwargs.keys()), {"values"})
        self.assertDictEqual(
            kwargs["values"],
            {
                "col1": "dataframe.col1",
                "col2": "dataframe.col2",
                "col3": "dataframe.col3",
                "col4": "dataframe.col4",
                "col5": "dataframe.col5",
                "_date_row_inserted": "dataframe._date_row_inserted",
            }
        )

        update_call = insert_call.return_value.whenMatchedUpdate
        update_call.assert_called_once()
        args, kwargs = update_call.call_args_list[0]
        self.assertTupleEqual(args, ())
        self.assertSetEqual(set(kwargs.keys()), {"condition", "set"})
        self.assertEqual(kwargs["condition"], self.difference_string)
        self.assertDictEqual(
            kwargs["set"],
            {
                "col1": "dataframe.col1",
                "col2": "dataframe.col2",
                "col3": "dataframe.col3",
                "col4": "dataframe.col4",
                "col5": "dataframe.col5",
                "_date_row_updated": "dataframe._date_row_updated",
            }
        )

        update_call.return_value.execute.assert_called_once_with()

    def test_merge_dataframe_merge_update(self):
        merge_table_mock = Mock()
        dataframe_mock = Mock(columns=self.all_columns)
        merge_dataframe_into_table(
            merge_table_mock, dataframe_mock,
            self.merge_columns, "merge_update")

        merge_table_mock.alias.assert_called_once_with("deltatable")
        dataframe_mock.alias.assert_called_once_with("dataframe")

        merge_table_mock.alias.return_value.merge.assert_called_once_with(
            dataframe_mock.alias.return_value, self.match_string
        )

        updated_table = merge_table_mock.alias.return_value.merge.return_value

        insert_call = updated_table.whenNotMatchedInsertAll
        insert_call.assert_called_once_with()

        update_call = insert_call.return_value.whenMatchedUpdateAll
        update_call.assert_called_once_with(condition=self.difference_string)

        update_call.return_value.execute.assert_called_once_with()

    def test_merge_dataframe_merge_insert(self):
        merge_table_mock = Mock()
        dataframe_mock = Mock(columns=self.all_columns)
        merge_dataframe_into_table(
            merge_table_mock, dataframe_mock,
            self.merge_columns, "merge_insert")

        merge_table_mock.alias.assert_called_once_with("deltatable")
        dataframe_mock.alias.assert_called_once_with("dataframe")

        merge_table_mock.alias.return_value.merge.assert_called_once_with(
            dataframe_mock.alias.return_value, self.match_string
        )

        updated_table = merge_table_mock.alias.return_value.merge.return_value

        insert_call = updated_table.whenNotMatchedInsertAll
        insert_call.assert_called_once_with()

        insert_call.return_value.execute.assert_called_once_with()

    def test_delete_table_entries(self):
        deltatable_mock, dataframe_mock = Mock(), Mock()
        delete_table_entries(
            deltatable_mock, dataframe_mock, self.merge_columns)

        deltatable_mock.alias.assert_called_once_with("deltatable")
        dataframe_mock.alias.assert_called_once_with("dataframe")

        merge_call = deltatable_mock.alias.return_value.merge
        merge_call.assert_called_once()
        args, kwargs = merge_call.call_args_list[0]
        self.assertTupleEqual(args, ())
        self.assertSetEqual(set(kwargs.keys()), {"condition", "source"})
        self.assertEqual(kwargs["condition"], self.match_string)
        self.assertEqual(kwargs["source"], dataframe_mock.alias.return_value)

        merge_call.return_value.whenMatchedDelete.assert_called_once_with()

    def test_delete_table_data(self):
        for res, opts in self.table_names.items():
            for opt in opts:
                spark_session = Mock()
                delete_table_data(spark_session, opt[0], opt[1])
                spark_session.sql.assert_called_once_with(
                    f"DELETE FROM {res}"
                )

    def test_delete_table(self):
        for res, opts in self.table_names.items():
            for opt in opts:
                spark_session = Mock()
                delete_table(spark_session, opt[0], opt[1])
                calls = spark_session.sql.call_args_list
                self.assertEqual(len(calls), 2)
                args, kwargs = calls[0]
                self.assertEqual(args, (f"DELETE FROM {res}",))
                self.assertDictEqual(kwargs, {})
                args, kwargs = calls[1]
                self.assertEqual(args, (f"DROP TABLE IF EXISTS {res}",))
                self.assertDictEqual(kwargs, {})
