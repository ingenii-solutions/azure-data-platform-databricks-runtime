from unittest import TestCase
from unittest.mock import Mock

from ingenii_databricks.orchestration import ImportFileEntry
from ingenii_databricks.table_utils import delete_table, delete_table_data, \
    is_table_metadata, schema_as_dict, schema_as_string, sql_table_name

file_str = "ingenii_databricks.table_utils"


class TestTableUtils(TestCase):

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

    def test_delete_table_data(self):
        for res, opts in self.table_names.items():
            for opt in opts:
                spark_session = Mock()
                delete_table_data(spark_session, opt[0], opt[1])
                spark_session.sql.assert_called_once_with(
                    f"DELETE FROM {res}"
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

    def test_sql_table_name(self):
        for res, opts in self.table_names.items():
            for opt in opts:
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

    def test_schema_as_dict(self):
        self.assertListEqual(
            schema_as_dict(self.table_schema), self.dictionary_schema
        )

    def test_schema_as_string(self):
        self.assertEqual(
            schema_as_string(self.dictionary_schema), self.string_schema
        )
