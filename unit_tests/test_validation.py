from copy import deepcopy
from pyspark.sql.utils import AnalysisException
from unittest import TestCase
from unittest.mock import Mock

from ingenii_data_engineering.dbt_schema import get_source

from ingenii_databricks.validation import check_source_schema, \
    compare_schema_and_table, SchemaException


class TestValidation(TestCase):

    source = "test_data"
    table = "table0"
    example_source = get_source("integration_tests/dbt", source)

    def test_inital_form(self):
        check_source_schema(self.example_source)

    def test_schema_name(self):
        test_source = deepcopy(self.example_source)
        test_source["schema"] += "-"

        self.assertRaises(SchemaException, check_source_schema, test_source)

    def test_table_name(self):
        test_source = deepcopy(self.example_source)
        test_source["tables"][self.table]["name"] += "-"

        self.assertRaises(SchemaException, check_source_schema, test_source)

    def test_join_type(self):
        test_source = deepcopy(self.example_source)
        test_source["tables"][self.table]["join"]["type"] += "-"

        self.assertRaises(SchemaException, check_source_schema, test_source)

    def test_join_column_name(self):
        test_source = deepcopy(self.example_source)

        col_name = test_source["tables"][self.table]["columns"][0]["name"]
        test_source["tables"][self.table]["columns"][0]["name"] = \
            "`" + col_name + "`"
        check_source_schema(test_source)

        test_source["tables"][self.table]["join"]["column"] = col_name + "-"

        self.assertRaises(SchemaException, check_source_schema, test_source)

    def test_column_names(self):
        test_source = deepcopy(self.example_source)

        for col in test_source["tables"][self.table]["columns"]:
            col["name"] = "`" + col["name"] + "`"
        check_source_schema(test_source)

        test_source = deepcopy(self.example_source)
        orig_col_name = test_source["tables"][self.table]["columns"][1]["name"]

        test_source["tables"][self.table]["columns"][1]["name"] = \
            orig_col_name + "{}"
        self.assertRaises(SchemaException, check_source_schema, test_source)

        test_source["tables"][self.table]["columns"][1]["name"] = \
            "`" + orig_col_name + "{}`"
        self.assertRaises(SchemaException, check_source_schema, test_source)

        test_source["tables"][self.table]["columns"][1]["name"] = \
            "`" + orig_col_name + "[]`"
        check_source_schema(test_source)

        test_source["tables"][self.table]["columns"][1]["name"] = \
            orig_col_name + "[]"
        self.assertRaises(SchemaException, check_source_schema, test_source)

        test_source["tables"][self.table]["columns"][1]["name"] = \
            "_" + orig_col_name
        self.assertRaises(SchemaException, check_source_schema, test_source)

        test_source["tables"][self.table]["columns"][1]["name"] = \
            "`_" + orig_col_name + "`"
        self.assertRaises(SchemaException, check_source_schema, test_source)

    def test_compare_schema_and_table(self):
        spark_mock = Mock(
            sql=Mock(return_value=Mock(
                collect=Mock(return_value=[
                    Mock(col_name="date"),
                    Mock(col_name="open"),
                    Mock(col_name="close"),
                ])
            ))
        )
        import_entry_mock = Mock(source=self.source, table=self.table)

        compare_schema_and_table(
            spark_mock, import_entry_mock,
            table_schema=self.example_source["tables"][self.table]
        )

        self.assertEqual(len(spark_mock.sql.call_args_list), 2)

        args, kwargs = spark_mock.sql.call_args_list[0]
        self.assertTupleEqual(args, (
            f"DESCRIBE TABLE {self.source}.{self.table}",
        ))
        self.assertDictEqual(kwargs, {})

        args, kwargs = spark_mock.sql.call_args_list[1]
        self.assertTupleEqual(
            args,
            (
                f"ALTER TABLE {self.source}.{self.table} ADD COLUMNS ("
                "`volume` int NOT NULL, `height` timestamp NOT NULL, "
                "`isGood` boolean NOT NULL)",
            )
        )
        self.assertDictEqual(kwargs, {})

    def test_compare_schema_and_table_all_columns(self):
        spark_mock = Mock(sql=Mock(side_effect=AnalysisException(
            "Table or view not found", "Table or view not found"
        )))
        import_entry_mock = Mock(source=self.source, table=self.table)

        compare_schema_and_table(
            spark_mock, import_entry_mock,
            table_schema=self.example_source["tables"][self.table]
        )
        spark_mock.sql.assert_called_once_with(
            f"DESCRIBE TABLE {self.source}.{self.table}")

    def test_compare_schema_and_table_no_table(self):
        spark_mock = Mock(sql=Mock(side_effect=AnalysisException(
            "Table or view not found", "Table or view not found"
        )))
        import_entry_mock = Mock(source=self.source, table=self.table)

        compare_schema_and_table(
            spark_mock, import_entry_mock,
            table_schema=self.example_source["tables"][self.table]
        )
        spark_mock.sql.assert_called_once_with(
            f"DESCRIBE TABLE {self.source}.{self.table}")
