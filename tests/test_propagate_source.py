import sys
from unittest import TestCase
from unittest.mock import Mock, patch

sys.modules["delta.tables"] = Mock()
sys.modules["pyspark.sql.dataframe"] = Mock()
sys.modules["pyspark.sql.functions"] = Mock()
sys.modules["pyspark.sql.session"] = Mock()
sys.modules["pyspark.sql.types"] = Mock()

from ingenii_databricks.dbt_utils import get_dependency_tree

file_str = "ingenii_databricks.dbt_utils"

#  1   4
#  | \ |
#  |  \|
#  |   3
#  |  /
#  |/
#  2
# From 1: 1, 3, 2
# From 4: 4, 3, 2


class TestSourcePropagation(TestCase):

    def test_get_correct_path(self):
        run_dbt_command_mock = Mock(
            return_value=Mock(
                stdout=""
            )
        )

        with patch(file_str + ".run_dbt_command", run_dbt_command_mock):
            dependencies = get_dependency_tree("123")
            print(dependencies)
            raise
