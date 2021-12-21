from json import dumps
import sys
from unittest import TestCase
from unittest.mock import Mock, patch

sys.modules["delta.tables"] = Mock()
sys.modules["pyspark.sql.dataframe"] = Mock()
sys.modules["pyspark.sql.functions"] = Mock()
sys.modules["pyspark.sql.session"] = Mock()
sys.modules["pyspark.sql.types"] = Mock()

from ingenii_databricks.dbt_utils import get_dependency_tree, find_forward_nodes

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

    node_details = "\n".join([
        dumps(node_json) for node_json in [
            {
                "resource_type": "source",
                "unique_id": "id_1",
                "name": "name_1",
                "package_name": "package_1",
                "config": {"schema": "schema_1"}
            }, {
                "resource_type": "model",
                "unique_id": "id_2",
                "name": "name_2",
                "package_name": "package_1",
                "config": {"schema": "schema_1"},
                "depends_on": {
                    "nodes": ["id_1", "id_3"]
                }
            }, {
                "resource_type": "model",
                "unique_id": "id_3",
                "name": "name_3",
                "package_name": "package_1",
                "config": {"schema": "schema_1"},
                "depends_on": {
                    "nodes": ["id_1", "id_4"]
                }
            }, {
                "resource_type": "model",
                "unique_id": "id_4",
                "name": "name_4",
                "package_name": "package_1",
                "config": {"schema": "schema_1"},
            }
        ]
    ])

    forwards = {
        "id_1": {"id_2", "id_3"},
        "id_3": {"id_2"},
        "id_4": {"id_3"}
    }

    def test_get_correct_tree(self):
        run_dbt_command_mock = Mock(
            return_value=Mock(stdout=self.node_details)
        )

        with patch(file_str + ".run_dbt_command", run_dbt_command_mock):
            dependencies = get_dependency_tree("")

        for u_id, dependents in dependencies.items():
            self.assertSetEqual(set([dep["unique_id"] for dep in dependents]),
                                self.forwards[u_id])

        self.assertTrue("id_2" not in dependencies)

    def test_find_forward_nodes(self):
        run_dbt_command_mock = Mock(
            return_value=Mock(stdout=self.node_details)
        )

        with patch(file_str + ".run_dbt_command", run_dbt_command_mock):
            dependencies = get_dependency_tree("")

        self.assertSetEqual({"id_1", "id_2", "id_3"},
                            find_forward_nodes(dependencies, "id_1"))
        self.assertSetEqual({"id_4", "id_3", "id_2"},
                            find_forward_nodes(dependencies, "id_4"))
