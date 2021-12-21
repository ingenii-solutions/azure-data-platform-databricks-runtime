from json import dumps
import sys
from unittest import TestCase
from unittest.mock import Mock, patch

sys.modules["delta.tables"] = Mock()
sys.modules["pyspark.sql.dataframe"] = Mock()
sys.modules["pyspark.sql.functions"] = Mock()
sys.modules["pyspark.sql.session"] = Mock()
sys.modules["pyspark.sql.types"] = Mock()

from ingenii_databricks.dbt_utils import get_dependency_tree, \
    find_forward_nodes, find_node_order, propagate_source_data  # noqa: E402

file_str = "ingenii_databricks.dbt_utils"

# Example map
#  1   4
#  | \ |
#  |  \|
#  |   3
#  |  /
#  | /
#  2
# From 1: 1, 3, 2
# From 4: 4, 3, 2


class TestSourcePropagation(TestCase):

    dbt_token = "123456789"

    node_details = "\n".join([
        dumps(node_json) for node_json in [
            {
                "resource_type": "source",
                "unique_id": "package_1.schema_1.name_1",
                "name": "name_1",
                "package_name": "package_1",
                "config": {"schema": "schema_1"}
            }, {
                "resource_type": "model",
                "unique_id": "package_1.schema_1.name_2",
                "name": "name_2",
                "package_name": "package_1",
                "config": {"schema": "schema_1"},
                "depends_on": {
                    "nodes": [
                        "package_1.schema_1.name_1",
                        "package_1.schema_1.name_3",
                    ]
                }
            }, {
                "resource_type": "model",
                "unique_id": "package_1.schema_1.name_3",
                "name": "name_3",
                "package_name": "package_1",
                "config": {"schema": "schema_1"},
                "depends_on": {
                    "nodes": [
                        "package_1.schema_1.name_1",
                        "package_1.schema_1.name_4",
                    ]
                }
            }, {
                "resource_type": "model",
                "unique_id": "package_1.schema_1.name_4",
                "name": "name_4",
                "package_name": "package_1",
                "config": {"schema": "schema_1"},
            }
        ]
    ])

    forwards = {
        "package_1.schema_1.name_1": {
            "package_1.schema_1.name_2", "package_1.schema_1.name_3"
        },
        "package_1.schema_1.name_3": {"package_1.schema_1.name_2"},
        "package_1.schema_1.name_4": {"package_1.schema_1.name_3"},
    }
    backwards = {
        "package_1.schema_1.name_2": {
            "package_1.schema_1.name_1", "package_1.schema_1.name_3"
        },
        "package_1.schema_1.name_3": {
            "package_1.schema_1.name_1", "package_1.schema_1.name_4"
        },
    }
    expected_orders = {
        "package_1.schema_1.name_1": [
            "package_1.schema_1.name_1",
            "package_1.schema_1.name_3",
            "package_1.schema_1.name_2",
        ],
        "package_1.schema_1.name_4": [
            "package_1.schema_1.name_4",
            "package_1.schema_1.name_3",
            "package_1.schema_1.name_2",
        ],
    }

    run_dbt_command_mock = Mock(return_value=Mock(stdout=node_details))

    def test_get_correct_tree(self):
        """ Given the above definition, generate the correct tree """

        with patch(file_str + ".run_dbt_command", self.run_dbt_command_mock):
            dependencies, dependents = get_dependency_tree(self.dbt_token)

        for u_id, deps in dependencies.items():
            self.assertSetEqual(set(deps), self.backwards.get(u_id, set()))

        for u_id, deps in dependents.items():
            self.assertSetEqual(set([dep["unique_id"] for dep in deps]),
                                self.forwards[u_id])

        self.assertTrue("id_2" not in dependents)

    def test_find_forward_nodes(self):
        """ From a starting point, find only the forward nodes """

        with patch(file_str + ".run_dbt_command", self.run_dbt_command_mock):
            _, dependents = get_dependency_tree(self.dbt_token)

        self.assertSetEqual(
            set(self.expected_orders["package_1.schema_1.name_1"]),
            find_forward_nodes(dependents, "package_1.schema_1.name_1")
        )
        self.assertSetEqual(
            set(self.expected_orders["package_1.schema_1.name_4"]),
            find_forward_nodes(dependents, "package_1.schema_1.name_4")
        )

    def test_find_node_order(self):
        """ From a starting point, find the order to traverse the nodes """

        with patch(file_str + ".run_dbt_command", self.run_dbt_command_mock):
            dependencies, dependents = get_dependency_tree(self.dbt_token)

        self.assertListEqual(
            self.expected_orders["package_1.schema_1.name_1"],
            find_node_order(dependencies, dependents,
                            "package_1.schema_1.name_1")
        )
        self.assertListEqual(
            self.expected_orders["package_1.schema_1.name_4"],
            find_node_order(dependencies, dependents,
                            "package_1.schema_1.name_4")
        )

    def test_propagate_source_data(self):
        run_model_mock = Mock(return_value=Mock(returncode=0))

        with \
                patch(file_str + ".run_dbt_command",
                      self.run_dbt_command_mock), \
                patch(file_str + ".run_model",
                      run_model_mock):
            propagate_source_data(self.dbt_token,
                                  "package_1", "schema_1", "name_1")

        run_model_mock.assert_called_once_with(
            "source.package_1.schema_1.name_1")
