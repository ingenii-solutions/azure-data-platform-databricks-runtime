from json import dumps
import sys
from unittest import TestCase
from unittest.mock import Mock, patch

sys.modules["delta.tables"] = Mock()
sys.modules["pyspark.dbutils"] = Mock()
sys.modules["pyspark.sql.dataframe"] = Mock()
sys.modules["pyspark.sql.functions"] = Mock()
sys.modules["pyspark.sql.session"] = Mock()
sys.modules["pyspark.sql.types"] = Mock()
sys.modules["pre_process.root"] = Mock()

from ingenii_databricks.dbt_utils import get_nodes_and_dependents, \
    find_forward_nodes, find_node_order  # noqa: E402
from ingenii_databricks.pipeline import propagate_source_data  # noqa: E402

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
                "unique_id": "source.package_1.schema_1.name_1",
                "name": "name_1",
                "package_name": "package_1",
                "source_name": "schema_1"
            }, {
                "resource_type": "snapshot",
                "unique_id": "snapshot.package_1.name_2",
                "name": "name_2",
                "package_name": "package_1",
                "target_schema": "schema_1",
                "depends_on": {
                    "nodes": [
                        "source.package_1.schema_1.name_1",
                        "model.package_1.name_3",
                    ]
                }
            }, {
                "resource_type": "model",
                "unique_id": "model.package_1.name_3",
                "name": "name_3",
                "package_name": "package_1",
                "config": {"schema": "schema_1"},
                "depends_on": {
                    "nodes": [
                        "source.package_1.schema_1.name_1",
                        "source.package_1.schema_1.name_4",
                    ]
                }
            }, {
                "resource_type": "source",
                "unique_id": "source.package_1.schema_1.name_4",
                "name": "name_4",
                "package_name": "package_1",
                "source_name": "schema_1",
            }
        ]
    ])

    forwards = {
        "source.package_1.schema_1.name_1": {
            "snapshot.package_1.name_2", "model.package_1.name_3"
        },
        "model.package_1.name_3": {"snapshot.package_1.name_2"},
        "source.package_1.schema_1.name_4": {"model.package_1.name_3"},
    }
    backwards = {
        "snapshot.package_1.name_2": {
            "source.package_1.schema_1.name_1",
            "model.package_1.name_3"
        },
        "model.package_1.name_3": {
            "source.package_1.schema_1.name_1",
            "source.package_1.schema_1.name_4"
        },
    }
    expected_orders = {
        "source.package_1.schema_1.name_1": [
            "source.package_1.schema_1.name_1",
            "model.package_1.name_3",
            "snapshot.package_1.name_2",
        ],
        "source.package_1.schema_1.name_4": [
            "source.package_1.schema_1.name_4",
            "model.package_1.name_3",
            "snapshot.package_1.name_2",
        ],
    }

    run_dbt_command_mock = Mock(return_value=Mock(stdout=node_details))

    def test_get_correct_tree(self):
        """ Given the above definition, generate the correct tree """

        with patch(file_str + ".run_dbt_command", self.run_dbt_command_mock):
            nodes, dependents = get_nodes_and_dependents(self.dbt_token)

        for node_id, details in nodes.items():
            self.assertSetEqual(
                set(details["depends_on"]),
                self.backwards.get(node_id, set())
            )

        for node_id, deps in dependents.items():
            self.assertSetEqual(set(deps), self.forwards[node_id])

        self.assertTrue("model.package_1.name_2" not in dependents)

    def test_find_forward_nodes(self):
        """ From a starting point, find only the forward nodes """

        with patch(file_str + ".run_dbt_command", self.run_dbt_command_mock):
            _, dependents = get_nodes_and_dependents(self.dbt_token)

        self.assertSetEqual(
            set(self.expected_orders["source.package_1.schema_1.name_1"]),
            find_forward_nodes(dependents, "source.package_1.schema_1.name_1")
        )
        self.assertSetEqual(
            set(self.expected_orders["source.package_1.schema_1.name_4"]),
            find_forward_nodes(dependents, "source.package_1.schema_1.name_4")
        )

    def test_find_node_order(self):
        """ From a starting point, find the order to traverse the nodes """

        with patch(file_str + ".run_dbt_command", self.run_dbt_command_mock):
            nodes, dependents = get_nodes_and_dependents(self.dbt_token)

        self.assertListEqual(
            self.expected_orders["source.package_1.schema_1.name_1"][1:],
            find_node_order(nodes, dependents,
                            "source.package_1.schema_1.name_1")
        )
        self.assertListEqual(
            self.expected_orders["source.package_1.schema_1.name_4"][1:],
            find_node_order(nodes, dependents,
                            "source.package_1.schema_1.name_4")
        )

    def test_propagate_source_data(self):
        run_dbt_command_mock = \
            Mock(return_value=Mock(stdout=self.node_details, returncode=0))

        with \
            patch("ingenii_databricks.pipeline.run_dbt_command",
                  run_dbt_command_mock), \
            patch(file_str + ".run_dbt_command",
                  run_dbt_command_mock):
            propagate_source_data(self.dbt_token,
                                  "package_1", "schema_1", "name_1")

        all_calls = run_dbt_command_mock.call_args_list
        self.assertEqual(len(all_calls), 3)

        args, kwargs = all_calls[0]
        self.assertTupleEqual(args, (self.dbt_token, "ls", "--output", "json"))
        self.assertDictEqual(kwargs, {})

        args, kwargs = all_calls[1]
        self.assertTupleEqual(
            args, (self.dbt_token, "run", "--select", "name_3"))
        self.assertDictEqual(kwargs, {})

        args, kwargs = all_calls[2]
        self.assertTupleEqual(
            args, (self.dbt_token, "snapshot", "--select", "name_2"))
        self.assertDictEqual(kwargs, {})
