from pulumi_azure_native import databricks

from base import location, overall_name, resource_group
from networking import databricks_private_subnet, databricks_public_subnet, \
    vnet

workspace = databricks.Workspace(
    resource_name=overall_name,
    workspace_name=overall_name,
    location=location,
    managed_resource_group_id=f"{overall_name}-managed",
    parameters=databricks.WorkspaceCustomParametersArgs(
        custom_private_subnet_name=databricks.WorkspaceCustomStringParameterArgs(
            value=databricks_private_subnet.name,  # type: ignore
        ),
        custom_public_subnet_name=databricks.WorkspaceCustomStringParameterArgs(
            value=databricks_public_subnet.name,  # type: ignore
        ),
        custom_virtual_network_id=databricks.WorkspaceCustomStringParameterArgs(
            value=vnet.id,
        ),
        enable_no_public_ip=databricks.WorkspaceCustomBooleanParameterArgs(
            value=True
        ),
    ),
    sku=databricks.SkuArgs(name="Premium"),
    resource_group_name=resource_group.name,
)
