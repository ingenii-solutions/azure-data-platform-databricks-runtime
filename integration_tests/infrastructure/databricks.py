from os import environ, getenv
from pulumi import ResourceOptions
import pulumi_azuread as azuread
from pulumi_azure_native import authorization, databricks as az_databricks
from pulumi_databricks import databricks, Provider as DatabricksProvider, \
    ProviderArgs as DatabricksProviderArgs

from base import azure_client, location, overall_name, resource_group
from current_version import docker_image_url
from datalake import datalake, container_names
from networking import databricks_private_subnet, databricks_public_subnet, \
    vnet

workspace_name = overall_name
workspace = az_databricks.Workspace(
    resource_name=workspace_name,
    workspace_name=workspace_name,
    location=location,
    managed_resource_group_id=resource_group.id.apply(lambda id: f"{id}-managed"),
    parameters=az_databricks.WorkspaceCustomParametersArgs(
        custom_private_subnet_name=az_databricks.WorkspaceCustomStringParameterArgs(
            value=databricks_private_subnet.name,  # type: ignore
        ),
        custom_public_subnet_name=az_databricks.WorkspaceCustomStringParameterArgs(
            value=databricks_public_subnet.name,  # type: ignore
        ),
        custom_virtual_network_id=az_databricks.WorkspaceCustomStringParameterArgs(
            value=vnet.id,
        ),
        enable_no_public_ip=az_databricks.WorkspaceCustomBooleanParameterArgs(
            value=True
        ),
    ),
    sku=az_databricks.SkuArgs(name="Premium"),
    resource_group_name=resource_group.name,
)

# AZURE AD SERVICE PRINCIPAL USED FOR STORAGE MOUNTING
storage_mounts_sp_name = f"{workspace_name}-databricks-storage-mounting-sp"
storage_mounts_sp_app = azuread.Application(
    resource_name=storage_mounts_sp_name,
    display_name=storage_mounts_sp_name,
    identifier_uris=[f"api://{storage_mounts_sp_name}"],
    owners=[azure_client.object_id],
)

storage_mounts_sp = azuread.ServicePrincipal(
    resource_name=storage_mounts_sp_name,
    application_id=storage_mounts_sp_app.application_id,
    app_role_assignment_required=False,
)

storage_mounts_sp_password = azuread.ServicePrincipalPassword(
    resource_name=storage_mounts_sp_name,
    service_principal_id=storage_mounts_sp.object_id,
)

authorization.RoleAssignment(
    resource_name=f"{workspace_name}-mounting-service-principal-to-data-lake",
    principal_type="ServicePrincipal",
    principal_id=storage_mounts_sp.object_id,
    role_definition_id="/providers/Microsoft.Authorization/roleDefinitions/ba92f5b4-2d11-453d-a403-e96b0029c9fe",
    scope=datalake.id,
    opts=ResourceOptions(delete_before_replace=True),
)

#######
# DATABRICKS
#######

databricks_provider = DatabricksProvider(
    resource_name=workspace_name,
    args=DatabricksProviderArgs(
        azure_client_id=getenv("ARM_CLIENT_ID", azure_client.client_id),
        azure_client_secret=getenv("ARM_CLIENT_SECRET"),
        azure_tenant_id=getenv("ARM_TENANT_ID", azure_client.tenant_id),
        azure_workspace_resource_id=workspace.id,
    ),
)

databricks.WorkspaceConf(
    resource_name=workspace_name,
    custom_config={
        "enableDcs": "true",
        "enableIpAccessLists": "true"
    },
    opts=ResourceOptions(provider=databricks_provider),
)

system_cluster = databricks.Cluster(
    resource_name=f"{workspace_name}-system-cluster",
    cluster_name="system",
    spark_version="9.1.x-scala2.12",
    node_type_id="Standard_F4s",
    is_pinned=True,
    autotermination_minutes=10,
    spark_conf={
        "spark.databricks.cluster.profile": "singleNode",
        "spark.master": "local[*]",
        "spark.databricks.delta.preview.enabled": "true",
    },
    custom_tags={"ResourceClass": "SingleNode"},
    opts=ResourceOptions(provider=databricks_provider),
)

secret_scope_name = "main"
secret_scope = databricks.SecretScope(
    resource_name=f"{workspace_name}-secret-scope-{secret_scope_name}",
    name=secret_scope_name,
    opts=ResourceOptions(provider=databricks_provider),
)


storage_mounts_dbw_password = databricks.Secret(
    resource_name=storage_mounts_sp_name,
    scope=secret_scope.id,
    string_value=storage_mounts_sp_password.value,
    key=storage_mounts_sp_name,
    opts=ResourceOptions(provider=databricks_provider),
)

# STORAGE MOUNTS
for container_name in container_names:
    databricks.AzureAdlsGen2Mount(
        resource_name=f"{workspace_name}-{container_name}",
        client_id=storage_mounts_sp.application_id,
        client_secret_key=storage_mounts_dbw_password.key,
        tenant_id=azure_client.tenant_id,
        client_secret_scope=secret_scope.name,
        storage_account_name=datalake.name,
        initialize_file_system=False,
        container_name=container_name,
        mount_name=container_name,
        cluster_id=system_cluster.id,
        opts=ResourceOptions(
            provider=databricks_provider,
            delete_before_replace=True,
        ),
    )

# DBT TOKEN
dbt_token_name = f"testing-token-for-dbt"
dbt_token_resource_name = f"{workspace_name}-token-for-dbt"

# Also used to generate the DBT documentation by the DevOps pipeline
dbt_token = databricks.Token(
    resource_name=dbt_token_resource_name,
    comment="Data Build Tool Token - Used for DBT automation",
    opts=ResourceOptions(provider=databricks_provider),
)

dbt_token_as_scope_secret = databricks.Secret(
    resource_name=dbt_token_resource_name,
    scope=secret_scope.id,
    string_value=dbt_token.token_value,
    key=dbt_token_name,
    opts=ResourceOptions(provider=databricks_provider),
)

# Testing cluster
testing_cluster = databricks.Cluster(
    resource_name=f"{workspace_name}-testing-cluster",
    cluster_name="testing",
    spark_version="9.1.x-scala2.12",
    node_type_id="Standard_F4s",
    is_pinned=True,
    autotermination_minutes=10,
    libraries=[
        databricks.ClusterLibraryArgs(
            whl="dbfs:/mnt/preprocess/pre_process-1.0.0-py3-none-any.whl"
        )
    ],
    docker_image=databricks.ClusterDockerImageArgs(
        url=docker_image_url
    ),
    spark_conf={
        "spark.databricks.cluster.profile": "singleNode",
        "spark.master": "local[*]",
        "spark.databricks.delta.preview.enabled": "true",
    },
    spark_env_vars={
            "DATABRICKS_WORKSPACE_HOSTNAME": workspace.workspace_url,
            "DATABRICKS_CLUSTER_NAME": "testing",
            "DBT_TOKEN_SCOPE": secret_scope_name,
            "DBT_TOKEN_NAME": dbt_token_name,
            "DBT_ROOT_FOLDER": "/dbfs/mnt/dbt",
            "DBT_LOGS_FOLDER": "/dbfs/mnt/dbt-logs",
    },
    custom_tags={"ResourceClass": "SingleNode"},
    opts=ResourceOptions(provider=databricks_provider),
)

# Service principal
service_principal_name = "Databricks Runtime Testing"
service_principal = databricks.ServicePrincipal(
    resource_name=f"{workspace_name}-service-principal-testing",
    application_id=environ["TESTING_PRINCIPAL_CLIENT_ID"],
    display_name=service_principal_name
)
databricks.Permissions(
    resource_name=f"{workspace_name}-service-principal-testing-cluster-permission",
    access_controls=[databricks.PermissionsAccessControlArgs(
        permission_level="CAN_RESTART",
        service_principal_name=service_principal.application_id
    )],
    cluster_id=testing_cluster.id
)
databricks.SecretAcl(
    resource_name=f"{workspace_name}-secret-scope-{secret_scope_name}-acl",
    permission="READ",
    principal=service_principal.application_id,
    scope=secret_scope_name
)
