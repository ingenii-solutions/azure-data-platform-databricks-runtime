from pulumi_azuread import Application, ServicePrincipal, ServicePrincipalPassword
from pulumi_databricks import databricks

from base import azure_client
from databricks import secret_scope_name, testing_cluster, workspace_name

service_principal_name = "Databricks Runtime Testing"
service_principal_resource_name = "databricks-runtime-testing-service-principal"

testing_app = Application(
    resource_name=service_principal_resource_name,
    display_name=service_principal_name,
    identifier_uris=[f"api://{service_principal_resource_name}"],
    owners=[azure_client.object_id],
)

testing_sp = ServicePrincipal(
    resource_name=service_principal_resource_name,
    application_id=testing_app.application_id,
    app_role_assignment_required=False,
)

testing_sp_password = ServicePrincipalPassword(
    resource_name=service_principal_resource_name,
    service_principal_id=testing_sp.object_id,
)

service_principal = databricks.ServicePrincipal(
    resource_name=f"{workspace_name}-service-principal-testing",
    application_id=testing_app.application_id,
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
