from pulumi import ResourceOptions
from pulumi_azuread import Application, ServicePrincipal, ServicePrincipalPassword
from pulumi_azure_native import authorization, resources

azure_client = authorization.get_client_config()

location = "eastus"
overall_name = "databricks-runtime-testing"
runner_ip = "151.251.5.72"

# Resource group
resource_group_name = overall_name
resource_group = resources.ResourceGroup(
    resource_name=resource_group_name,
    resource_group_name=resource_group_name,
    location=location
)

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

authorization.RoleAssignment(
    resource_name=service_principal_resource_name,
    principal_type="ServicePrincipal",
    principal_id=testing_sp.object_id,
    role_definition_id="/providers/Microsoft.Authorization/roleDefinitions/b24988ac-6180-42a0-ab88-20f7382dd24c",
    scope=resource_group.id,
    opts=ResourceOptions(delete_before_replace=True),
)
