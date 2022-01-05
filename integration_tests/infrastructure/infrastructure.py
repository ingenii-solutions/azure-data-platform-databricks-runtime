from pulumi_azure_native import authorization, resources

location = "eastus"

azure_client = authorization.get_client_config()

# Resource group
resource_name = "databricks-runtime-testing"
resource_group = resources.ResourceGroup(
    resource_name=resource_name,
    resource_group_name=resource_name,
    location=location
)
