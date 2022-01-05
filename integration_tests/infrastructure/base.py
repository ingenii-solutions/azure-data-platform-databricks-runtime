from pulumi_azure_native import authorization, resources

azure_client = authorization.get_client_config()

location = "eastus"
overall_name = "databricks-runtime-testing"

# Resource group
resource_group_name = overall_name
resource_group = resources.ResourceGroup(
    resource_name=resource_group_name,
    resource_group_name=resource_group_name,
    location=location
)
