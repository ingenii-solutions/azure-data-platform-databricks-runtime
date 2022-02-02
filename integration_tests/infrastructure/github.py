from pulumi_github import ActionsSecret

from base import azure_client
from databricks import workspace, testing_cluster
from datalake import datalake
from testing_service_principal import testing_app, testing_sp_password

organization = "ingenii-solutions"
repository_name = "azure-data-platform-databricks-runtime"


def create_secret(secret_name, secret_value):
    ActionsSecret(
        resource_name=f"github-repository-secret-{secret_name.lower()}",
        repository=repository_name,
        secret_name=secret_name,
        plaintext_value=secret_value,
    )


create_secret("ARM_TENANT_ID", azure_client.tenant_id)
create_secret("ARM_SUBSCRIPTION_ID", azure_client.subscription_id)
create_secret("ARM_CLIENT_ID", testing_app.application_id)
create_secret("ARM_CLIENT_SECRET", testing_sp_password.value)

create_secret("DATA_LAKE_NAME", datalake.name)
create_secret("DATABRICKS_HOST", workspace.workspace_url)
create_secret("DATABRICKS_CLUSTER_ID", testing_cluster.cluster_id)
