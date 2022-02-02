from pulumi_github import ActionsSecret

from databricks import workspace, testing_cluster
from datalake import datalake

organization = "ingenii-solutions"
repository_name = "azure-data-platform-databricks-runtime"


def create_secret(secret_name, secret_value):
    ActionsSecret(
        resource_name=f"github-repository-secret-{secret_name.lower()}",
        repository=repository_name,
        secret_name=secret_name,
        plaintext_value=secret_value,
    )


create_secret("DATA_LAKE_NAME", datalake.name)
create_secret("DATABRICKS_HOST", workspace.workspace_url)
create_secret("DATABRICKS_CLUSTER_ID", testing_cluster.cluster_id)
