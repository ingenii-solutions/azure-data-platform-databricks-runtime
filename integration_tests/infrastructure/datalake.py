from pulumi import ResourceOptions
from pulumi_azure_native import storage

from base import location, overall_name, resource_group, runner_ip
from networking import databricks_public_subnet, databricks_private_subnet

datalake_name = overall_name.replace("-", "")

# While we don't have a runner
#    network_rule_set=storage.NetworkRuleSetArgs(
#        bypass=storage.Bypass.AZURE_SERVICES,
#        default_action=storage.DefaultAction.DENY,
#        ip_rules=[storage.IPRuleArgs(i_p_address_or_range=runner_ip)],
#        virtual_network_rules=[
#            storage.VirtualNetworkRuleArgs(
#                virtual_network_resource_id=subnet.id,
#                state="Succeeded",
#            )
#            for subnet in (databricks_public_subnet, databricks_private_subnet)
#        ],
#    ),

datalake = storage.StorageAccount(
    resource_name=datalake_name,
    account_name=datalake_name,
    allow_blob_public_access=False,
    network_rule_set=storage.NetworkRuleSetArgs(
        bypass=storage.Bypass.AZURE_SERVICES,
        default_action=storage.DefaultAction.ALLOW,
    ),
    is_hns_enabled=True,
    kind=storage.Kind.STORAGE_V2,
    location=location,
    minimum_tls_version=storage.MinimumTlsVersion.TLS1_2,
    resource_group_name=resource_group.name,
    sku=storage.SkuArgs(name=storage.SkuName.STANDARD_GRS),
)

container_names = ("archive", "dbt", "dbt-logs", "models", "orchestration",
                   "preprocess", "raw", "snapshots", "source")
for container_name in container_names:
    storage.BlobContainer(
        resource_name=f"datalake-container-{container_name}",
        account_name=datalake.name,
        container_name=container_name,
        resource_group_name=resource_group.name,
        opts=ResourceOptions(
            ignore_changes=[
                "public_access",
                "default_encryption_scope",
                "deny_encryption_scope_override",
            ],
        ),
    )
