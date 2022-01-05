from ipaddress import ip_network
from pulumi_azure_native import network

from base import location, overall_name, resource_group

vnet_name = overall_name
vnet_address_space = "10.105.0.0/16"
vnet = network.VirtualNetwork(
    resource_name=vnet_name,
    virtual_network_name=vnet_name,
    resource_group_name=resource_group.name,
    location=location,
    address_space=network.AddressSpaceArgs(
        address_prefixes=[vnet_address_space]
    ),
)

nsg = network.NetworkSecurityGroup(
    resource_name=overall_name,
    network_security_group_name=overall_name,
    resource_group_name=resource_group.name,
)
main_route_table = network.RouteTable(
    resource_name=overall_name,
    route_table_name=overall_name,
    resource_group_name=resource_group.name,
    disable_bgp_route_propagation=True,
)

gateway_public_ip = network.PublicIPAddress(
    overall_name,
    idle_timeout_in_minutes=10,
    public_ip_address_name=overall_name,
    public_ip_address_version="IPv4",
    public_ip_allocation_method="Static",
    resource_group_name=resource_group.name,
    sku=network.PublicIPAddressSkuArgs(name="Standard"),
)
gateway = network.NatGateway(
    overall_name,
    nat_gateway_name=overall_name,
    public_ip_addresses=[
        network.SubResourceArgs(
            id=gateway_public_ip.id,
        )
    ],
    resource_group_name=resource_group.name,
    sku=network.NatGatewaySkuArgs(
        name=network.NatGatewaySkuName.STANDARD,
    ),
)


def generate_cidr(cidr_subnet: str, new_prefix: int, network_number: int):
    return list(ip_network(cidr_subnet).subnets(new_prefix=new_prefix))[
        network_number
    ].exploded


databricks_private_subnet_name = f"{overall_name}-private-subnet"
databricks_private_subnet = network.Subnet(
    resource_name=databricks_private_subnet_name,
    subnet_name=databricks_private_subnet_name,
    resource_group_name=resource_group.name,
    virtual_network_name=vnet.name,
    address_prefix=generate_cidr(vnet_address_space, 22, 1),
    route_table=network.RouteTableArgs(id=main_route_table.id),
    network_security_group=network.NetworkSecurityGroupArgs(id=nsg.id),
    nat_gateway=network.SubResourceArgs(id=gateway.id),
    service_endpoints=[
        network.ServiceEndpointPropertiesFormatArgs(
            service="Microsoft.Storage",
        ),
        network.ServiceEndpointPropertiesFormatArgs(
            service="Microsoft.KeyVault",
        ),
    ],
    delegations=[
        network.DelegationArgs(
            name="databricks", service_name="Microsoft.Databricks/workspaces"
        )
    ],
)
databricks_private_subnet_name = f"{overall_name}-public-subnet"
databricks_public_subnet = network.Subnet(
    resource_name=databricks_private_subnet_name,
    subnet_name=databricks_private_subnet_name,
    resource_group_name=resource_group.name,
    virtual_network_name=vnet.name,
    address_prefix=generate_cidr(vnet_address_space, 22, 2),
    route_table=network.RouteTableArgs(id=main_route_table.id),
    network_security_group=network.NetworkSecurityGroupArgs(id=nsg.id),
    nat_gateway=network.SubResourceArgs(id=gateway.id),
    service_endpoints=[
        network.ServiceEndpointPropertiesFormatArgs(
            service="Microsoft.Storage",
        ),
        network.ServiceEndpointPropertiesFormatArgs(
            service="Microsoft.KeyVault",
        ),
    ],
    delegations=[
        network.DelegationArgs(
            name="databricks", service_name="Microsoft.Databricks/workspaces"
        )
    ],
)
