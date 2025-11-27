resource "azurerm_resource_group" "resource_group" {
  name     = var.ResourceGroupName
  location = var.Location
}

resource "azurerm_virtual_network" "vnet" {
  name                = "${var.ResourceBaseName}-vnet"
  location            = var.Location
  resource_group_name = azurerm_resource_group.resource_group.name
  address_space       = [var.VirtualNetworkAddressSpace]
}

resource "azurerm_subnet" "app_subnet" {
  name                 = var.AppSubnetName
  resource_group_name  = azurerm_resource_group.resource_group.name
  virtual_network_name = azurerm_virtual_network.vnet.name
  address_prefixes     = [var.AppSubnetPrefix]
  service_endpoints    = ["Microsoft.Storage"]

  delegation {
    name = "appservice-delegation"
    service_delegation {
      name = "Microsoft.Web/serverFarms"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/action",
      ]
    }
  }
}

resource "azurerm_subnet" "db_subnet" {
  name                 = var.DBSubnetName
  resource_group_name  = azurerm_resource_group.resource_group.name
  virtual_network_name = azurerm_virtual_network.vnet.name
  address_prefixes     = [var.DBSubnetPrefix]

  delegation {
    name = "PostgresFlexibleServers"
    service_delegation {
      name = "Microsoft.DBforPostgres/flexibleServers"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
      ]
    }
  }
}

resource "azurerm_app_service_virtual_network_swift_connection" "web_app_vnet_integration" {
  app_service_id = azurerm_linux_web_app.web_app.id
  subnet_id      = azurerm_subnet.app_subnet.id
}

resource "azurerm_private_dns_zone" "postgres_private_dns_zone" {
  name                = "privatelink.postgres.database.azure.com"
  resource_group_name = azurerm_resource_group.resource_group.name
}

resource "azurerm_private_dns_zone_virtual_network_link" "postgres_private_dns_vnet_link" {
  name                  = "${var.ResourceBaseName}-postgres-dns-link"
  resource_group_name   = azurerm_resource_group.resource_group.name
  private_dns_zone_name = azurerm_private_dns_zone.postgres_private_dns_zone.name
  virtual_network_id    = azurerm_virtual_network.vnet.id
}

resource "azurerm_private_endpoint" "postgres_private_endpoint" {
  name                = "${var.ResourceBaseName}-postgres-pe"
  location            = var.Location
  resource_group_name = azurerm_resource_group.resource_group.name
  subnet_id           = azurerm_subnet.db_subnet.id

  private_service_connection {
    name                           = "${var.ResourceBaseName}-postgres-psc"
    private_connection_resource_id = azurerm_postgresql_flexible_server.postgres_server.id
    subresource_names              = ["postgresqlServer"]
    is_manual_connection           = false
  }

  private_dns_zone_group {
    name                 = "${var.ResourceBaseName}-postgres-dns-zone-group"
    private_dns_zone_ids = [azurerm_private_dns_zone.postgres_private_dns_zone.id]
  }
}
