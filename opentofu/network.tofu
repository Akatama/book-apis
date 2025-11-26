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
  # delegation {
  #   name = "PostgresFlexibleServers"
  #   service_delegation {
  #     name = "Microsoft.DBforPostgres/flexibleServers"
  #   }
  # }
}

resource "azurerm_app_service_virtual_network_swift_connection" "web_app_vnet_integration" {
  app_service_id = azurerm_linux_web_app.web_app.id
  subnet_id      = azurerm_subnet.app_subnet.id
}
