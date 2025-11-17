resource "azurerm_postgresql_flexible_server" "postgres_server" {
  name                = lower(var.ResourceBaseName)
  resource_group_name = azurerm_resource_group.resource_group.name
  location            = var.Location

  version                       = "12"
  administrator_login           = var.AdminUserName
  administrator_password        = data.azurerm_key_vault_secret.my_sql_admin_password.value
  public_network_access_enabled = true

  storage_mb   = 32768
  storage_tier = "P4"

  sku_name = "${var.ServerEdition}_${var.DBSkuName}"
}

resource "azurerm_postgresql_flexible_server_firewall_rule" "my_access" {
  name             = "my_access"
  server_id        = azurerm_postgresql_flexible_server.postgres_server.id
  start_ip_address = var.IPAddress
  end_ip_address   = var.IPAddress
}
