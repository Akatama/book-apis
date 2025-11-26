resource "azurerm_container_registry" "acr" {
  name                = "${var.ResourceBaseName}acr"
  resource_group_name = var.ResourceGroupName
  location            = var.Location

  sku           = var.ContainerRegistrySku
  admin_enabled = var.ContainerRegistryAdminEnabled

  tags = {
    application = var.ResourceBaseName
    component   = "container-registry"
  }
}
