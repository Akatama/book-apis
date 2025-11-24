resource "azurerm_container_registry" "acr" {
  name                = var.ContainerRegistryName
  resource_group_name = var.ResourceGroupName
  location            = var.Location

  sku           = var.ContainerRegistrySku
  admin_enabled = var.ContainerRegistryAdminEnabled

  tags = {
    application = var.ResourceBaseName
    component   = "container-registry"
  }
}
