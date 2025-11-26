resource "azurerm_service_plan" "app_service_plan" {
  name                = "${var.ResourceBaseName}-asp"
  resource_group_name = var.ResourceGroupName
  location            = var.Location

  os_type  = "Linux"
  sku_name = var.AppServicePlanSkuName

  tags = {
    application = var.ResourceBaseName
    component   = "app-service-plan"
  }
}

resource "azurerm_linux_web_app" "web_app" {
  name                = "${var.ResourceBaseName}-webapp"
  resource_group_name = var.ResourceGroupName
  location            = var.Location
  service_plan_id     = azurerm_service_plan.app_service_plan.id

  https_only = true

  site_config {
    application_stack {
      docker_image_name   = "${azurerm_container_registry.acr.login_server}/${var.WebAppContainerImageName}:${var.WebAppContainerImageTag}"
      docker_registry_url = "https://${azurerm_container_registry.acr.login_server}"
    }

    always_on = true
  }

  app_settings = {
    WEBSITES_PORT                        = var.WebAppContainerPort
    DOCKER_REGISTRY_SERVER_URL           = "https://${azurerm_container_registry.acr.login_server}"
    DOCKER_REGISTRY_SERVER_USERNAME      = azurerm_container_registry.acr.admin_username
    DOCKER_REGISTRY_SERVER_PASSWORD      = azurerm_container_registry.acr.admin_password
    WEBSITES_ENABLE_APP_SERVICE_STORAGE  = "false"
  }

  identity {
    type = "SystemAssigned"
  }

  tags = {
    application = var.ResourceBaseName
    component   = "linux-web-app"
  }

  depends_on = [
    azurerm_container_registry.acr
  ]
}
