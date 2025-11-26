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
    WEBSITES_PORT                       = var.WebAppContainerPort
    WEBSITES_ENABLE_APP_SERVICE_STORAGE = "false"

    # Database-related settings
    DB_NAME     = var.DBName
    DB_USER     = var.AdminUserName
    DB_PASSWORD = data.azurerm_key_vault_secret.my_sql_admin_password.value
    DB_HOST     = azurerm_postgresql_flexible_server.postgres_server.fqdn
    DB_PORT     = "5432"
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
