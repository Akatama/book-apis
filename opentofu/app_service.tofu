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
      # WebAppContainerImageName is expected to already include the registry hostname,
      # e.g. "booksapiacr.azurecr.io/booksapi"
      docker_image_name   = "${var.WebAppContainerImageName}:${var.WebAppContainerImageTag}"
      docker_registry_url = "https://${azurerm_container_registry.acr.login_server}"
    }

    always_on = true

    # Use the web app's system-assigned managed identity to pull from ACR
    container_registry_use_managed_identity = true
  }

  app_settings = {
    WEBSITES_PORT                       = var.WebAppContainerPort
    WEBSITES_ENABLE_APP_SERVICE_STORAGE = "false"

    # Database-related settings
    DB_NAME     = var.DBName
    DB_USER     = var.AdminUserName
    DB_PASSWORD = data.azurerm_key_vault_secret.my_sql_admin_password.value

    # Use the private endpoint hostname instead of the public FQDN
    # Server name is lower(var.ResourceBaseName), so the private endpoint FQDN is:
    #   <server-name>.privatelink.postgres.database.azure.com
    DB_HOST = "${lower(var.ResourceBaseName)}.privatelink.postgres.database.azure.com"
    DB_PORT = "5432"
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

# Grant the web app's managed identity AcrPull on the ACR
resource "azurerm_role_assignment" "web_app_acr_pull" {
  scope                = azurerm_container_registry.acr.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_linux_web_app.web_app.identity[0].principal_id
}
