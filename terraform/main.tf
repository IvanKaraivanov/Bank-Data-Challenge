# 1. Terraform Configuration & Providers
terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 3.0" }
    azuread = { source = "hashicorp/azuread", version = "~> 2.0" }
  }
}

provider "azurerm" {
  features {}
}
provider "azuread" {}

# 2. Main Resource Group
resource "azurerm_resource_group" "rg" {
  name     = "rg-navique-challenge"
  location = "West Europe"
}

# 3. Data Lake Storage Gen2 (with HNS enabled)
resource "azurerm_storage_account" "sa" {
  name                     = "stnaviquedata2026"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true # Critical for Spark performance
}

# 4. Logical Environments: Test and Production Containers
resource "azurerm_storage_container" "test_data" {
  name                  = "test-data"
  storage_account_name  = azurerm_storage_account.sa.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "prod_data" {
  name                  = "prod-data"
  storage_account_name  = azurerm_storage_account.sa.name
  container_access_type = "private"
}

# 5. Azure Container Registry for Docker images
resource "azurerm_container_registry" "acr" {
  name                = "acrnavique2026"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "Basic"
  admin_enabled       = true
}

# 6. Identity: Service Principal for Spark Authentication
resource "azuread_application" "spark_app" {
  display_name = "sp-navique-spark"
}

resource "azuread_service_principal" "spark_sp" {
  application_id = azuread_application.spark_app.application_id
}

resource "azuread_service_principal_password" "spark_password" {
  service_principal_id = azuread_service_principal.spark_sp.id
}

# 7. Security: Role-Based Access Control (RBAC)
resource "azurerm_role_assignment" "storage_role" {
  scope                = azurerm_storage_account.sa.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azuread_service_principal.spark_sp.object_id
}

# 8. Secret Management: Azure Key Vault
resource "azurerm_key_vault" "kv" {
  name                = "kv-navique-2026"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id # Current user (You)
    secret_permissions = ["Get", "List", "Set", "Delete"]
  }
}

# 9. Store Service Principal Secret in Key Vault
resource "azurerm_key_vault_secret" "spark_secret" {
  name         = "AZURE-CLIENT-SECRET"
  value        = azuread_service_principal_password.spark_password.value
  key_vault_id = azurerm_key_vault.kv.id
}

# 10. Outputs for Manual Setup in Azure DevOps
output "AZURE_CLIENT_ID" { value = azuread_application.spark_app.application_id }
output "AZURE_TENANT_ID" { value = data.azurerm_client_config.current.tenant_id }
output "KEY_VAULT_NAME"  { value = azurerm_key_vault.kv.name }

data "azurerm_client_config" "current" {}