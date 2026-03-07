# 1. Define the Azure provider
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# 2. Main Resource Group
resource "azurerm_resource_group" "rg" {
  name     = "rg-navique-challenge"
  location = "West Europe"
}

# 3. Cloud Bucket for data (Storage Account)
resource "azurerm_storage_account" "sa" {
  name                     = "stnaviquedata2026"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true # Enables Data Lake Gen2
}

# 4. Container inside the Storage Account for the data files
resource "azurerm_storage_container" "data" {
  name                  = "banking-data"
  storage_account_name  = azurerm_storage_account.sa.name
  container_access_type = "private"
}

# 5. Azure Container Registry (Storage for the Docker image)
resource "azurerm_container_registry" "acr" {
  name                = "acrnavique2026" 
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "Basic"
  admin_enabled       = true
}