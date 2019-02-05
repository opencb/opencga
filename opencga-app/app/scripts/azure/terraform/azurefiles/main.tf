variable "location" {
  type = "string"
}

variable "resource_group_name" {
  type = "string"
}

resource "azurerm_resource_group" "storage" {
  name     = "${var.resource_group_name}"
  location = "${var.location}"
}


resource "random_string" "storage_name" {
  keepers = {
    # Generate a new id each time we switch to a new resource group
    group_name = "${var.resource_group_name}"
  }

  length  = 8
  upper   = false
  special = false
  number  = false
}


resource "azurerm_storage_account" "storage" {
  name                     = "${random_string.storage_name.result}"
  resource_group_name      = "${azurerm_resource_group.storage.name}"
  location                 = "${var.location}"
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_share" "share" {
  name = "opencgashare"

  resource_group_name  = "${azurerm_resource_group.storage.name}"
  storage_account_name = "${azurerm_storage_account.storage.name}"

  quota = 50
}

output "storage_account_name" {
  value = "${random_string.storage_name.result}"
}

output "storage_key" {
    value = "${azurerm_storage_account.storage.primary_access_key}"
}

output "share_name" {
  value = "opencgashare"
}


