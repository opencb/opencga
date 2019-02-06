variable "virtual_network_subnet_id" {
  type = "string"
}

variable "location" {
  type = "string"
}

variable "resource_group_name" {
  type = "string"
}

variable "mount_args" {
  type = "string"
}

resource "azurerm_resource_group" "batch" {
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

locals {
  // Zip up and base64 the command then ship it to python with the mount_args set
  startupCmd = "/bin/bash -c \" echo ${base64gzip(file("${path.module}/../scripts/mount.py"))} | base64 -d | gunzip | python3 - ${var.mount_args}\""
}

resource "random_string" "batch_name" {
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
  resource_group_name      = "${azurerm_resource_group.batch.name}"
  location                 = "${azurerm_resource_group.batch.location}"
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_template_deployment" "batchpool" {
  name                = "batchpool"
  resource_group_name = "${azurerm_resource_group.opencga.name}"

  # these key-value pairs are passed into the ARM Template's `parameters` block
  parameters {
    "virtualNetworkSubnetId" = "${var.virtual_network_subnet_id}"
    "storageAccountId"       = "${azurerm_storage_account.storage.id}"
    "startupCmd"             = "${local.startupCmd}"

    # //Todo: pull in images that we actually use
    # "dockerImagesToCache" = "['ubuntu']"
  }

  deployment_mode = "Incremental"

  template_body = "${file("${path.module}/azuredeploy.json")}"
}

output "batch_account_endpoint" {
  value = "${azurerm_template_deployment.batchpool.outputs["batchEndpoint"]}"

}

output "batch_account_name" {
  value = "${azurerm_template_deployment.batchpool.outputs["batchAccountName"]}"

}

output "batch_account_pool_id" {
  value = "${azurerm_template_deployment.batchpool.outputs["batchPoolId"]}"
}

output "batch_account_key" {
  value = "${azurerm_template_deployment.batchpool.outputs["batchAccountKey"]}"
}
