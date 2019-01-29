variable "virtual_network_subnet_id" {
  type = "string"
}

variable "virtual_network_id" {
  type = "string"
}

variable "location" {
  type = "string"
}

variable "resource_group_name" {
  type = "string"
}

resource "azurerm_resource_group" "hdinsight-rg" {
  name     = "${var.resource_group_name}"
  location = "${var.location}"
}

resource "random_string" "cluster_name" {
  keepers = {
    # Generate a new id each time we switch to a new resource group
    group_name = "${var.resource_group_name}"
  }

  length  = 8
  upper   = false
  special = false
  number  = false
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

resource "random_string" "cluster_password" {
  keepers = {
    # Generate a new id each time we switch to a new resource group
    group_name = "${var.resource_group_name}"
  }

  length  = 18
  upper   = true
  special = true
  number  = true
  
}

resource "random_integer" "cluster_password_int" {
  keepers = {
    # Generate a new id each time we switch to a new resource group
    group_name = "${var.resource_group_name}"
  }

  min     = 1
  max     = 99999
}

resource "azurerm_template_deployment" "hdinsight" {
  name                = "hdinsight"
  resource_group_name = "${var.resource_group_name}"

  # these key-value pairs are passed into the ARM Template's `parameters` block
  parameters {
    "clusterName"          = "${random_string.cluster_name.result}"
    "clusterLoginPassword" = "${random_string.cluster_password.result}${random_integer.cluster_password_int.result}"
    "sshPassword"          = "${random_string.cluster_password.result}${random_integer.cluster_password_int.result}"
    "storageAccountName"   = "${random_string.storage_name.result}"
    "vnetId"               = "${var.virtual_network_id}"
    "subnetId"             = "${var.virtual_network_subnet_id}"
  }

  deployment_mode = "Incremental"

  template_body = "${file("${path.module}/azuredeploy.json")}"
}

output "cluster_name" {
  value = "${random_string.cluster_name.result}"
}

output "cluster_password" {
  value = "${random_string.cluster_password.result}"
}
