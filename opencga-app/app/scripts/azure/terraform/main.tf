variable "location" {}

variable "resource_group_prefix" {
  default = "opencga"
}

resource "azurerm_resource_group" "opencga" {
  name     = "${var.resource_group_prefix}"
  location = "${var.location}"
}

module "hdinsight" {
  source = "./hdinsight"

  virtual_network_id        = "${azurerm_virtual_network.opencga.id}"
  virtual_network_subnet_id = "${azurerm_subnet.hdinsight.id}"

  location            = "${var.location}"
  resource_group_name = "${var.resource_group_prefix}-hdinsight"
}

module "azurefiles" {
  source = "./azurefiles"

  location            = "${var.location}"
  resource_group_name = "${var.resource_group_prefix}-storage"
}

module "azurebatch" {
  source = "./azurebatch"

  location            = "${var.location}"
  resource_group_name = "${var.resource_group_prefix}-batch"

  virtual_network_subnet_id = "${azurerm_subnet.batch.id}"

  mount_args = "${module.azurefiles.storage_account_name},${module.azurefiles.share_name},${module.azurefiles.storage_key}"
}
