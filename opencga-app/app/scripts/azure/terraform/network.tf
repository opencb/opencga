resource "azurerm_virtual_network" "opencga" {
  name                = "opencga-network"
  address_space       = ["10.0.0.0/16"]
  location            = "${var.location}"
  resource_group_name = "${var.existing_resource_group ? var.resource_group_prefix : join("", azurerm_resource_group.opencga.*.name)}"
}

resource "azurerm_subnet" "web" {
  name                 = "webservers"
  resource_group_name  = "${var.existing_resource_group ? var.resource_group_prefix : join("", azurerm_resource_group.opencga.*.name)}"
  virtual_network_name = "${azurerm_virtual_network.opencga.name}"
  address_prefix       = "10.0.0.0/24"
}

resource "azurerm_subnet" "hdinsight" {
  name                 = "hdinsight"
  resource_group_name  = "${var.existing_resource_group ? var.resource_group_prefix : join("", azurerm_resource_group.opencga.*.name)}"
  virtual_network_name = "${azurerm_virtual_network.opencga.name}"
  address_prefix       = "10.0.1.0/24"
  service_endpoints    = ["Microsoft.Storage"]
}

resource "azurerm_subnet" "mongo" {
  name                 = "mongo"
  resource_group_name  = "${var.existing_resource_group ? var.resource_group_prefix : join("", azurerm_resource_group.opencga.*.name)}"
  virtual_network_name = "${azurerm_virtual_network.opencga.name}"
  address_prefix       = "10.0.2.0/24"
}

resource "azurerm_subnet" "solr" {
  name                 = "solr"
  resource_group_name  = "${var.existing_resource_group ? var.resource_group_prefix : join("", azurerm_resource_group.opencga.*.name)}"
  virtual_network_name = "${azurerm_virtual_network.opencga.name}"
  address_prefix       = "10.0.3.0/24"
}

resource "azurerm_subnet" "batch" {
  name                 = "batch"
  resource_group_name  = "${var.existing_resource_group ? var.resource_group_prefix : join("", azurerm_resource_group.opencga.*.name)}"
  virtual_network_name = "${azurerm_virtual_network.opencga.name}"
  address_prefix       = "10.0.4.0/24"
}

resource "azurerm_subnet" "avere" {
  name                 = "avere"
  resource_group_name  = "${var.existing_resource_group ? var.resource_group_prefix : join("", azurerm_resource_group.opencga.*.name)}"
  virtual_network_name = "${azurerm_virtual_network.opencga.name}"
  address_prefix       = "10.0.5.0/24"
}

resource "azurerm_subnet" "daemonvm" {
  name                 = "daemonvm"
  resource_group_name  = "${var.existing_resource_group ? var.resource_group_prefix : join("", azurerm_resource_group.opencga.*.name)}"
  virtual_network_name = "${azurerm_virtual_network.opencga.name}"
  address_prefix       = "10.0.6.0/24"
}
