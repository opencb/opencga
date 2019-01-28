
resource "azurerm_virtual_network" "opencga" {
  name                = "opencga-network"
  address_space       = ["10.0.0.0/16"]
  location            = "${azurerm_resource_group.root.location}"
  resource_group_name = "${azurerm_resource_group.root.name}"
}

resource "azurerm_subnet" "web" {
  name                 = "webservers"
  resource_group_name  = "${azurerm_resource_group.opencga.name}"
  virtual_network_name = "${azurerm_virtual_network.opencga.name}"
  address_prefix       = "10.0.0.0/24"
}


resource "azurerm_subnet" "hdinsight" {
  name                 = "webservers"
  resource_group_name  = "${azurerm_resource_group.opencga.name}"
  virtual_network_name = "${azurerm_virtual_network.opencga.name}"
  address_prefix       = "10.0.1.0/24"
}


resource "azurerm_subnet" "mongo" {
  name                 = "webservers"
  resource_group_name  = "${azurerm_resource_group.opencga.name}"
  virtual_network_name = "${azurerm_virtual_network.opencga.name}"
  address_prefix       = "10.0.2.0/24"
}


resource "azurerm_subnet" "solr" {
  name                 = "webservers"
  resource_group_name  = "${azurerm_resource_group.opencga.name}"
  virtual_network_name = "${azurerm_virtual_network.opencga.name}"
  address_prefix       = "10.0.3.0/24"
}



resource "azurerm_subnet" "batch" {
  name                 = "webservers"
  resource_group_name  = "${azurerm_resource_group.opencga.name}"
  virtual_network_name = "${azurerm_virtual_network.opencga.name}"
  address_prefix       = "10.0.4.0/24"
}


resource "azurerm_subnet" "avere" {
  name                 = "webservers"
  resource_group_name  = "${azurerm_resource_group.opencga.name}"
  virtual_network_name = "${azurerm_virtual_network.opencga.name}"
  address_prefix       = "10.0.5.0/24"
}