resource "azurerm_network_security_group" "nsg-web" {
  name                = "nsg-opencga"
  location            = "${var.location}"
  resource_group_name = "${var.resource_group_prefix}" 
}

resource "azurerm_network_security_group" "nsg-hdinsight" {
  name                = "nsg-opencga"
  location            = "${var.location}"
  resource_group_name = "${var.resource_group_prefix}" 
}

resource "azurerm_network_security_group" "nsg-mongo" {
  name                = "nsg-opencga"
  location            = "${var.location}"
  resource_group_name = "${var.resource_group_prefix}" 
}

resource "azurerm_network_security_group" "nsg-solr" {
  name                = "nsg-opencga"
  location            = "${var.location}"
  resource_group_name = "${var.resource_group_prefix}" 
}

resource "azurerm_network_security_group" "nsg-batch" {
  name                = "nsg-opencga"
  location            = "${var.location}"
  resource_group_name = "${var.resource_group_prefix}" 
}

resource "azurerm_network_security_group" "nsg-avere" {
  name                = "nsg-opencga"
  location            = "${var.location}"
  resource_group_name = "${var.resource_group_prefix}" 
}

resource "azurerm_network_security_group" "nsg-daemonvm" {
  name                = "nsg-opencga"
  location            = "${var.location}"
  resource_group_name = "${var.resource_group_prefix}" 
}

resource "azurerm_network_security_rule" "allow-mongo-in" {
  name                        = "allow-mongo-in"
  priority                    = 3100
  direction                   = "Inbound"
  access                      = "Allow"
  protocol                    = "*"
  source_port_range           = "*"
  destination_port_range      = "27017"
  source_address_prefix       = "*"
  destination_address_prefix  = "*"
  resource_group_name         = "${var.resource_group_prefix}"
  network_security_group_name = "${azurerm_network_security_group.nsg-mongo.name}"
}

resource "azurerm_network_security_rule" "allow-ssh-in" {
  name                        = "allow-ssh-in"
  priority                    = 3100
  direction                   = "Inbound"
  access                      = "Allow"
  protocol                    = "*"
  source_port_range           = "*"
  destination_port_range      = "22"
  source_address_prefix       = "*"
  destination_address_prefix  = "*"
  resource_group_name         = "${var.resource_group_prefix}"
  network_security_group_name = "${azurerm_network_security_group.nsg-daemonvm.name}"
}

resource "azurerm_network_security_rule" "allow-all-outbound-hdinsight" {
  name                        = "allow-all-outbound-hdinsight"
  priority                    = 3100
  direction                   = "outbound"
  access                      = "Allow"
  protocol                    = "*"
  source_port_range           = "*"
  destination_port_range      = "*"
  source_address_prefix       = "*"
  destination_address_prefix  = "*"
  resource_group_name         = "${var.resource_group_prefix}"
  network_security_group_name = "${azurerm_network_security_group.nsg-hdinsight.name}"
}


resource "azurerm_subnet_network_security_group_association" "web" {
    subnet_id                   = "${azurerm_subnet.web.name}"
    network_security_group_id   = "${azurerm_network_security_group.nsg-web.name}"
}

resource "azurerm_subnet_network_security_group_association" "hdinsight" {
    subnet_id                   = "${azurerm_subnet.hdinsight.name}"
    network_security_group_id   = "${azurerm_network_security_group.nsg-hdinsight.name}"
}

resource "azurerm_subnet_network_security_group_association" "mongo" {
    subnet_id                   = "${azurerm_subnet.mongo.name}"
    network_security_group_id   = "${azurerm_network_security_group.nsg-mongo.name}"
}

resource "azurerm_subnet_network_security_group_association" "solr" {
    subnet_id                   = "${azurerm_subnet.solr.name}"
    network_security_group_id   = "${azurerm_network_security_group.nsg-solr.name}"
}

resource "azurerm_subnet_network_security_group_association" "batch" {
    subnet_id                   = "${azurerm_subnet.batch.name}"
    network_security_group_id   = "${azurerm_network_security_group.nsg-batch.name}"
}

resource "azurerm_subnet_network_security_group_association" "avere" {
    subnet_id                   = "${azurerm_subnet.avere.name}"
    network_security_group_id   = "${azurerm_network_security_group.nsg-avere.name}"
}

resource "azurerm_subnet_network_security_group_association" "daemonvm" {
    subnet_id                   = "${azurerm_subnet.daemonvm.name}"
    network_security_group_id   = "${azurerm_network_security_group.nsg-daemonvm.name}"
}