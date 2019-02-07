resource "azurerm_network_security_group" "nsg-web" {
  name                = "nsg-opencga-web"
  location            = "${var.location}"
  resource_group_name = "${azurerm_resource_group.opencga.name}" 
}

resource "azurerm_network_security_group" "nsg-hdinsight" {
  name                = "nsg-opencga-hdinsight"
  location            = "${var.location}"
  resource_group_name = "${azurerm_resource_group.opencga.name}" 
}

resource "azurerm_network_security_group" "nsg-mongo" {
  name                = "nsg-opencga-mongo"
  location            = "${var.location}"
  resource_group_name = "${azurerm_resource_group.opencga.name}" 
}

resource "azurerm_network_security_group" "nsg-solr" {
  name                = "nsg-opencga-solr"
  location            = "${var.location}"
  resource_group_name = "${azurerm_resource_group.opencga.name}" 
}

resource "azurerm_network_security_group" "nsg-batch" {
  name                = "nsg-opencga"
  location            = "${var.location}"
  resource_group_name = "${azurerm_resource_group.opencga.name}" 
}

resource "azurerm_network_security_group" "nsg-avere" {
  name                = "nsg-opencga-avere"
  location            = "${var.location}"
  resource_group_name = "${azurerm_resource_group.opencga.name}" 
}

resource "azurerm_network_security_group" "nsg-daemonvm" {
  name                = "nsg-opencga-daemonvm"
  location            = "${var.location}"
  resource_group_name = "${azurerm_resource_group.opencga.name}" 
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
  resource_group_name         = "${azurerm_resource_group.opencga.name}"
  network_security_group_name = "${azurerm_network_security_group.nsg-mongo.name}"
}

resource "azurerm_network_security_rule" "allow-ssh-in" {
  name                        = "allow-ssh-in"
  priority                    = 3200
  direction                   = "Inbound"
  access                      = "Allow"
  protocol                    = "*"
  source_port_range           = "*"
  destination_port_range      = "22"
  source_address_prefix       = "*"
  destination_address_prefix  = "*"
  resource_group_name         = "${azurerm_resource_group.opencga.name}"
  network_security_group_name = "${azurerm_network_security_group.nsg-daemonvm.name}"
}

resource "azurerm_network_security_rule" "allow-all-outbound-hdinsight" {
  name                        = "allow-all-outbound-hdinsight"
  priority                    = 3300
  direction                   = "outbound"
  access                      = "Allow"
  protocol                    = "*"
  source_port_range           = "*"
  destination_port_range      = "*"
  source_address_prefix       = "*"
  destination_address_prefix  = "*"
  resource_group_name         = "${azurerm_resource_group.opencga.name}"
  network_security_group_name = "${azurerm_network_security_group.nsg-hdinsight.name}"
}


resource "azurerm_subnet_network_security_group_association" "web" {
    subnet_id                   = "${azurerm_subnet.web.id}"
    network_security_group_id   = "${azurerm_network_security_group.nsg-web.id}"
}

resource "azurerm_subnet_network_security_group_association" "hdinsight" {
    subnet_id                   = "${azurerm_subnet.hdinsight.id}"
    network_security_group_id   = "${azurerm_network_security_group.nsg-hdinsight.id}"
}

resource "azurerm_subnet_network_security_group_association" "mongo" {
    subnet_id                   = "${azurerm_subnet.mongo.id}"
    network_security_group_id   = "${azurerm_network_security_group.nsg-mongo.id}"
}

resource "azurerm_subnet_network_security_group_association" "solr" {
    subnet_id                   = "${azurerm_subnet.solr.id}"
    network_security_group_id   = "${azurerm_network_security_group.nsg-solr.id}"
}

resource "azurerm_subnet_network_security_group_association" "batch" {
    subnet_id                   = "${azurerm_subnet.batch.id}"
    network_security_group_id   = "${azurerm_network_security_group.nsg-batch.id}"
}

resource "azurerm_subnet_network_security_group_association" "avere" {
    subnet_id                   = "${azurerm_subnet.avere.id}"
    network_security_group_id   = "${azurerm_network_security_group.nsg-avere.id}"
}

resource "azurerm_subnet_network_security_group_association" "daemonvm" {
    subnet_id                   = "${azurerm_subnet.daemonvm.id}"
    network_security_group_id   = "${azurerm_network_security_group.nsg-daemonvm.id}"
}