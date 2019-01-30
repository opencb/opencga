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

variable "ssh_key_data" {
  type = "string"
}

variable "admin_username" {
  type = "string"
}

variable "cluster_size" {}

resource "azurerm_resource_group" "opencga" {
  name     = "${var.resource_group_name}-mongo"
  location = "${var.location}"
}

resource "random_string" "dns_name" {
  keepers = {
    # Generate a new id each time we switch to a new resource group
    group_name = "${var.resource_group_name}"
  }

  length  = 12
  upper   = true
  special = true
  number  = true
}

resource "random_string" "password" {
  keepers = {
    # Generate a new id each time we switch to a new resource group
    group_name = "${var.resource_group_name}"
  }

  length  = 18
  upper   = true
  special = true
  number  = true
}

resource "azurerm_public_ip" "mongo" {
  count = "${var.cluster_size}"

  name              = "mongopip-${count.index}"
  domain_name_label = "${random_string.dns_name.result}"

  location            = "${var.location}"
  resource_group_name = "${var.resource_group_name}"
}

resource "azurerm_network_interface" "mongo" {
  count               = "${var.cluster_size}"
  name                = "mongo-nic-${count.index}"
  location            = "${azurerm_resource_group.opencga.location}"
  resource_group_name = "${azurerm_resource_group.opencga.name}"

  ip_configuration {
    name                          = "ipconfig"
    subnet_id                     = "${var.virtual_network_subnet_id}"
    private_ip_address_allocation = "Static"
    internal_dns_name_label       = "mongoreplica-${count.index}"
    public_ip_address_id          = "${element(azurerm_public_ip.mongo.*.id, count.index)}"
  }
}

data "template_file" "cloud_init" {
  count    = "${var.cluster_size}"
  template = "${file("${path.module}/cloudinit.tmpl.yaml")}"

  vars {
    post_deploy_script      = "${base64gzip(file("${path.module}/postdeploy.sh"))}"
    renew_mongo_cert_script = "${base64gzip(file("${path.module}/renew_mongo_cert.sh"))}"
    cloud_init_check_script = "${base64gzip(file("${path.module}/../scripts/cloudinitcheck.sh"))}"
    index                   = "${count.index}"
    size                    = "${var.cluster_size}"
    email                   = "${var.email_address}"
    fqdn                    = "${element(azurerm_public_ip.mongo.*.fqdn, count.index)}"
    usename                 = "${var.admin_username}"
    password                = "${random_string.password.result}"
  }
}

# Render a multi-part cloud-init config making use of the part
# above, and other source files
data "template_cloudinit_config" "config" {
  count         = "${var.cluster_size}"
  gzip          = true
  base64_encode = true

  # Main cloud-config configuration file.
  part {
    filename     = "init.cfg"
    content_type = "text/cloud-config"
    content      = "${data.template_file.cloud_init.rendered}"
  }
}

resource "azurerm_virtual_machine" "mongo" {
  count = "${var.cluster_size}"

  name                  = "mongo-vm-${count.index}"
  location              = "${azurerm_resource_group.opencga.location}"
  resource_group_name   = "${azurerm_resource_group.opencga.name}"
  network_interface_ids = ["${element(azurerm_network_interface.mongo.*.id, count.index)}"]
  vm_size               = "Standard_DS1_v2"

  storage_image_reference {
    publisher = "Canonical"
    offer     = "UbuntuServer"
    sku       = "16.04-LTS"
    version   = "latest"
  }

  storage_os_disk {
    name              = "mongoosdisk"
    caching           = "ReadWrite"
    create_option     = "FromImage"
    managed_disk_type = "Standard_LRS"
  }

  os_profile {
    computer_name  = "mongo-vm-${count.index}"
    admin_username = "${var.admin_username}"
    custom_data    = "${data.template_cloudinit_config.config.rendered}"
  }

  os_profile_linux_config {
    disable_password_authentication = true

    ssh_keys {
      path     = "/home/${var.admin_username}/.ssh/authorized_keys"
      key_data = "${var.ssh_key_data}"
    }
  }
}

resource "azurerm_virtual_machine_extension" "mongo" {
  count = "${var.cluster_size}"

  name                 = "check-cloud-init"
  location             = "${azurerm_resource_group.opencga.location}"
  resource_group_name  = "${azurerm_resource_group.opencga.name}"
  virtual_machine_name = "${element(azurerm_virtual_machine.mongo.*.name, count.index)}"
  publisher            = "Microsoft.Azure.Extensions"
  type                 = "CustomScript"
  type_handler_version = "2.0"

  settings = <<SETTINGS
    {
        "commandToExecute": "/bin/bash -f /opt/cloudinitcheck.sh"
    }
    SETTINGS
}

output "replica_dns_names" {
  value = ["${azurerm_public_ip.mongo.*.fqdn}"]
}