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

variable "opencga_image" {}
variable "iva_image" {}

resource "azurerm_resource_group" "batch" {
  name     = "${var.resource_group_name}"
  location = "${var.location}"
}

resource "random_string" "dns_label" {
  keepers = {
    # Generate a new id each time we switch to a new resource group
    group_name = "${var.resource_group_name}"
  }

  length  = 8
  upper   = false
  special = false
  number  = false
}

resource "azurerm_public_ip" "ip" {
  name                = "webservers-publicip"
  location            = "${azurerm_resource_group.batch.location}"
  resource_group_name = "${azurerm_resource_group.batch.name}"
  allocation_method   = "Static"
  domain_name_label   = "${random_string.dns_label.result}"
}

resource "azurerm_lb" "lb" {
  name                = "webservers-loadbalancer"
  location            = "${azurerm_resource_group.batch.location}"
  resource_group_name = "${azurerm_resource_group.batch.name}"

  frontend_ip_configuration {
    name                 = "PublicIPAddress"
    public_ip_address_id = "${azurerm_public_ip.ip.id}"
  }
}

resource "azurerm_lb_backend_address_pool" "bpepool" {
  resource_group_name = "${azurerm_resource_group.batch.name}"
  loadbalancer_id     = "${azurerm_lb.lb.id}"
  name                = "BackEndAddressPool"
}

resource "azurerm_lb_nat_pool" "lbnatpool" {
  count                          = 3
  resource_group_name            = "${azurerm_resource_group.batch.name}"
  name                           = "ssh"
  loadbalancer_id                = "${azurerm_lb.lb.id}"
  protocol                       = "Tcp"
  frontend_port_start            = 50000
  frontend_port_end              = 50119
  backend_port                   = 22
  frontend_ip_configuration_name = "PublicIPAddress"
}

resource "azurerm_lb_probe" "probe" {
  resource_group_name = "${azurerm_resource_group.batch.name}"
  loadbalancer_id     = "${azurerm_lb.lb.id}"
  name                = "http-probe"
  request_path        = "/opencga/webservices/rest/v1/meta/status"
  port                = 8080
  protocol            = "http"
}

resource "azurerm_lb_rule" "webservices" {
  resource_group_name            = "${azurerm_resource_group.batch.name}"
  loadbalancer_id                = "${azurerm_lb.lb.id}"
  name                           = "opencgawebservices"
  protocol                       = "Tcp"
  frontend_port                  = 8080
  backend_port                   = 8080
  frontend_ip_configuration_name = "PublicIPAddress"
  probe_id                       = "${azurerm_lb_probe.probe.id}"

  backend_address_pool_id = "${azurerm_lb_backend_address_pool.bpepool.id}"
}

data "template_file" "cloud_init" {
  template = "${file("${path.module}/cloudinit.tmpl.yaml")}"

  vars {
    mount_args              = "${var.mount_args}"
    opencga_image           = "${var.opencga_image}"
    iva_image               = "${var.iva_image}"
    cloud_init_check_script = "${base64gzip(file("${path.module}/../scripts/cloudinitcheck.sh"))}"
    mount_script            = "${base64gzip(file("${path.module}/../scripts/mount.py"))}"
  }
}

# Render a multi-part cloud-init config making use of the part
# above, and other source files
data "template_cloudinit_config" "config" {
  gzip          = true
  base64_encode = true

  # Main cloud-config configuration file.
  part {
    filename     = "init.cfg"
    content_type = "text/cloud-config"
    content      = "${data.template_file.cloud_init.rendered}"
  }
}

resource "azurerm_virtual_machine_scale_set" "webservers" {
  name                = "opencga-webservers"
  location            = "${azurerm_resource_group.batch.location}"
  resource_group_name = "${azurerm_resource_group.batch.name}"

  # automatic rolling upgrade
  automatic_os_upgrade = true
  upgrade_policy_mode  = "Rolling"

  # rolling_upgrade_policy {
  #   max_batch_instance_percent              = 20
  #   max_unhealthy_instance_percent          = 20
  #   max_unhealthy_upgraded_instance_percent = 5
  #   pause_time_between_batches              = "PT0S"
  # }
  upgrade_policy_mode = "Automatic"

  # required when using rolling upgrade policy
  health_probe_id = "${azurerm_lb_probe.probe.id}"

  sku {
    name     = "Standard_F2"
    tier     = "Standard"
    capacity = 2
  }

  storage_profile_image_reference {
    publisher = "Canonical"
    offer     = "UbuntuServer"
    sku       = "18.04-LTS"
    version   = "latest"
  }

  storage_profile_os_disk {
    name              = ""
    caching           = "ReadWrite"
    create_option     = "FromImage"
    managed_disk_type = "Standard_LRS"
  }

  os_profile {
    computer_name_prefix = "opencgaweb"
    admin_username       = "${var.admin_username}"
    custom_data          = "${data.template_cloudinit_config.config.rendered}"
  }

  os_profile_linux_config {
    disable_password_authentication = true

    ssh_keys {
      path     = "/home/${var.admin_username}/.ssh/authorized_keys"
      key_data = "${var.ssh_key_data}"
    }
  }

  extension = {
    name                       = "cloudinit-check"
    publisher                  = "Microsoft.Azure.Extensions"
    type                       = "CustomScript"
    type_handler_version       = "2.0"
    auto_upgrade_minor_version = true

    settings = <<SETTINGS
    {
        "commandToExecute": "/bin/bash -f /opt/cloudinitcheck.sh"
    }
    SETTINGS
  }

  network_profile {
    name    = "networkprofile"
    primary = true

    ip_configuration {
      name                                   = "ipconfig"
      primary                                = true
      subnet_id                              = "${var.virtual_network_subnet_id}"
      load_balancer_backend_address_pool_ids = ["${azurerm_lb_backend_address_pool.bpepool.id}"]
      load_balancer_inbound_nat_rules_ids    = ["${element(azurerm_lb_nat_pool.lbnatpool.*.id, count.index)}"]
    }
  }
}
