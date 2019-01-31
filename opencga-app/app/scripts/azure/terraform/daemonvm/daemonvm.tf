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
variable "opencga_init_image" {}

variable init_cmd {}
variable "opencga_admin_password" {
  
}


resource "azurerm_resource_group" "opencga" {
  name     = "${var.resource_group_name}"
  location = "${var.location}"
}

locals {
  daemon_start_cmd = "docker run --mount type=bind,src=/media/primarynfs/conf,dst=/opt/opencga/conf,readonly --mount type=bind,src=/media/primarynfs/sessions,dst=/opt/opencga/sessions -e OPENCGA_PASS=${var.opencga_admin_password} ${var.opencga_image})"
}

data "template_file" "cloud_init" {
  template = "${file("${path.module}/cloudinit.tmpl.yaml")}"

  vars {
    mount_args       = "${var.mount_args}"
    init_cmd         = "${var.init_cmd}"
    daemon_start_cmd = "${local.daemon_start_cmd}"
    mount_script     = "${base64gzip(file("${path.module}/../scripts/mount.py"))}"
    cloud_init_check_script = "${base64gzip(file("${path.module}/../scripts/cloudinitcheck.sh"))}"
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

resource "azurerm_public_ip" "daemon" {
  name              = "daemon-pip"
  allocation_method = "Dynamic"

  location            = "${var.location}"
  resource_group_name = "${var.resource_group_name}"
}


resource "azurerm_network_interface" "daemon" {
  name                = "daemon-nic"
  location            = "${azurerm_resource_group.opencga.location}"
  resource_group_name = "${azurerm_resource_group.opencga.name}"

  ip_configuration {
    name                          = "ipconfig"
    subnet_id                     = "${var.virtual_network_subnet_id}"
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = "${azurerm_public_ip.daemon.id}"
  }
}

resource "azurerm_virtual_machine" "daemon" {
  name                  = "daemon-vm"
  location              = "${azurerm_resource_group.opencga.location}"
  resource_group_name   = "${azurerm_resource_group.opencga.name}"
  network_interface_ids = ["${azurerm_network_interface.daemon.id}"]
  vm_size               = "Standard_DS1_v2"

  storage_image_reference {
    publisher = "Canonical"
    offer     = "UbuntuServer"
    sku       = "18.04-LTS"
    version   = "latest"
  }

  storage_os_disk {
    name              = "daemon-os-disk"
    caching           = "ReadWrite"
    create_option     = "FromImage"
    managed_disk_type = "Standard_LRS"
  }

  os_profile {
    computer_name  = "daemonvm"
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

resource "azurerm_virtual_machine_extension" "daemon" {
  name                 = "hostname"
  location             = "${azurerm_resource_group.opencga.location}"
  resource_group_name  = "${azurerm_resource_group.opencga.name}"
  virtual_machine_name = "${azurerm_virtual_machine.daemon.name}"
  publisher            = "Microsoft.Azure.Extensions"
  type                 = "CustomScript"
  type_handler_version = "2.0"

  settings = <<SETTINGS
    {
        "commandToExecute": "/bin/bash -f /opt/cloudinitcheck.sh"
    }
    SETTINGS
}
