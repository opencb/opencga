variable "location" {
  type = "string"
}

variable "resource_group_name" {
  type = "string"
}

variable "log_analytics_sku" {}


resource "azurerm_resource_group" "opencga" {
  name     = "${var.resource_group_name}"
  location = "${var.location}"
}


resource "random_string" "log_name" {
  keepers = {
    # Generate a new id each time we switch to a new resource group
    group_name = "${var.resource_group_name}"
  }

  length  = 8
  upper   = false
  special = false
  number  = false
}


resource "azurerm_log_analytics_workspace" "opencga" {
  name                = "${random_string.log_name.result}"
  location            = "${azurerm_resource_group.opencga.location}"
  resource_group_name = "${azurerm_resource_group.opencga.name}"
  sku                 = "${var.log_analytics_sku}"
}

resource "azurerm_log_analytics_solution" "servicemap" {
  solution_name         = "ServiceMap"
  location              = "${azurerm_resource_group.opencga.location}"
  resource_group_name   = "${azurerm_resource_group.opencga.name}"
  workspace_resource_id = "${azurerm_log_analytics_workspace.opencga.id}"
  workspace_name        = "${azurerm_log_analytics_workspace.opencga.name}"

  plan {
    publisher = "Microsoft"
    product   = "OMSGallery/ServiceMap"
  }
}


resource "azurerm_log_analytics_solution" "infra" {
  solution_name         = "InfrastructureInsights"
  location              = "${azurerm_resource_group.opencga.location}"
  resource_group_name   = "${azurerm_resource_group.opencga.name}"
  workspace_resource_id = "${azurerm_log_analytics_workspace.opencga.id}"
  workspace_name        = "${azurerm_log_analytics_workspace.opencga.name}"

  plan {
    publisher = "Microsoft"
    product   = "OMSGallery/InfrastructureInsights"
  }
}


resource "azurerm_log_analytics_solution" "malware" {
  solution_name         = "AntiMalware"
  location              = "${azurerm_resource_group.opencga.location}"
  resource_group_name   = "${azurerm_resource_group.opencga.name}"
  workspace_resource_id = "${azurerm_log_analytics_workspace.opencga.id}"
  workspace_name        = "${azurerm_log_analytics_workspace.opencga.name}"

  plan {
    publisher = "Microsoft"
    product   = "OMSGallery/AntiMalware"
  }
}


resource "azurerm_log_analytics_solution" "appgw" {
  solution_name         = "AzureAppGatewayAnalytics"
  location              = "${azurerm_resource_group.opencga.location}"
  resource_group_name   = "${azurerm_resource_group.opencga.name}"
  workspace_resource_id = "${azurerm_log_analytics_workspace.opencga.id}"
  workspace_name        = "${azurerm_log_analytics_workspace.opencga.name}"

  plan {
    publisher = "Microsoft"
    product   = "OMSGallery/AzureAppGatewayAnalytics"
  }
}

resource "azurerm_log_analytics_solution" "containers" {
  solution_name         = "Containers"
  location              = "${azurerm_resource_group.opencga.location}"
  resource_group_name   = "${azurerm_resource_group.opencga.name}"
  workspace_resource_id = "${azurerm_log_analytics_workspace.opencga.id}"
  workspace_name        = "${azurerm_log_analytics_workspace.opencga.name}"

  plan {
    publisher = "Microsoft"
    product   = "OMSGallery/Containers"
  }
}


resource "azurerm_log_analytics_solution" "hdinsight" {
  solution_name         = "HDInsight"
  location              = "${azurerm_resource_group.opencga.location}"
  resource_group_name   = "${azurerm_resource_group.opencga.name}"
  workspace_resource_id = "${azurerm_log_analytics_workspace.opencga.id}"
  workspace_name        = "${azurerm_log_analytics_workspace.opencga.name}"

  plan {
    publisher = "Microsoft"
    product   = "OMSGallery/HDInsight"
  }
}


resource "azurerm_log_analytics_solution" "security" {
  solution_name         = "Security"
  location              = "${azurerm_resource_group.opencga.location}"
  resource_group_name   = "${azurerm_resource_group.opencga.name}"
  workspace_resource_id = "${azurerm_log_analytics_workspace.opencga.id}"
  workspace_name        = "${azurerm_log_analytics_workspace.opencga.name}"

  plan {
    publisher = "Microsoft"
    product   = "OMSGallery/Security"
  }
}

output "workspace_resource_id" {
  value = "${azurerm_log_analytics_workspace.opencga.id}"
}

output "workspace_id" {
    value = "${azurerm_log_analytics_workspace.opencga.workspace_id}"
}

output "workspace_key" {
    value = "${azurerm_log_analytics_workspace.opencga.primary_shared_key}"
}