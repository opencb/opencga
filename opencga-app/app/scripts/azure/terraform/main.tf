variable "location" {}
variable "opencga_image" {}
variable "iva_image" {}
variable "opencga_init_image" {}
variable "batch_container_image" {}

variable "catalog_secret_key" {}

variable "opencga_admin_password" {}

variable "lets_encrypt_email_address" {
  description = "This is the email address used when obtaining SSL certs for the solution. This should be a valid email for the solution admin."
}

variable "resource_group_prefix" {
  default = "opencga"
}

// Pin to a version of the terraform provider to prevent breaking changes of future releases
// work should be undertaken to update this from time-to-time to track the lastest release
provider "azurerm" {
  version = "=1.21.0"
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

  mount_args = "azurefiles ${module.azurefiles.storage_account_name},${module.azurefiles.share_name},${module.azurefiles.storage_key}"
}

module "mongo" {
  source = "./mongo"

  location            = "${var.location}"
  resource_group_name = "${var.resource_group_prefix}-mongo"

  virtual_network_subnet_id = "${azurerm_subnet.mongo.id}"
  admin_username            = "opencga"
  ssh_key_data              = "${file("~/.ssh/id_rsa.pub")}"

  email_address = "${var.lets_encrypt_email_address}"
  cluster_size  = 3
}

resource "random_string" "webservers_dns_prefix" {
  keepers = {
    # Generate a new id each time we switch to a new resource group
    group_name = "${var.resource_group_prefix}-web"
  }

  length  = 8
  upper   = false
  special = false
  number  = false
}

locals {
  webservers_url = "http://${random_string.webservers_dns_prefix.result}.${var.location}.cloudapp.azure.com"
}

module "webservers" {
  source = "./webservers"

  location            = "${var.location}"
  resource_group_name = "${var.resource_group_prefix}-web"

  virtual_network_subnet_id = "${azurerm_subnet.web.id}"

  mount_args = "azurefiles ${module.azurefiles.storage_account_name},${module.azurefiles.share_name},${module.azurefiles.storage_key}"

  admin_username = "opencga"
  ssh_key_data   = "${file("~/.ssh/id_rsa.pub")}"

  opencga_image = "${var.opencga_image}"
  iva_image     = "${var.iva_image}"

  dns_prefix = "${random_string.webservers_dns_prefix.result}"
}

data "template_file" "opencga_init_cmd" {
  template = <<EOF
docker run --mount type=bind,src=/media/primarynfs,dst=/opt/volume \
-e OPENCGA_PASS=$${opencga_password}\
-e HBASE_SSH_DNS=$${hdinsight_ssh_dns} \
-e HBASE_SSH_USER=$${hdinsight_ssh_username}\
-e HBASE_SSH_PASS=$${hdinsight_ssh_password} \
-e SEARCH_HOSTS=$${solr_hosts_csv} \
-e CELLBASE_HOSTS=$${mongo_hosts_csv} \
-e CLINICAL_HOSTS=$${solr_hosts_csv}\
-e CATALOG_DATABASE_HOSTS=$${mongo_hosts_csv}\
-e CATALOG_DATABASE_USER=$${mongo_user}\
-e CATALOG_DATABASE_PASSWORD=$${mongo_password}\
-e CATALOG_SEARCH_HOSTS=$${solr_hosts_csv} \
-e CATALOG_SEARCH_USER=$${solr_user} \
-e CATALOG_SEARCH_PASSWORD=$${solr_password} \
-e REST_HOST=\"$${rest_host}\" \
-e GRPC_HOST=\"$${grpc_host}\"\
-e BATCH_EXEC_MODE=AZURE \
-e BATCH_ACCOUNT_NAME=$${batch_account_name} \
-e BATCH_ACCOUNT_KEY=$${batch_account_key}\
-e BATCH_ENDPOINT=$${batch_account_endpoint} \
-e BATCH_POOL_ID=$${batch_account_pool_id}\
-e BATCH_DOCKER_ARGS='$${batch_docker_args}'\
-e BATCH_DOCKER_IMAGE=$${batch_container_image} \
-e BATCH_MAX_CONCURRENT_JOBS=1
 $${opencga_init_image} $${catalog_secret_key}
      EOF

  vars {
    opencga_password       = "${var.opencga_admin_password}"
    hdinsight_ssh_dns      = "${module.hdinsight.cluster_dns}"
    hdinsight_ssh_username = "${module.hdinsight.cluster_username}"
    hdinsight_ssh_password = "${module.hdinsight.cluster_password}"
    solr_hosts_csv         = "todo"
    solr_user              = "todo"
    solr_password          = "todo"
    mongo_hosts_csv        = "${join(",", module.mongo.replica_dns_names)}"
    mongo_user             = "${module.mongo.mongo_username}"
    mongo_password         = "${module.mongo.mongo_password}"
    rest_host              = "${local.webservers_url}"
    grpc_host              = "${local.webservers_url}"
    batch_account_name     = "${module.azurebatch.batch_account_name}"
    batch_account_key      = "${module.azurebatch.batch_account_key}"
    batch_account_endpoint = "${module.azurebatch.batch_account_endpoint}"
    batch_account_pool_id  = "${module.azurebatch.batch_account_pool_id}"
    batch_docker_args      = "--mount type=bind,src=/media/primarynfs/conf,dst=/opt/opencga/conf,readonly --mount type=bind,src=/media/primarynfs/sessions,dst=/opt/opencga/sessions --mount type=bind,src=/media/primarynfs/variants,dst=/opt/opencga/variants --rm"
    batch_container_image  = "${var.batch_container_image}"
    opencga_init_image     = "${var.opencga_init_image}"
    catalog_secret_key     = "${var.catalog_secret_key}"
  }
}

module "daemonvm" {
  source = "./daemonvm"

  location            = "${var.location}"
  resource_group_name = "${var.resource_group_prefix}"

  virtual_network_subnet_id = "${azurerm_subnet.daemonvm.id}"

  mount_args = "azurefiles ${module.azurefiles.storage_account_name},${module.azurefiles.share_name},${module.azurefiles.storage_key}"

  admin_username = "opencga"
  ssh_key_data   = "${file("~/.ssh/id_rsa.pub")}"

  opencga_image          = "${var.opencga_image}"
  opencga_init_image     = "${var.opencga_init_image}"
  init_cmd               = "${data.template_file.opencga_init_cmd.rendered}"
  opencga_admin_password = "${var.opencga_admin_password}"
}
