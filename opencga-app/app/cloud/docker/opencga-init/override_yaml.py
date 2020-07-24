import oyaml as yaml
import sys
import configargparse

parser = configargparse.ArgumentParser(auto_env_var_prefix="INIT_")
parser.add_argument("--config-path", help="path to the configuration.yml file", default="/opt/opencga/conf/configuration.yml")
parser.add_argument("--client-config-path", help="path to the client-configuration.yml file", default="/opt/opencga/conf/client-configuration.yml")
parser.add_argument("--storage-config-path", help="path to the storage-configuration.yml file", default="/opt/opencga/conf/storage-configuration.yml")
parser.add_argument("--search-hosts", required=True)
parser.add_argument("--clinical-hosts", required=True)
parser.add_argument("--cellbase-mongo-hosts", required=False, help="A CSV list of mongodb hosts which are running the cellbase database")
parser.add_argument("--cellbase-mongo-hosts-password", required=False, help="The password for the cellbase mongo server provided in '--cellbase-mongo-hosts'")
parser.add_argument("--cellbase-mongo-hosts-user", required=False, help="The username for the cellbase mongo server provided in '--cellbase-mongo-hosts'")
parser.add_argument("--cellbase-rest-urls", required=False, help="A CSV list of cellbase rest servers hosting the cellbase service")
parser.add_argument("--catalog-database-hosts", required=True)
parser.add_argument("--catalog-database-user", required=True)
parser.add_argument("--catalog-database-password", required=True)
parser.add_argument("--catalog-database-ssl", required=False, default=True)
parser.add_argument("--catalog-search-hosts", required=True)
parser.add_argument("--catalog-search-user", required=False)
parser.add_argument("--catalog-search-password", required=False)
parser.add_argument("--rest-host", required=True)
parser.add_argument("--grpc-host", required=True)
parser.add_argument("--analysis-execution-mode", required=False)
parser.add_argument("--batch-account-name", required=False)
parser.add_argument("--batch-account-key", required=False)
parser.add_argument("--batch-endpoint", required=False)
parser.add_argument("--batch-pool-id", required=False)
parser.add_argument("--k8s-master-node", required=False)
parser.add_argument("--k8s-namespace", required=False, default="default")
parser.add_argument("--max-concurrent-jobs", required=False)
parser.add_argument("--variant-default-engine", required=False, default="hadoop")
parser.add_argument("--hadoop-ssh-dns", required=True)
parser.add_argument("--hadoop-ssh-user", required=True)
parser.add_argument("--hadoop-ssh-pass", required=True)
parser.add_argument("--hadoop-ssh-key", required=False)
parser.add_argument("--hadoop-ssh-remote-opencga-home", required=False)
parser.add_argument("--health-check-interval", required=False)
parser.add_argument("--save", help="save update to source configuration files (default: false)", default=False, action='store_true')
args = parser.parse_args()

# TODO: Add check for a job config.

##############################################################################################################
# Load storage configuration yaml
##############################################################################################################

with open(args.storage_config_path) as f:
    storage_config = yaml.safe_load(f)

# Inject search hosts
search_hosts = args.search_hosts.replace('\"','').replace('[','').replace(']','').split(",")
for i, search_host in enumerate(search_hosts):
    if i == 0:
        # If we are overriding the default hosts,
        # clear them only on the first iteration
        storage_config["search"]["hosts"].clear()
    storage_config["search"]["hosts"].insert(i, search_host.strip())

# Inject clinical hosts
clinical_hosts = args.clinical_hosts.replace('\"','').replace('[','').replace(']','').split(",")
for i, clinical_host in enumerate(clinical_hosts):
    if i == 0:
        # If we are overriding the default hosts,
        # clear them only on the first iteration
        storage_config["clinical"]["hosts"].clear()
    storage_config["clinical"]["hosts"].insert(i, clinical_host.strip())

# Inject cellbase database
has_cellbase_mongo_hosts = args.cellbase_mongo_hosts is not None and args.cellbase_mongo_hosts != ""

if has_cellbase_mongo_hosts:
    cellbase_mongo_hosts = args.cellbase_mongo_hosts.replace('\"','').replace('[','').replace(']','').split(",")
    for i, cellbase_mongo_host in enumerate(cellbase_mongo_hosts):
        if i == 0:
            # If we are overriding the default hosts,
            # clear them only on the first iteration
            storage_config["cellbase"]["database"]["hosts"].clear()
        storage_config["cellbase"]["database"]["hosts"].insert(i, cellbase_mongo_host.strip())

    storage_config["cellbase"]["database"]["options"]["authenticationDatabase"] = "admin"
    storage_config["cellbase"]["database"]["options"]["sslEnabled"] = True
    storage_config["cellbase"]["database"]["user"] = args.cellbase_mongo_hosts_user
    storage_config["cellbase"]["database"]["password"] = args.cellbase_mongo_hosts_password
    storage_config["cellbase"]["preferred"] = "local"

# Inject cellbase rest host, if set
if args.cellbase_rest_urls is not None and args.cellbase_rest_urls != "":
    cellbase_rest_urls = args.cellbase_rest_urls.replace('\"', '').replace('[','').replace(']','').split(",")
    if len(cellbase_rest_urls) > 0:
        for i, cellbase_url in enumerate(cellbase_rest_urls):
            if i == 0:
                # If we are overriding the default hosts,
                # clear them only on the first iteration
                storage_config["cellbase"]["hosts"].clear()
            storage_config["cellbase"]["hosts"].insert(i, cellbase_url.strip())
    
# set default engine
storage_config["variant"]["defaultEngine"] = args.variant_default_engine


# If we have cellbase hosts set the annotator to the DB Adaptor
if has_cellbase_mongo_hosts:
    storage_config["variant"]["options"]["annotator"] = "cellbase_db_adaptor"
else :
    storage_config["variant"]["options"]["annotator"] = "cellbase_rest"

# Inject Hadoop ssh configuration
for _, storage_engine in enumerate(storage_config["variant"]["engines"]):

    if storage_engine["id"] == "hadoop": 
        storage_engine["options"]["storage.hadoop.mr.executor"] = "ssh"
        storage_engine["options"]["storage.hadoop.mr.executor.ssh.host"] = args.hadoop_ssh_dns
        storage_engine["options"]["storage.hadoop.mr.executor.ssh.user"] = args.hadoop_ssh_user
        storage_engine["options"]["storage.hadoop.mr.executor.ssh.password"] = args.hadoop_ssh_pass
        if  args.hadoop_ssh_key is not None and  args.hadoop_ssh_key != "":
            storage_engine["options"]["storage.hadoop.mr.executor.ssh.key"] = args.hadoop_ssh_key
        else:
            storage_engine["options"]["storage.hadoop.mr.executor.ssh.key"] = ""

        storage_engine["options"]["storage.hadoop.mr.executor.ssh.remoteOpenCgaHome"] = args.hadoop_ssh_remote_opencga_home

##############################################################################################################
# Load configuration yaml
##############################################################################################################

with open(args.config_path) as f:
    config = yaml.safe_load(f)

# Inject catalog database
catalog_hosts = args.catalog_database_hosts.replace('\"','').replace('[','').replace(']','').split(",")
for i, catalog_host in enumerate(catalog_hosts):
    if i == 0:
        # If we are overriding the default hosts,
        # clear them only on the first iteration
        config["catalog"]["database"]["hosts"].clear()
    config["catalog"]["database"]["hosts"].insert(i, catalog_host.strip())

config["catalog"]["database"]["user"] = args.catalog_database_user
config["catalog"]["database"]["password"] = args.catalog_database_password
config["catalog"]["database"]["options"]["sslEnabled"] = args.catalog_database_ssl
config["catalog"]["database"]["options"]["sslInvalidCertificatesAllowed"] = True
config["catalog"]["database"]["options"]["authenticationDatabase"] = "admin"

# Inject search database
catalog_search_hosts = args.catalog_search_hosts.replace('\"','').replace('[','').replace(']','').split(",")
for i, catalog_search_host in enumerate(catalog_search_hosts):
    if i == 0:
        # If we are overriding the default hosts,
        # clear them only on the first iteration
        config["catalog"]["searchEngine"]["hosts"].clear()
    config["catalog"]["searchEngine"]["hosts"].insert(i, catalog_search_host.strip())

if args.catalog_search_user is not None:
    config["catalog"]["searchEngine"]["user"] = args.catalog_search_user
    config["catalog"]["searchEngine"]["password"] = args.catalog_search_password

# Inject execution settings
config["analysis"]["scratchDir"] = "/tmp"
if args.max_concurrent_jobs is not None:
    config["analysis"]["execution"]["maxConcurrentJobs"]["variant-index"] = int(args.max_concurrent_jobs)

if args.analysis_execution_mode is not None:
    config["analysis"]["execution"]["id"] = args.analysis_execution_mode

if args.analysis_execution_mode == "AZURE":
    config["analysis"]["execution"]["options"] = {}
    config["analysis"]["execution"]["options"]["azure.batchAccount"] = args.batch_account_name
    config["analysis"]["execution"]["options"]["azure.batchKey"] = args.batch_account_key
    config["analysis"]["execution"]["options"]["azure.batchUri"] = args.batch_endpoint
    config["analysis"]["execution"]["options"]["azure.batchPoolId"] = args.batch_pool_id
elif args.analysis_execution_mode == "k8s":
    config["analysis"]["execution"]["options"]["k8s.masterUrl"] = args.k8s_master_node
    config["analysis"]["execution"]["options"]["k8s.namespace"] = args.k8s_namespace

  

# Inject healthCheck interval
if args.health_check_interval is not None:
    config["healthCheck"]["interval"] = args.health_check_interval

##############################################################################################################
# Load client configuration yaml
##############################################################################################################

with open(args.client_config_path) as f:
    client_config = yaml.safe_load(f)

# Inject grpc and rest host
client_config["rest"]["host"] = args.rest_host.replace('"','')
client_config["grpc"]["host"] = args.grpc_host.replace('"','')

# Running with --save will update the configuration files inplace.
# Without --save will simply dump the update YAML to stdout so that
# the caller can handle it.
# Note: The dump will use the safe representation so there is likely
# to be format diffs between the original input and the output as well
# as value changes.
if args.save == False:
    yaml.dump(storage_config, sys.stdout, default_flow_style=False, allow_unicode=True)
    print("---")  # Add yaml delimiter
    yaml.dump(config, sys.stdout, default_flow_style=False, allow_unicode=True)
    print("---")  # Add yaml delimiter
    yaml.dump(client_config, sys.stdout, default_flow_style=False, allow_unicode=True)
else:
    with open(args.storage_config_path, "w") as f:
        yaml.dump(storage_config, f, default_flow_style=False)
    with open(args.config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False)
    with open(args.client_config_path, "w") as f:
        yaml.dump(client_config, f, default_flow_style=False)

