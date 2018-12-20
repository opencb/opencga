import oyaml as yaml
import argparse
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--config-path", help="path to the configuration.yml file", default="/opt/opencga/conf/configuration.yml")
parser.add_argument("--client-config-path", help="path to the client-configuration.yml file", default="/opt/opencga/conf/client-configuration.yml")
parser.add_argument("--storage-config-path", help="path to the storage-configuration.yml file", default="/opt/opencga/conf/storage-configuration.yml")
parser.add_argument("--search-host", required=True)
parser.add_argument("--clinical-host", required=True)
parser.add_argument("--catalog-database-host", required=True)
parser.add_argument("--catalog-database-user", required=True)
parser.add_argument("--catalog-database-password", required=True)
parser.add_argument("--catalog-search-host", required=True)
parser.add_argument("--catalog-search-user", required=True)
parser.add_argument("--catalog-search-password", required=True)
parser.add_argument("--rest-host", required=True)
parser.add_argument("--grpc-host", required=True)
parser.add_argument("--save", help="save update to source configuration files (default: false)", default=False, action='store_true')
args = parser.parse_args()

# Load storage configuration yaml
with open(args.storage_config_path) as f:
    storage_config = yaml.load(f)

# Inject search and clinical hosts
storage_config["search"]["host"] = args.search_host
storage_config["clinical"]["host"] = args.clinical_host

# Load configuration yaml
with open(args.config_path) as f:
    config = yaml.load(f)

# Inject catalog database
config["catalog"]["database"]["hosts"][0] = args.catalog_database_host
config["catalog"]["database"]["user"] = args.catalog_database_user
config["catalog"]["database"]["password"] = args.catalog_database_password
config["catalog"]["database"]["options"]["enableSSL"] = "true"

# Inject search database
config["catalog"]["search"]["host"] = args.catalog_search_host
config["catalog"]["search"]["user"] = args.catalog_search_user
config["catalog"]["search"]["password"] = args.catalog_search_password

# Load client configuration yaml
with open(args.client_config_path) as f:
    client_config = yaml.load(f)

# Inject grpc and rest host
client_config["rest"]["host"] = args.rest_host
client_config["grpc"]["host"] = args.grpc_host

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

