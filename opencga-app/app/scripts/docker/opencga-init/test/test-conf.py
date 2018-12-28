import subprocess
import yaml
from io import StringIO

res = subprocess.run(["python3", "../init-config.py",
               "--config-path", "./test-conf.yml",
               "--client-config-path", "./test-client-conf.yml",
               "--storage-config-path", "./test-storage-conf.yml",
               "--search-host", "test-search-host",
               "--clinical-host", "test-clinical-host",
               "--cellbase-host", "test-cellbase-host",
               "--catalog-database-host", "test-catalog-database-host",
               "--catalog-database-user", "test-catalog-database-user",
               "--catalog-database-password", "test-catalog-database-password",
               "--catalog-search-host", "test-catalog-search-host",
               "--catalog-search-user", "test-catalog-search-user",
               "--catalog-search-password", "test-catalog-search-password",
               "--rest-host", "test-rest-host",
               "--grpc-host", "test-grpc-host"],
               stdout=subprocess.PIPE,
               stderr=subprocess.STDOUT, check=True)
configs = []
configsRaw = res.stdout.decode("utf-8").split("---")

for config in configsRaw:
    configAsFile = StringIO(config)
    configs.append(yaml.load(configAsFile))

storage_config = configs[0]
config = configs[1]
client_config = configs[2]

assert(storage_config["search"]["host"] == "test-search-host")
assert(storage_config["clinical"]["host"] == "test-clinical-host")
assert(storage_config["cellbase"]["hosts"][0] == "test-cellbase-host")
assert(config["catalog"]["database"]["hosts"][0] == "test-catalog-database-host")
assert(config["catalog"]["database"]["user"] == "test-catalog-database-user")
assert(config["catalog"]["database"]["password"] == "test-catalog-database-password")
assert(config["catalog"]["database"]["options"]["enableSSL"] == True)
assert(config["catalog"]["search"]["host"] == "test-catalog-search-host")
assert(config["catalog"]["search"]["user"] == "test-catalog-search-user")
assert(config["catalog"]["search"]["password"] == "test-catalog-search-password")
assert(client_config["rest"]["host"] == "test-rest-host")
assert(client_config["grpc"]["host"] == "test-grpc-host")

print("Successfully tested configuration update")
