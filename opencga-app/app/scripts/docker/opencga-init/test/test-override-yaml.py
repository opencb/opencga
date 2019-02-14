import subprocess
import yaml
from io import StringIO
import sys
import os

os.chdir(sys.path[0])

print("> Running Yaml overrides")

res = subprocess.run(["python3", "../override-yaml.py",
               "--config-path", "./test-conf.yml",
               "--client-config-path", "./test-client-conf.yml",
               "--storage-config-path", "./test-storage-conf.yml",
               "--search-hosts", "test-search-host1,test-search-host2",
               "--clinical-hosts", "test-clinical-host",
               "--cellbase-hosts", "test-cellbase-host",
               "--catalog-database-hosts", "test-catalog-database-host1,test-catalog-database-host2,test-catalog-database-host3",
               "--catalog-database-user", "test-catalog-database-user",
               "--catalog-database-password", "test-catalog-database-password",
               "--catalog-search-hosts", "test-catalog-search-host1,test-catalog-search-host2",
               "--catalog-search-user", "test-catalog-search-user",
               "--catalog-search-password", "test-catalog-search-password",
               "--rest-host", "test-rest-host",
               "--grpc-host", "test-grpc-host",
               "--batch-execution-mode", "test-batch-execution-mode",
               "--batch-account-name", "test-batch-account-name",
               "--batch-account-key", "test-batch-account-key",
               "--batch-endpoint", "test-batch-endpoint",
               "--batch-pool-id", "test-batch-pool-id",
               "--batch-docker-args", "test-batch-docker-args",
               "--batch-docker-image", "test-batch-docker-image",
               "--batch-max-concurrent-jobs", "25",
               "--hadoop-ssh-host", "test-hadoop-ssh-host",
               "--hadoop-ssh-user", "test-hadoop-ssh-user",
               "--hadoop-ssh-password", "test-hadoop-ssh-password",
               "--hadoop-ssh-remote-opencga-home", "test-hadoop-ssh-remote-opencga-home"
               ],
               stdout=subprocess.PIPE,
               stderr=subprocess.STDOUT, check=False)
if res.returncode != 0:
    print("Error calling override-yaml.py:")
    print(res.stdout)
    sys.exit(1)

configs = []
configsRaw = res.stdout.decode("utf-8").split("---")

for config in configsRaw:
    configAsFile = StringIO(config)
    configs.append(yaml.load(configAsFile))

storage_config = configs[0]
config = configs[1]
client_config = configs[2]

print("> Testing results")

assert(storage_config["search"]["hosts"][0] == "test-search-host1")
assert(storage_config["search"]["hosts"][1] == "test-search-host2")
assert(storage_config["clinical"]["hosts"][0] == "test-clinical-host")
assert(storage_config["cellbase"]["database"]["hosts"][0] == "test-cellbase-host")
assert(storage_config["storageEngines"][1]["variant"]["options"]["opencga.mr.executor"] == "ssh")
assert(storage_config["storageEngines"][1]["variant"]["options"]["opencga.mr.executor.ssh.host"] == "test-hadoop-ssh-host")
assert(storage_config["storageEngines"][1]["variant"]["options"]["opencga.mr.executor.ssh.user"] == "test-hadoop-ssh-user")
assert(storage_config["storageEngines"][1]["variant"]["options"]["opencga.mr.executor.ssh.password"] == "test-hadoop-ssh-password")
assert(storage_config["storageEngines"][1]["variant"]["options"]["opencga.mr.executor.ssh.remote_opencga_home"] == "test-hadoop-ssh-remote-opencga-home")
assert(config["catalog"]["database"]["hosts"][0] == "test-catalog-database-host1")
assert(config["catalog"]["database"]["hosts"][1] == "test-catalog-database-host2")
assert(config["catalog"]["database"]["hosts"][2] == "test-catalog-database-host3")
assert(config["catalog"]["database"]["user"] == "test-catalog-database-user")
assert(config["catalog"]["database"]["password"] == "test-catalog-database-password")
assert(config["catalog"]["database"]["options"]["enableSSL"] == True)
assert(config["catalog"]["search"]["hosts"][0] == "test-catalog-search-host1")
assert(config["catalog"]["search"]["hosts"][1] == "test-catalog-search-host2")
assert(config["catalog"]["search"]["user"] == "test-catalog-search-user")
assert(config["catalog"]["search"]["password"] == "test-catalog-search-password")
assert(config["execution"]["mode"] == "test-batch-execution-mode")
assert(config["execution"]["maxConcurrentIndexJobs"] == 25)
assert(config["execution"]["options"]["batchAccount"] == "test-batch-account-name")
assert(config["execution"]["options"]["batchKey"] == "test-batch-account-key")
assert(config["execution"]["options"]["batchUri"] == "test-batch-endpoint")
assert(config["execution"]["options"]["batchPoolId"] == "test-batch-pool-id")
assert(config["execution"]["options"]["dockerImageName"] == "test-batch-docker-image")
assert(config["execution"]["options"]["dockerArgs"] == "test-batch-docker-args")
assert(client_config["rest"]["host"] == "test-rest-host")
assert(client_config["grpc"]["host"] == "test-grpc-host")


print("PASS: Yaml configuration overrides successful")