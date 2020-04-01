import subprocess
from shutil import copyfile
import unittest
import yaml
from io import StringIO
import sys
import os


os.chdir(sys.path[0])

class Test_init_script(unittest.TestCase):
    def setUp(self):
        if "OPENCGA_CONFIG_DIR" in  os.environ:
            config_dir = os.environ["OPENCGA_CONFIG_DIR"]
        else:
            config_dir = "./conf"

        storage_config = os.path.join(config_dir, "storage-configuration.yml")
        copyfile(storage_config, "./storage-configuration.yml")

        client_config = os.path.join(config_dir, "client-configuration.yml")
        copyfile(client_config, "./client-configuration.yml")

        config = os.path.join(config_dir, "configuration.yml")
        copyfile(config, "./configuration.yml")

    def test_end_2_end(self):
    
        res = subprocess.run(
            [
                "python3", "../override_yaml.py",
                    "--config-path", "./configuration.yml",
                    "--client-config-path", "./client-configuration.yml",
                    "--storage-config-path", "./storage-configuration.yml",
                    "--search-hosts", "test-search-host1,test-search-host2",
                    "--cellbase-mongo-hosts", "test-cellbase-host,test-cellbase-host2",
                    "--cellbase-mongo-hosts-user", "cellbaseuser",
                    "--cellbase-mongo-hosts-password", "cellbasepassword",
                    "--catalog-database-hosts", "test-catalog-database-host1,test-catalog-database-host2,test-catalog-database-host3",
                    "--catalog-database-user", "test-catalog-database-user",
                    "--catalog-database-password", "test-catalog-database-password",
                    "--catalog-search-hosts", "test-catalog-search-host1,test-catalog-search-host2",
                    "--catalog-search-user", "test-catalog-search-user",
                    "--catalog-search-password", "test-catalog-search-password",
                    "--rest-host", "test-rest-host",
                    "--grpc-host", "test-grpc-host",
                    "--max-concurrent-jobs", "25",
                    "--analysis-execution-mode", "test-analysis-execution-mode",
                    "--variant-default-engine","test-variant-default-engine",
                    "--hadoop-ssh-dns", "test-hadoop-ssh-host",
                    "--hadoop-ssh-user", "test-hadoop-ssh-user",
                    "--hadoop-ssh-pass", "test-hadoop-ssh-password",
                    "--hadoop-ssh-remote-opencga-home", "test-hadoop-ssh-remote-opencga-home",
                    "--health-check-interval", "30"
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
            env={**os.environ, "INIT_CLINICAL_HOSTS": "test-clinical-host"}, #Test that the auto import of environment vars is working
        )
        if res.returncode != 0:
            print("Error calling override_yaml.py:")
            print(res.stdout)
            sys.exit(1)

        configs = []
        configsRaw = res.stdout.decode("utf-8").split("---")

        for config in configsRaw:
            configAsFile = StringIO(config)
            configs.append(yaml.safe_load(configAsFile))

        storage_config = configs[0]
        config = configs[1]
        client_config = configs[2]

        self.assertEqual(storage_config["search"]["hosts"][0], "test-search-host1")
        self.assertEqual(storage_config["search"]["hosts"][1], "test-search-host2")
        self.assertEqual(storage_config["clinical"]["hosts"][0], "test-clinical-host")
        self.assertEqual(
            storage_config["cellbase"]["database"]["hosts"][0], "test-cellbase-host"
        )
        self.assertEqual(storage_config["cellbase"]["database"]["user"], "cellbaseuser")
        self.assertEqual(
            storage_config["cellbase"]["database"]["password"], "cellbasepassword"
        )
        self.assertEqual(
            storage_config["cellbase"]["database"]["options"]["enableSSL"], True
        )        
        self.assertEqual(
            storage_config["variant"]["defaultEngine"],
            "test-variant-default-engine",
        )
        self.assertEqual(
            storage_config["variant"]["options"]["annotator"],
            "cellbase_db_adaptor",
        )
        self.assertEqual(
            storage_config["variant"]["engines"][1]["options"][
                "storage.hadoop.mr.executor"
            ],
            "ssh",
        )
        self.assertEqual(
            storage_config["variant"]["engines"][1]["options"][
                "storage.hadoop.mr.executor.ssh.host"
            ],
            "test-hadoop-ssh-host",
        )
        self.assertEqual(
            storage_config["variant"]["engines"][1]["options"][
                "storage.hadoop.mr.executor.ssh.user"
            ],
            "test-hadoop-ssh-user",
        )
        self.assertEqual(
            storage_config["variant"]["engines"][1]["options"][
                "storage.hadoop.mr.executor.ssh.password"
            ],
            "test-hadoop-ssh-password",
        )
        self.assertEqual(
            storage_config["variant"]["engines"][1]["options"][
                "storage.hadoop.mr.executor.ssh.key"
            ],
            "",
        )
        self.assertEqual(
            storage_config["variant"]["engines"][1]["options"][
                "storage.hadoop.mr.executor.ssh.remoteOpenCgaHome"
            ],
            "test-hadoop-ssh-remote-opencga-home",
        )
        self.assertEqual(config["healthCheck"]["interval"], "30")
        self.assertEqual(
            config["catalog"]["database"]["hosts"][0], "test-catalog-database-host1"
        )
        self.assertEqual(
            config["catalog"]["database"]["hosts"][1], "test-catalog-database-host2"
        )
        self.assertEqual(
            config["catalog"]["database"]["hosts"][2], "test-catalog-database-host3"
        )
        self.assertEqual(
            config["catalog"]["database"]["user"], "test-catalog-database-user"
        )
        self.assertEqual(
            config["catalog"]["database"]["password"], "test-catalog-database-password"
        )
        self.assertEqual(config["catalog"]["database"]["options"]["enableSSL"], True)
        self.assertEqual(
            config["catalog"]["searchEngine"]["hosts"][0], "test-catalog-search-host1"
        )
        self.assertEqual(
            config["catalog"]["searchEngine"]["hosts"][1], "test-catalog-search-host2"
        )
        self.assertEqual(
            config["catalog"]["searchEngine"]["user"], "test-catalog-search-user"
        )
        self.assertEqual(
            config["catalog"]["searchEngine"]["password"], "test-catalog-search-password"
        )
        self.assertEqual(config["analysis"]["execution"]["id"], "test-analysis-execution-mode")
        self.assertEqual(config["analysis"]["index"]["variant"]["maxConcurrentJobs"], 25)
        self.assertEqual(client_config["rest"]["host"], "test-rest-host")
        self.assertEqual(client_config["grpc"]["host"], "test-grpc-host")

    def test_azure_batch_execution(self):
      
        res = subprocess.run(
            [
                "python3", "../override_yaml.py",
                    "--config-path", "./configuration.yml",
                    "--client-config-path", "./client-configuration.yml",
                    "--storage-config-path", "./storage-configuration.yml",
                    "--search-hosts", "test-search-host1,test-search-host2",
                    "--clinical-hosts",
                    "test-clinical-host",
                    "--cellbase-mongo-hosts-user", "cellbaseuser",
                    "--cellbase-mongo-hosts-password", "cellbasepassword",
                    "--catalog-database-hosts", "test-catalog-database-host1,test-catalog-database-host2,test-catalog-database-host3",
                    "--catalog-database-user", "test-catalog-database-user",
                    "--catalog-database-password", "test-catalog-database-password",
                    "--catalog-search-hosts", "test-catalog-search-host1,test-catalog-search-host2",
                    "--catalog-search-user", "test-catalog-search-user",
                    "--catalog-search-password", "test-catalog-search-password",
                    "--rest-host", "test-rest-host",
                    "--grpc-host", "test-grpc-host",
                    "--analysis-execution-mode", "AZURE",
                    "--batch-account-name", "test-batch-account-name",
                    "--batch-account-key", "test-batch-account-key",
                    "--batch-endpoint", "test-batch-endpoint",
                    "--batch-pool-id", "test-batch-pool-id",
                    "--max-concurrent-jobs", "25",
                    "--variant-default-engine","test-variant-default-engine",
                    "--hadoop-ssh-dns", "test-hadoop-ssh-host",
                    "--hadoop-ssh-user", "test-hadoop-ssh-user",
                    "--hadoop-ssh-pass", "test-hadoop-ssh-password",
                    "--hadoop-ssh-remote-opencga-home", "test-hadoop-ssh-remote-opencga-home",
                    "--health-check-interval", "30"
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,

        )
        if res.returncode != 0:
            print("Error calling override_yaml.py:")
            print(res.stdout)
            sys.exit(1)

        configs = []
        configsRaw = res.stdout.decode("utf-8").split("---")

        for config in configsRaw:
            configAsFile = StringIO(config)
            configs.append(yaml.safe_load(configAsFile))

        storage_config = configs[0]
        config = configs[1]
        client_config = configs[2]

        self.assertEqual(
            config["analysis"]["execution"]["id"], "AZURE"
        )
        self.assertEqual(
            config["analysis"]["execution"]["options"]["azure.batchAccount"], "test-batch-account-name"
        )
        self.assertEqual(
            config["analysis"]["execution"]["options"]["azure.batchKey"], "test-batch-account-key"
        )
        self.assertEqual(
            config["analysis"]["execution"]["options"]["azure.batchUri"], "test-batch-endpoint"
        )
        self.assertEqual(
            config["analysis"]["execution"]["options"]["azure.batchPoolId"], "test-batch-pool-id"
        )

        self.assertEqual(client_config["rest"]["host"], "test-rest-host")
        self.assertEqual(client_config["grpc"]["host"], "test-grpc-host")
   

    def test_kubernetes_execution(self):
      
        res = subprocess.run(
            [
                "python3", "../override_yaml.py",
                    "--config-path", "./configuration.yml",
                    "--client-config-path", "./client-configuration.yml",
                    "--storage-config-path", "./storage-configuration.yml",
                    "--search-hosts", "test-search-host1,test-search-host2",
                    "--clinical-hosts",
                    "test-clinical-host",
                    "--cellbase-mongo-hosts-user", "cellbaseuser",
                    "--cellbase-mongo-hosts-password", "cellbasepassword",
                    "--catalog-database-hosts", "test-catalog-database-host1,test-catalog-database-host2,test-catalog-database-host3",
                    "--catalog-database-user", "test-catalog-database-user",
                    "--catalog-database-password", "test-catalog-database-password",
                    "--catalog-search-hosts", "test-catalog-search-host1,test-catalog-search-host2",
                    "--catalog-search-user", "test-catalog-search-user",
                    "--catalog-search-password", "test-catalog-search-password",
                    "--rest-host", "test-rest-host",
                    "--grpc-host", "test-grpc-host",
                    "--analysis-execution-mode", "k8s",
                    "--k8s-master-node","test-k8s-master-node",
                    "--max-concurrent-jobs", "25",
                    "--variant-default-engine","test-variant-default-engine",
                    "--hadoop-ssh-dns", "test-hadoop-ssh-host",
                    "--hadoop-ssh-user", "test-hadoop-ssh-user",
                    "--hadoop-ssh-pass", "test-hadoop-ssh-password",
                    "--hadoop-ssh-remote-opencga-home", "test-hadoop-ssh-remote-opencga-home",
                    "--health-check-interval", "30"
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,

        )
        if res.returncode != 0:
            print("Error calling override_yaml.py:")
            print(res.stdout)
            sys.exit(1)

        configs = []
        configsRaw = res.stdout.decode("utf-8").split("---")

        for config in configsRaw:
            configAsFile = StringIO(config)
            configs.append(yaml.safe_load(configAsFile))

        storage_config = configs[0]
        config = configs[1]
        client_config = configs[2]
        self.assertEqual(
            config["analysis"]["scratchDir"], "/tmp"
        )
        self.assertEqual(
            config["analysis"]["execution"]["id"], "k8s"
        )
        self.assertEqual(
            config["analysis"]["execution"]["options"]["k8s.masterUrl"], "test-k8s-master-node"
        )


    def test_cellbasedb_with_empty_hosts(self):

        res = subprocess.run(
            [
                "python3",
                "../override_yaml.py",
                "--config-path",
                "./configuration.yml",
                "--client-config-path",
                "./client-configuration.yml",
                "--storage-config-path",
                "./storage-configuration.yml",
                "--cellbase-mongo-hosts",
                "",
                "--search-hosts",
                "test-search-host1,test-search-host2",
                "--clinical-hosts",
                "test-clinical-host",
                "--catalog-database-hosts",
                "test-catalog-host",
                "--catalog-database-user",
                "test-catalog-database-user",
                "--catalog-database-password",
                "test-catalog-database-password",
                "--catalog-search-hosts",
                "test-catalog-search-host1,test-catalog-search-host2",
                "--catalog-search-user",
                "test-catalog-search-user",
                "--catalog-search-password",
                "test-catalog-search-password",
                "--rest-host",
                "test-rest-host",
                "--grpc-host",
                "test-grpc-host",
                "--analysis-execution-mode",
                "test-analysis-execution-mode",
                "--batch-account-name",
                "test-batch-account-name",
                "--batch-account-key",
                "test-batch-account-key",
                "--batch-endpoint",
                "test-batch-endpoint",
                "--batch-pool-id",
                "test-batch-pool-id",
                "--max-concurrent-jobs",
                "25",
                "--variant-default-engine",
                "test-variant-default-engine",
                "--hadoop-ssh-dns",
                "test-hadoop-ssh-host",
                "--hadoop-ssh-user",
                "test-hadoop-ssh-user",
                "--hadoop-ssh-pass",
                "test-hadoop-ssh-password",
                "--hadoop-ssh-remote-opencga-home",
                "test-hadoop-ssh-remote-opencga-home",
                "--health-check-interval",
                "30",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        if res.returncode != 0:
            print("Error calling override_yaml.py:")
            print(res.stdout)
            sys.exit(1)

        configs = []
        configsRaw = res.stdout.decode("utf-8").split("---")

        for config in configsRaw:
            configAsFile = StringIO(config)
            configs.append(yaml.safe_load(configAsFile))

        storage_config = configs[0]

        self.assertEqual(
            storage_config["variant"]["options"]["annotator"],
            "cellbase_rest",
        )

    def test_cellbasedb_with_no_db_hosts(self):

        res = subprocess.run(
            [
                "python3",
                "../override_yaml.py",
                "--config-path",
                "./configuration.yml",
                "--client-config-path",
                "./client-configuration.yml",
                "--storage-config-path",
                "./storage-configuration.yml",
                "--search-hosts",
                "test-search-host1,test-search-host2",
                "--clinical-hosts",
                "test-clinical-host",
                "--catalog-database-hosts",
                "test-catalog-host",
                "--catalog-database-user",
                "test-catalog-database-user",
                "--catalog-database-password",
                "test-catalog-database-password",
                "--catalog-search-hosts",
                "test-catalog-search-host1,test-catalog-search-host2",
                "--catalog-search-user",
                "test-catalog-search-user",
                "--catalog-search-password",
                "test-catalog-search-password",
                "--rest-host",
                "test-rest-host",
                "--grpc-host",
                "test-grpc-host",
                 "--analysis-execution-mode",
                "test-analysis-execution-mode",
                "--batch-account-name",
                "test-batch-account-name",
                "--batch-account-key",
                "test-batch-account-key",
                "--batch-endpoint",
                "test-batch-endpoint",
                "--batch-pool-id",
                "test-batch-pool-id",
                "--max-concurrent-jobs",
                "25",
                "--variant-default-engine",
                "test-variant-default-engine",
                "--hadoop-ssh-dns",
                "test-hadoop-ssh-host",
                "--hadoop-ssh-user",
                "test-hadoop-ssh-user",
                "--hadoop-ssh-pass",
                "test-hadoop-ssh-password",
                "--hadoop-ssh-remote-opencga-home",
                "test-hadoop-ssh-remote-opencga-home",
                "--health-check-interval",
                "30",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        if res.returncode != 0:
            print("Error calling override_yaml.py:")
            print(res.stdout)
            sys.exit(1)

        configs = []
        configsRaw = res.stdout.decode("utf-8").split("---")

        for config in configsRaw:
            configAsFile = StringIO(config)
            configs.append(yaml.safe_load(configAsFile))

        storage_config = configs[0]

        self.assertEqual(
            storage_config["variant"]["options"]["annotator"],
            "cellbase_rest",
        )

    def test_cellbase_rest_set(self):

        res = subprocess.run(
            [
                "python3",
                "../override_yaml.py",
                "--cellbase-rest-url",
                "http://test-cellbase-server1:8080, http://test-cellbase-server2:8080",
                "--cellbase-mongo-hosts",
                "",
                "--config-path",
                "./configuration.yml",
                "--client-config-path",
                "./client-configuration.yml",
                "--storage-config-path",
                "./storage-configuration.yml",
                "--search-hosts",
                "test-search-host1,test-search-host2",
                "--clinical-hosts",
                "test-clinical-host",
                "--catalog-database-hosts",
                "test-catalog-host",
                "--catalog-database-user",
                "test-catalog-database-user",
                "--catalog-database-password",
                "test-catalog-database-password",
                "--catalog-search-hosts",
                "test-catalog-search-host1,test-catalog-search-host2",
                "--catalog-search-user",
                "test-catalog-search-user",
                "--catalog-search-password",
                "test-catalog-search-password",
                "--rest-host",
                "test-rest-host",
                "--grpc-host",
                "test-grpc-host",
                 "--analysis-execution-mode",
                "test-analysis-execution-mode",
                "--batch-account-name",
                "test-batch-account-name",
                "--batch-account-key",
                "test-batch-account-key",
                "--batch-endpoint",
                "test-batch-endpoint",
                "--batch-pool-id",
                "test-batch-pool-id",
                "--max-concurrent-jobs",
                "25",
                "--variant-default-engine",
                "test-variant-default-engine",
                "--hadoop-ssh-dns",
                "test-hadoop-ssh-host",
                "--hadoop-ssh-user",
                "test-hadoop-ssh-user",
                "--hadoop-ssh-pass",
                "test-hadoop-ssh-password",
                "--hadoop-ssh-remote-opencga-home",
                "test-hadoop-ssh-remote-opencga-home",
                "--health-check-interval",
                "30",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        if res.returncode != 0:
            print("Error calling override_yaml.py:")
            print(res.stdout)
            sys.exit(1)

        configs = []
        configsRaw = res.stdout.decode("utf-8").split("---")

        for config in configsRaw:
            configAsFile = StringIO(config)
            configs.append(yaml.safe_load(configAsFile))

        storage_config = configs[0]

        self.assertEqual(
            storage_config["variant"]["options"]["annotator"],
            "cellbase_rest",
        )
        self.assertEqual(
            storage_config["cellbase"]["hosts"][0], "http://test-cellbase-server1:8080"
        )
        self.assertEqual(
            storage_config["cellbase"]["hosts"][1], "http://test-cellbase-server2:8080"
        )

    def test_cellbase_rest_empty_set(self):

        res = subprocess.run(
            [
                "python3",
                "../override_yaml.py",
                "--cellbase-rest-url",
                "",
                "--config-path",
                "./configuration.yml",
                "--client-config-path",
                "./client-configuration.yml",
                "--storage-config-path",
                "./storage-configuration.yml",
                "--search-hosts",
                "test-search-host1,test-search-host2",
                "--clinical-hosts",
                "test-clinical-host",
                "--catalog-database-hosts",
                "test-catalog-host",
                "--catalog-database-user",
                "test-catalog-database-user",
                "--catalog-database-password",
                "test-catalog-database-password",
                "--catalog-search-hosts",
                "test-catalog-search-host1,test-catalog-search-host2",
                "--catalog-search-user",
                "test-catalog-search-user",
                "--catalog-search-password",
                "test-catalog-search-password",
                "--rest-host",
                "test-rest-host",
                "--grpc-host",
                "test-grpc-host",
                "--analysis-execution-mode",
                "test-analysis-execution-mode",
                "--batch-account-name",
                "test-batch-account-name",
                "--batch-account-key",
                "test-batch-account-key",
                "--batch-endpoint",
                "test-batch-endpoint",
                "--batch-pool-id",
                "test-batch-pool-id",
                "--max-concurrent-jobs",
                "25",
                "--variant-default-engine",
                "test-variant-default-engine",
                "--hadoop-ssh-dns",
                "test-hadoop-ssh-host",
                "--hadoop-ssh-user",
                "test-hadoop-ssh-user",
                "--hadoop-ssh-pass",
                "test-hadoop-ssh-password",
                "--hadoop-ssh-remote-opencga-home",
                "test-hadoop-ssh-remote-opencga-home",
                "--health-check-interval",
                "30",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        if res.returncode != 0:
            print("Error calling override_yaml.py:")
            print(res.stdout)
            sys.exit(1)

        configs = []
        configsRaw = res.stdout.decode("utf-8").split("---")

        for config in configsRaw:
            configAsFile = StringIO(config)
            configs.append(yaml.safe_load(configAsFile))

        storage_config = configs[0]

        self.assertEqual(
            storage_config["variant"]["options"]["annotator"],
            "cellbase_rest",
        )
        self.assertEqual(
            storage_config["cellbase"]["hosts"][0],
            "http://bioinfo.hpc.cam.ac.uk/cellbase/",
        )

    def test_cellbase_rest_not_set(self):

        res = subprocess.run(
            [
                "python3",
                "../override_yaml.py",
                "--config-path",
                "./configuration.yml",
                "--client-config-path",
                "./client-configuration.yml",
                "--storage-config-path",
                "./storage-configuration.yml",
                "--search-hosts",
                "test-search-host1,test-search-host2",
                "--clinical-hosts",
                "test-clinical-host",
                "--catalog-database-hosts",
                "test-catalog-host",
                "--catalog-database-user",
                "test-catalog-database-user",
                "--catalog-database-password",
                "test-catalog-database-password",
                "--catalog-search-hosts",
                "test-catalog-search-host1,test-catalog-search-host2",
                "--catalog-search-user",
                "test-catalog-search-user",
                "--catalog-search-password",
                "test-catalog-search-password",
                "--rest-host",
                "test-rest-host",
                "--grpc-host",
                "test-grpc-host",
                "--analysis-execution-mode",
                "test-analysis-execution-mode",
                "--batch-account-name",
                "test-batch-account-name",
                "--batch-account-key",
                "test-batch-account-key",
                "--batch-endpoint",
                "test-batch-endpoint",
                "--batch-pool-id",
                "test-batch-pool-id",
                "--max-concurrent-jobs",
                "25",
                "--variant-default-engine",
                "test-variant-default-engine",
                "--hadoop-ssh-dns",
                "test-hadoop-ssh-host",
                "--hadoop-ssh-user",
                "test-hadoop-ssh-user",
                "--hadoop-ssh-pass",
                "test-hadoop-ssh-password",
                "--hadoop-ssh-remote-opencga-home",
                "test-hadoop-ssh-remote-opencga-home",
                "--health-check-interval",
                "30",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        if res.returncode != 0:
            print("Error calling override_yaml.py:")
            print(res.stdout)
            sys.exit(1)

        configs = []
        configsRaw = res.stdout.decode("utf-8").split("---")

        for config in configsRaw:
            configAsFile = StringIO(config)
            configs.append(yaml.safe_load(configAsFile))

        storage_config = configs[0]

        self.assertEqual(
            storage_config["variant"]["options"]["annotator"],
            "cellbase_rest",
        )
        self.assertEqual(
            storage_config["cellbase"]["hosts"][0],
            "http://bioinfo.hpc.cam.ac.uk/cellbase/",
        )

# TODO: Tests for k8s config

if __name__ == "__main__":
    unittest.main()
