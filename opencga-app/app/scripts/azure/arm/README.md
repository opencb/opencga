# ARM Templates and Deploying OpenCGA to Azure

This document contains information related to the deployment of OpenCGA to Azure using ARM automation scripts.

## Deploy to Azure

<a href="https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Fopencb%2Fopencga%2Fazure%2Fopencga-app%2Fapp%2Fscripts%2Fazure%2Farm%2Fazuredeploy.json" target="_blank">
    <img src="http://azuredeploy.net/deploybutton.png"/>
</a>

## T-Shirt Sizing

The ARM templates defined here support three "t-shirt-sized" deployments. Each of these sizes defines properties such as the number of HDInsight master nodes, the size of VMs, the types of disks those VMs use etc. While it's possible to tweak each of these properties independently, these t-shirt sizes should give you some decent defaults.

The sizes are:

- Small (0): Useful for small teams, or individuals.
- Medium (1): A decent default for most installs that need so support a team of researchers
- Large (2): A configurartion that should support a large organisation

Here are the properties that are defined for each t-shirt size:

| Component   | Property            | 1 (Small)        | 2 (Medium) | 3 (Large)  |
| ----------- | ------------------- | ------------ | ------ | ------ |
| Avere       |
|             | use-azure           | FALSE        | TRUE   | TRUE   |
|             | node-quantity       | n/a          | 3      | 12     |
|             | ssd-per-node        | n/a          | 4      | 4      |
|             | node-cache-size     | n/a          | 1024GB | 4096TB |
|             | node-size           | n/a          | D16    | E32    |
|             |                     |              |
| Solr        |
|             | vm-size             | E4v3         | E8v3   | E16v3  |
|             | vm-quantity         | 1            | 2      | 4      |
|             | disk-type           | SSD          | SSD    | SSD    |
|             |                     |              |
| Batch       |
|             | node-size           | low-priority | F8v2   | F8v2   |
|             | max-nodes           | 5            | 16     | 1000   |
|             | jobs-per-node       | 1            | 1      | 1      |
|             | disk-type           | SSD          | SSD    | SSD    |
|             |                     |              |
| MongoDB     |
|             | node-quanity        | 1            | 3      | 5      |
|             | node-size           | D2v2         | E8v3   | E16v3  |
|             | disk-type           | E10          | P20    | P20    |
|             |                     |              |
| HDInsights  |
|             | head-node-quanity   | 1            | 2      | 2      |
|             | head-node-size      | D4v2         | D4v2   | D4v2   |
|             | worker-node-quanity | 2            | 20     | 50     |
|             | worker-node-size    | D3v2         | D5v2   | D14v2  |
|             |                     |              |
| Daemon      |
|             | node-size           | DS2v2        | F8v2   | F8v2   |
|             | disk-type           | HDD          | HDD    | HDD    |
|             |                     |              |        |
| Web Servers |
|             | node-quantity       | 1            | 2      | 4      |
|             | node-size           | D2sv3        | D4sv3  | D4sv3  |
|             | disk-type           | HDD          | HDD    | HDD    |

Additionally you can deploy a custom size by specifying the `customDeploymentSize` field and setting `deploymentSize=0`. The object has to contain all required fields. For an example see below. 

```json
        "deploymentSize": {
            "value": 0
        },
        "customDeploymentSize": {
            "value": {
                "type": "0 = CustomSize",
                "avereEnabled": true,
                "avere": {
                    "nodeCount": 3,
                    "diskSize": 1024,
                    "nodeSize": "Standard_D16s_v3"
                },
                "azureFilesEnabled": false,
                "solr": {
                    "ha": false,
                    "nodeSize": "Standard_E4_v3",
                    "nodeCount": 1
                },
                "batch": {
                    "maxNodeCount": 6,
                    "nodeSize": "Standard_F8s_v2"
                },
                "mongo": {
                    "nodeCount": 1,
                    "nodeSize": "Standard_D4_v2",
                    "diskType": "E10"
                },
                "hdInsight": {
                    "head": {
                        "nodeCount": 2,
                        "nodeSize": "Standard_D4_v2"
                    },
                    "worker": {
                        "nodeCount": 2,
                        "nodeSize": "Standard_D14_v2"
                    }
                },
                "daemon": {
                    "nodeSize": "Standard_DS2_v2"
                },
                "webServers": {
                    "minNodeCount": 1,
                    "maxNodeCount": 2,
                    "nodeSize": "Standard_DS2_v2"
                }
            }
        }
```

## Additional Notes

### Avere - First Run

To run Avere you must first accept the legal terms of the license using the following command `az vm image accept-terms --urn microsoft-avere:vfxt:avere-vfxt-controller:latest`

See [the Avere docs for more details](https://docs.microsoft.com/en-us/azure/avere-vfxt/avere-vfxt-prereqs#accept-software-terms-in-advance)

