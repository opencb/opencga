# ARM Templates and Deploying OpenCGA to Azure with Kubernetes

This document contains information related to the deployment of OpenCGA to Azure using ARM automation scripts.

## Deploy to Azure

### With `az cli`

1. Clone the repository and move into the `ARM Kubernetes` directory with `cd ./opencga-app/app/scripts/azure/arm-kubernetes`.
2. Using your editor fill in the `azuredeploy.parameters.json` with the required parameters
3. Create  service principal for Azure Kubernetes Service (AKS) by running `./createsp.sh`
4. Deploy Open CGA using the command `./deploy.sh <subscription_name> <artifact_storage_rg> <artifact_storage_name> <azure_region> $aksServicePrincipalAppId $aksServicePrincipalClientSecret $aksServicePrincipalObjectId` . The arguments `<artifact_storage_rg>` and `<artifact_storage_name>` specify the location for artifacts that are needed for deployment to be stored.

## How do I connect to the Open CGA master pod

1. Run `kubectl get pods` and record the name of the master pod.
2. Run `kubectl exec -it <master_pod_name> sh`, and you shoudl get a prompt within the master pod.
3. Then [follow the Testing guide here to](../README.md), from the step `Create a new user` - you do not need to use sudo within the container.

## What is deployed

The automated OpenCGA deployment uses [docker images built here](../../docker/README.md) to setup and run Open CGA in Azure.

In a deployment the following components are deployed and configured

### Resource Group: `opencga`

This contains:

- VNET: This provides the virtual network on which the solution runs. It consists of multiple `subnets` on which different parts of the solution sit to allow simple management and configuration.
- Azure Kubernetes Service: Helm charts at `../../kuberneres/charts/`, and two agent pools, default agent pool for `master` and `rest` pods, and a jobs pool for running jobs with cluster-autoscaler [https://docs.microsoft.com/en-us/azure/aks/cluster-autoscaler](https://docs.microsoft.com/en-us/azure/aks/cluster-autoscaler) enabled.
- Azure Monitor and Solutions: A group of logging solutions to collect and aggregate logs from across the deployment
- Azure Storage: Azure storage with Azure Files enabled, with three shares, `conf`, `sessions` and  `varaints`. Please note Azure Files shares IOPS and pricing is defined by amount of storage provisioned, not used.

### Resource Group: `opencga-hdinsights`

This resource group contains the `HDInsights` cluster (Hosted `HBASE` on Hortonworks). This differs from a traditional `HBASE` cluster in that the data storage is detached from the nodes and stored in `Azure Data Lake` via `WebHDFS`.

### Resource Group: `opencga-mongodb`

This resource group contains an HA mongo cluster which consists of several nodes joined into a replica set and issued with an SSL certificate from LetsEncrypt for use via SSL

### Resource Group: `opencga-solr`

This resource group contains an HA SOLR cluster used by OpenCGA for indexing the data in `HBASE`. To provide the leadership election and failover a small `zookeeper` HA deployment is also present in this resource group.

## Deployment Sizing

The ARM templates defined here support three "t-shirt-sized" deployments. Each of these sizes defines properties such as the number of HDInsight master nodes, the size of VMs, the types of disks those VMs use etc. While it's possible to tweak each of these properties independently, these t-shirt sizes should give you some decent defaults.

The sizes are:

- Small (1): Useful for small teams, or individuals.
- Medium (2): A decent default for most installs that need so support a team of researchers
- Large (3): A configurartion that should support a large organisation

Here are the properties that are defined for each t-shirt size:

| Component   | Property            | 1 (Small)        | 2 (Medium) | 3 (Large)  |
| ----------- | ------------------- | ------------ | ------ | ------ |
| AKS        |
|             | vm-size             | D2sv3        | D4sv3  | D4sv3  |
|             | vm-quantity         | 3            | 5      | 7      |
|             | maxJobsPoolAgents   | 5            | 20     | 250    |
|             | maxJobsPoolAgents   | D4sv3        | D4sv3  | D4sv3  |
|             |                     |              |
| Solr        |
|             | vm-size             | E4v3         | E8v3   | E16v3  |
|             | vm-quantity         | 1            | 2      | 4      |
|             | disk-type           | SSD          | SSD    | SSD    |
|             |                     |              |
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

Additionally you can deploy a custom size by specifying the `customDeploymentSize` field and setting `deploymentSize=0`. The object has to contain all required fields. For an example see below.

```json
        "deploymentSize": {
            "value": 0
        },
        "customDeploymentSize": {
            "value": {
                "type": "0 = CustomSize",
                "aks": {
                    "nodeSize": "Standard_D4_v2",
                    "nodeCount": 3,
                    "maxJobsPoolAgents": 16,
                    "aksJobsAgentVMSize": "Standard_D4s_v3"
                },
                "solr": {
                    "ha": false,
                    "nodeSize": "Standard_E4_v3",
                    "nodeCount": 1
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
                }
            }
        }
```

## Possible further work

- Redirect scratch storage to node temporary files - unsure if this will bring perf improvement
- Azure Bastion and configuration of internal/external access to rest API
- Kubernetes Network Policies and Azure Network Security Groups
- Resource lock for HD Insight storage
- Optimal job pool node and job configuration resource request/limits
- Virtual Node - Already implemented in ARM templates. Not currently avaiable in all regions and still in preview.
