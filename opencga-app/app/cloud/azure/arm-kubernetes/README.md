# ARM Templates and Deploying OpenCGA to Azure with Kubernetes

This document contains information related to the deployment of OpenCGA to Azure using ARM automation scripts.

## Deploy to Azure

### With `az cli`

1. Clone the repository and move into the `ARM Kubernetes` directory with `cd ./opencga-app/app/scripts/azure/arm-kubernetes`.
2. Using your editor fill in the `azuredeploy.parameters.json` with the required parameters
3. Create  service principal for Azure Kubernetes Service (AKS) by running `./createsp.sh`
4. Deploy Open CGA using the command `./deploy.sh <subscription_name> <artifact_storage_rg> <artifact_storage_name> <azure_region> $aksServicePrincipalAppId $aksServicePrincipalClientSecret $aksServicePrincipalObjectId` . The arguments `<artifact_storage_rg>` and `<artifact_storage_name>` specify the location for artifacts that are needed for deployment to be stored.


## Deploy without User Access Administrator role

To deploy Azure HDInsight and Azure Kubernetes Services, some "Role Assignment" operations are required. 

If the OpenCGA administrator does not have the role "[_User access administrator_](https://docs.microsoft.com/en-gb/azure/role-based-access-control/built-in-roles#user-access-administrator)", these operations will need to be executed separately from an account with these permissions.

### Required roles
**Azure Kubernetes Service:**
  - [Network Contributor Role](https://docs.microsoft.com/en-gb/azure/role-based-access-control/built-in-roles#network-contributor) for the main OpenCGA Vnet through a Service Principal 

**Azure HDInsight (Hadoop)**
 - [Storage Blob Data Owner Role](https://docs.microsoft.com/en-gb/azure/role-based-access-control/built-in-roles#storage-blob-data-owner) for the OpenCGA HDI storage account through a Managed Identity

OpenCGA ARM Templates for role assignment to Azure with Kubernetes are located in folder `roleAssignments/`

### Usage
This step should be executed before executing the general deployment.

```shell script
./createsp.sh <subscriptionName> <servicePrincipalName>
./role-assignments.sh <subscription_name> <resourceGroupPrefix>  <location> <servicePrincipalObjectId>
```

To skip role assignments within the main arm templates, add the parameter `"skipRoleAssignment": {"value": true}` to the `azuredeploy.private.parameters.json`
                
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
|             | vm-size             | D2sv3        | D4sv3  | D4sv3   |
|             | vm-quantity         | 3            | 5      | 7       |
|             | maxJobsPoolAgents   | 1            | 5      | 50      |
|             | maxJobsPoolAgents   | D8sv3        | D8sv3  | D16sv3  |
|             |                     |              |
| Solr        |
|             | vm-size             | E4v3         | E8v3   | E16v3  |
|             | vm-quantity         | 1            | 2      | 2      |
|             | disk-type           | StandardSSD_LRS          | StandardSSD_LRS    | Premium_LRS    |
|             |                     |              |
|             |                     |              |
| MongoDB     |
|             | node-quanity        | 1            | 3      | 5      |
|             | node-size           | D4sv3         | E8v3   | E16v3  |
|             | disk-type           | StandardSSD_LRS          | StandardSSD_LRS    | StandardSSD_LRS    |
|             | disk-size           | 512          | 1028    | 1028    |
|             |                     |              |
| HDInsights  |
|             | head-node-quanity   | 2            | 2      | 2      |
|             | head-node-size      | D4v2         | D4v2   | D4v2   |
|             | worker-node-quanity | 3            | 20     | 50     |
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
                    "nodeCount": 3,
                    "nodeSize": "Standard_D4s_v3",
                    "maxJobsPoolAgents": 5,
                    "aksJobsAgentVMSize": "Standard_D16s_v3"
                },
                "solr": {
                    "ha": true,
                    "nodeSize": "Standard_E8_v3",
                    "nodeCount": 2,
                    "diskType": "StandardSSD_LRS",
                    "diskSizeGB": 1028,
                    "zookeeper": {
                        "nodeSize": "Standard_D2_v2"
                    }
                },
                "mongo": {
                    "nodeCount": 3,
                    "nodeSize": "Standard_E8s_v3",
                    "diskType": "StandardSSD_LRS",
                    "diskSizeGB": 1028
                },
                "hdInsight": {
                    "head": {
                        "nodeCount": 2,
                        "nodeSize": "Standard_D4_v2"
                    },
                    "worker": {
                        "nodeCount": 20,
                        "nodeSize": "Standard_D5_v2"
                    },
                    "yarnSiteMemoryInMb": 14000
                }
            }
        }
```

## Network customization

The generated cluster may need to be attached via vnet peering to an already existing network. In this case, there might be a collision on the address space, therefore, the default CIDR should be modified.

You can deploy a custom network cidr by specifying the `networkCIDR` field in the properties. The object has to contain all required fields. For an example see below.

```
        "networkCIDR": {
            "type": "object",
            "metadata": {
                "description": "Object containing all the hardcoded IPs and CIDRs required for the deployment"
            },
            "defaultValue": {
                "vnet": {
                    "addressPrefixes": "10.0.0.0/16",
                    "subnets": {
                        "kubernetes": "10.0.0.0/22",
                        "aci": "10.0.4.0/22",
                        "hdinsight": "10.0.8.0/24",
                        "mongo": "10.0.9.0/24",
                        "solr": "10.0.10.0/24",
                        "login": "10.0.12.0/24"
                    }
                },
                "aks": {
                    "serviceCIDR": "10.0.100.0/24",
                    "dnsServiceIP" : "10.0.100.10",
                    "dockerBridgeCIDR" : "172.17.0.1/16"
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
