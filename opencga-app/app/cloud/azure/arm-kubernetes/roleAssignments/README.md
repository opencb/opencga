
# OpenCGA ARM Templates for role assignment to Azure with Kubernetes

To deploy Azure HDInsight and Azure Kubernetes Services, some "Role Assignment" operations are required. 

If the OpenCGA administrators don't have the role "[_User access administrator_](https://docs.microsoft.com/en-gb/azure/role-based-access-control/built-in-roles#user-access-administrator)", these operations will need to be executed separately from an account with enough permissions.

General deployment should be executed in two steps.

## Usage

```shell script
./createsp.sh <subscriptionName> <servicePrincipalName>
./roleAssignments/opencga_role_assignments.sh <subscription_name> <resourceGroupPrefix> <servicePrincipalObjectId>
```
