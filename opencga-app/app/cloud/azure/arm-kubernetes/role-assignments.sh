#!/bin/bash

cd $(dirname "$0")

set -e


if [[ "$#" -ne 4 ]]; then
  echo "Usage: opencga_role_assignments.sh <subscription_name> <resourceGroupPrefix> <location> <servicePrincipalObjectId>"
  exit 1
fi

function requiredFile() {
  if [ ! -f $1 ]; then
    echo "Missing file $1"
    exit 1
  fi
}

requiredFile "roleAssignments/azuredeploy.hdi.json" || exit 1
requiredFile "roleAssignments/azuredeploy.aks.json" || exit 1

subscriptionName=$1
rgName=$2
location=$3
aksServicePrincipalObjectId=$4

hdiResourceGroup=${rgName}-hdinsight

set -x

az account set --subscription "${subscriptionName}"

## AKS
az group create --name "${rgName}" --location "${location}"

### Check VNET exists
if az network vnet show --name vnet --resource-group ${rgName} &> /dev/null; then
  echo "Vnet exists"
else
  requiredFile "vnet/azuredeploy.json" || exit 1
  az deployment group create -g ${rgName} -n ${rgName}-`date "+%Y-%m-%d-%H.%M.%S"`-R${RANDOM}  \
    --template-file vnet/azuredeploy.json
fi

az deployment group create -g ${rgName} -n ${rgName}-`date "+%Y-%m-%d-%H.%M.%S"`-R${RANDOM}  \
    --template-file roleAssignments/azuredeploy.aks.json  \
    --parameter aksServicePrincipalObjectId=${aksServicePrincipalObjectId}

## HDI
az group create --name "${hdiResourceGroup}" --location "${location}"

az deployment group create -g ${hdiResourceGroup} -n ${rgName}-`date "+%Y-%m-%d-%H.%M.%S"`-R${RANDOM}  \
    --template-file roleAssignments/azuredeploy.hdi.json

