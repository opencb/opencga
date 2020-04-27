#!/bin/bash -x

cd $(dirname "$0")

set -e


if [[ "$#" -ne 3 ]]; then
  echo "Usage: opencga_role_assignments.sh <subscription_name> <resourceGroupPrefix> <servicePrincipalObjectId>"
  exit 1
fi

subscriptionName=$1
rgName=$2
aksServicePrincipalObjectId=$3

hdiResourceGroup=${rgName}-hdinsight
aksResourceGroup=${rgName}

az account set --subscription "${subscriptionName}"


az deployment group create -g ${hdiResourceGroup} -n ${rgName}-`date "+%Y-%m-%d-%H.%M.%S"`-R${RANDOM}  \
    --template-file azuredeploy.hdi.json


az deployment group create -g ${aksResourceGroup} -n ${rgName}-`date "+%Y-%m-%d-%H.%M.%S"`-R${RANDOM}  \
    --template-file azuredeploy.aks.json  \
    --parameter aksServicePrincipalObjectId=${aksServicePrincipalObjectId}
