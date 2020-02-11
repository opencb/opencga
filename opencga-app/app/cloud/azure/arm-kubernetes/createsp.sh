#!/bin/bash
cd $(dirname "$0")

set -e


if [[ "$#" -ne 2 ]]; then
  echo "Usage: createsp.sh <subscriptionName> <servicePrincipalName>"
  exit 1
fi


subscriptionName=$1
spname=$2
az account set --subscription $subscriptionName
spdetails=$(az ad sp create-for-rbac --years 5 -n $spname --skip-assignment)
sleep 10
aksServicePrincipalAppId=$(echo $spdetails | jq -r '.appId')
aksServicePrincipalClientSecret=$(echo $spdetails | jq -r '.password')
aksServicePrincipalObjectId=$(az ad sp show --id $aksServicePrincipalAppId --query "objectId" -o tsv)

echo "To deploy: ./deploy.sh $subscriptionName $aksServicePrincipalAppId $aksServicePrincipalClientSecret $aksServicePrincipalObjectId"
