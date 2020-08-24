#!/bin/bash
cd $(dirname "$0")

set -e


if [[ "$#" -ne 2 ]]; then
  echo "Usage: $0 <subscriptionName> <servicePrincipalName>"

  if [ ! -f azuredeploy.parameters.private.json ]; then
    rgName="opencga"
  else
    rgName=$(cat azuredeploy.parameters.private.json | jq -r '.parameters.rgPrefix.value')
  fi
  echo " * Recommended servicePrincipalName: ${rgName}-aks"
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


cd -
out=azuredeploy.servicePrincipal.parameters.json
cat << EOF > $out
{
    "\$schema": "https://schema.management.azure.com/schemas/2015-01-01/deploymentParameters.json#",
    "contentVersion": "1.0.0.0",
    "parameters": {
        "aksServicePrincipalAppId": {
            "value": "$aksServicePrincipalAppId"
        },
        "aksServicePrincipalClientSecret": {
            "value": "$aksServicePrincipalClientSecret"
        },
        "aksServicePrincipalObjectId": {
            "value": "$aksServicePrincipalObjectId"
        }
    }
}
EOF

echo "----------------------------------------------------------------------"
echo "Service Principal AppId        : ${aksServicePrincipalAppId}"
echo "Service Principal ClientSecret : ${aksServicePrincipalClientSecret}"
echo "Service Principal ObjectId     : ${aksServicePrincipalObjectId}"
echo "----------------------------------------------------------------------"
echo ""
echo "To assign roles (to be run separately if the user does not have enough permissions):"
echo "   ./role-assignments.sh $subscriptionName  <rgName> <location>  $aksServicePrincipalObjectId"
echo "To deploy: "
echo "   ./deploy.sh $subscriptionName <main-azuredeploy-parameters-json> ${out}"

