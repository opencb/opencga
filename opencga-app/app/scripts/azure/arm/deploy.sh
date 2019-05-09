#!/bin/bash
#
# Please be aware this script uploads artifacts to public blob storage with no SAS token. 
# 
# If the script is modifed to use a SAS token, be aware if the SAS token later changes then operations that depend on the storage and solution redeployment will fail.
# Given dependencies on the storage within OpenCGA a SAS token with a long lifetime needs to be created and used each time the solution is deployed.

cd $(dirname "$0")

set -e

if [[ "$#" -ne 3 ]]; then
  echo "Usage: deploy.sh <artifact_storage_rg> <artifact_storage_name> <azure_region>"
  exit 1
fi

storageRg=$1
storageAccountName=$2
location=$3
deployID=$RANDOM
templateContainer="templates"

az group create --name $storageRg --location $location

az storage account create \
    --resource-group $storageRg \
    --location $location \
    --sku Standard_LRS \
    --kind StorageV2 \
    --name $storageAccountName

connection=$(az storage account show-connection-string \
    --resource-group $storageRg \
    --name $storageAccountName \
    --query connectionString)

az storage container create \
    --name $templateContainer \
    --public-access container \
    --connection-string $connection

for filename in  ./*.json ./*.jsonc ./**/*.json ./**/*.jsonc ./**/*.sh ./**/*.py;  do
    [ -e "$filename" ] || continue
        echo "Uploading $filename..."
        az storage blob upload \
            --container-name $templateContainer \
            --file $filename \
            --name ${filename/'.\/'/} \
            --connection-string $connection \
            --no-progress &

done
wait
echo "Files uploaded"

expiretime=$(date -u -d '30 minutes' +%Y-%m-%dT%H:%MZ)
#token=$(az storage container generate-sas --name $templateContainer --expiry $expiretime --permissions r --output tsv --connection-string $connection)
template_url="$(az storage blob url --container-name $templateContainer --name azuredeploy.json --output tsv --connection-string $connection)?$token"
blob_base_url="$(az storage account show -n $storageAccountName  --query primaryEndpoints.blob)"
container_base_url=${blob_base_url//\"/}$templateContainer

az deployment create -n opencga-$deployID  -l uksouth --template-uri $template_url --parameters @azuredeploy.parameters.private.json  --parameters _artifactsLocation=$container_base_url   # --parameters _artifactsLocationSasToken="?$token" 