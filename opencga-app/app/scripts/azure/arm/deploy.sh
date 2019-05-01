#!/bin/bash
cd $(dirname "$0")

set -e

location='uksouth'
storageRg='<your_rg_name>'
storageAccountName='<your_account_name>'
templateContainer='templates'

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
    --public-access Off \
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
token=$(az storage container generate-sas --name $templateContainer --expiry $expiretime --permissions r --output tsv --connection-string $connection)
template_url="$(az storage blob url --container-name $templateContainer --name azuredeploy.json --output tsv --connection-string $connection)?$token"
blob_base_url="$(az storage account show -n mrtemplates2019 --query primaryEndpoints.blob)"
container_base_url=${blob_base_url//\"/}$templateContainer

az deployment create -n opencga-$RANDOM  -l uksouth --template-uri $template_url --parameters @azuredeploy.parameters.private.json  --parameters _artifactsLocation=$container_base_url   --parameters _artifactsLocationSasToken="?$token" 