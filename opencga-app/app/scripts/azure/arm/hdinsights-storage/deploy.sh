#!/bin/bash
cd $(dirname "$0")

az group deployment create -n storage -g $RG --template-file azuredeploy.json --parameters @azuredeploy.parameters.json   --parameters networkAclsVirtualNetworkRule=$SUBNET_ID --parameters storageAccountName=$STORAGE_ACCOUNT_NAME