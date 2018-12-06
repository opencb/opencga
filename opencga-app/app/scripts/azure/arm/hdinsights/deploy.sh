#!/bin/bash
cd $(dirname "$0")
az group deployment create -n hdinsight -g $RG --template-file azuredeploy.json --parameters @azuredeploy.parameters.json   --parameters sshPassword=$PASSWORD --parameters clusterLoginPassword=$PASSWORD --parameters clusterName=$CLUSTERNAME --parameters storageAccountName=$STORAGE_ACCOUNT_NAME