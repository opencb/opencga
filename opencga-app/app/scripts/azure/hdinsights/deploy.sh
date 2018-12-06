#!/bin/bash
cd $(dirname "$0")
az group deployment create -g $RG --template-file azuredeploy.json --parameters @azuredeploy.parameters.json   --parameters sshPassword=$PASSWORD --parameters clusterLoginPassword=$PASSWORD --parameters clusterName=$CLUSTERNAME