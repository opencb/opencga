#!/bin/bash
cd $(dirname "$0")
az group deployment create -n avere -g $RG --template-file azuredeploy.json --parameters @azuredeploy.parameters.private.json 