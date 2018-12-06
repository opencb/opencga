#!/bin/bash
cd $(dirname "$0")
az group deployment create -n vnet -g $RG --template-file azuredeploy.json 