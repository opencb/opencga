#!/bin/bash
cd $(dirname "$0")
#az vm image accept-terms --urn microsoft-avere:vfxt:avere-vfxt-controller:latest
az deployment create -n opencga$RANDOM --template-file azuredeploy.json --parameters @azuredeploy.parameters.json --parameters  _artifactsLocationSasToken=?$RANDOM -l uksouth
