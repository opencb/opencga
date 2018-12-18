#!/bin/bash
cd $(dirname "$0")
az vm image accept-terms --urn microsoft-avere:vfxt:avere-vfxt-controller:latest
az deployment create -n opencga --template-file azuredeploy.json --parameters @azuredeploy.parameters.json    --parameters sshPassword=$PASSWORD --parameters clusterLoginPassword=$PASSWORD --parameters clusterName=$CLUSTERNAME --parameters storageOption=$STORAGE_OPTION --parameters _artifactsLocation=$ARTIFACTS_LOCATION --parameters rgPrefix=$RG --parameters  _artifactsLocationSasToken=?$RANDOM -l $LOCATION