#!/bin/bash

cd $(dirname "$0")

export RG=temp-opencga-arm
export SP_ID="d25b66ce-cb60-4d27-aeaa-e73df7611b33"
az group delete -n $RG
az group create -n $RG -l westeurope
az role assignment create --role "Owner" --assignee $SP_ID -g $RG
bash -f ./vnet/deploy.sh
bash -f ./avere/deploy.sh
notify-send -u critical "Finished with exit code: $?"

