#!/bin/bash
#
# Please be aware this script uploads artifacts to public blob storage with no SAS token. 
# 
# If the script is modifed to use a SAS token, be aware if the SAS token later changes then operations that depend on the storage and solution redeployment will fail.
# Given dependencies on the storage within OpenCGA a SAS token with a long lifetime needs to be created and used each time the solution is deployed.

set -e

function printUsage() {
  echo ""
  echo "Usage:   $(basename $0) --subscription <subscriotion_name> [options]"
  echo ""
  echo "Options:"
  echo "   * -s     --subscription              Subscription name or subscription id"
  echo "     --af   --azure-file                Azure deploy parameters file [azuredeploy.parameters.private.json]"
  echo "     --spf  --service-principal-file    Azure service principal deploy parameters file. Execute createsp.sh to obtain the service principal parameters"
  echo "     --hf   --helm-file                 Helm values file. Used when calling to 'setup-k8s.sh' "
  echo "     -h     --help                      Print this help"
  echo ""
}

function requiredParam() {
  key=$1
  value=$2
  if [ -z "${value}" ]; then
    echo "Missing param $key"
    printUsage
    exit 1
  fi
}

function requiredFile() {
  key=$1
  file=$2
  if [ ! -f "${file}" ]; then
    echo "Missing file ${key} : '${file}'"
    printUsage
    exit 1
  fi
}

#subscriptionName
#azudeDeployParameters
#spAzudeDeployParameters
#helmDeployParameters

while [[ $# -gt 0 ]]
do
key="$1"
value="$2"
case $key in
    -h|--help)
    printUsage
    exit 0
    ;;
    -s|--subscription)
    subscriptionName="$value"
    shift # past argument
    shift # past value
    ;;
    --af|--azure-file)
    azudeDeployParameters="$value"
    shift # past argument
    shift # past value
    ;;
    --spf|--service-principal-file)
    spAzudeDeployParameters="$value"
    shift # past argument
    shift # past value
    ;;
    --hf|--helm-file)
    helmDeployParameters="$value"
    shift # past argument
    shift # past value
    ;;
    *)    # unknown option
    echo "Unknown option $key"
    printUsage
    exit 1
    ;;
esac
done

azudeDeployParameters=${azudeDeployParameters:-azuredeploy.parameters.private.json}
spAzudeDeployParameters=${spAzudeDeployParameters:-$azudeDeployParameters}

requiredParam "--subscription" "${subscriptionName}"
requiredFile "--azure-file" "${azudeDeployParameters}"
requiredFile "--service-principal-file" "${spAzudeDeployParameters}"

azudeDeployParameters=$(realpath "${azudeDeployParameters}")
spAzudeDeployParameters=$(realpath "${spAzudeDeployParameters}")

if [ -n "$helmDeployParameters" ]; then
  requiredFile "--helm-file" "${helmDeployParameters}"
  helmDeployParameters=$(realpath "${helmDeployParameters}")
fi


# Don't move the PWD until we found out the realpath. It could be a relative path.
cd "$(dirname "$0")"

# Location parameter is mandatory in the parameters file.
location=$(jq -r '.parameters.location.value' "${azudeDeployParameters}")

# Set account subscription
az account set --subscription "${subscriptionName}"

# Run validation to get final parameters.
finalParameters=$(az deployment sub validate \
    --template-file <(jq '{parameters:.parameters, "$schema":."$schema" , resources:[], contentVersion:.contentVersion}' azuredeploy.json) \
    --location "${location}" \
    --parameters @"${azudeDeployParameters}"  \
    --parameters @"${spAzudeDeployParameters}"  \
    --parameters _artifactsLocation="_artifactsLocation"   \
    --parameters _artifactsLocationSasToken="?_artifactsLocationSasToken" < /dev/null | jq .properties.parameters)

if [ -z "${finalParameters}" ]; then
  echo "Error executing validation"
  exit 1
fi

deploymentOut="$(dirname "${azudeDeployParameters}")/deployment-outputs-$(date "+%Y%m%d%H%M%S").json"
rgName=$(jq -r '.rgPrefix.value' <<< ${finalParameters})
clusterName=$(jq -r '.clusterName.value' <<< ${finalParameters})
deployId=${clusterName}-$(date "+%Y-%m-%d-%H.%M.%S")-R${RANDOM}
storageNamePrefix=$(jq -r '.storageNamePrefix.value' <<< ${finalParameters})
storageAccountName=$(echo "${storageNamePrefix}artifacts" | tr '[:upper:]' '[:lower:]' | tr -d "_-" | cut -c 1-24)
templateContainer="templates"

az group create --name "${rgName}" --location "${location}"

echo "# Uploading file templates"

az storage account create \
    --resource-group "${rgName}" \
    --location "${location}" \
    --sku Standard_LRS \
    --https-only \
    --kind StorageV2 \
    --name $storageAccountName

connection=$(az storage account show-connection-string \
    --resource-group "${rgName}" \
    --name $storageAccountName \
    --query connectionString)

az storage container create \
    --name $templateContainer \
    --connection-string $connection \
    --public-access blob

mkdir -p ARTIFACTS_BLOB_UPDATE/foo
rm -rf ARTIFACTS_BLOB_UPDATE/*
cp -r $(ls | grep -v "ARTIFACTS_BLOB_UPDATE\|parameters\|deployment-outputs") ARTIFACTS_BLOB_UPDATE

az storage blob upload-batch \
    --destination $templateContainer \
    --connection-string $connection \
    --source ARTIFACTS_BLOB_UPDATE \
    --no-progress

rm -rf ARTIFACTS_BLOB_UPDATE

echo "Files uploaded"

expiretime=$(date -u -d '30 minutes' +%Y-%m-%dT%H:%MZ)
token=$(az storage container generate-sas --name $templateContainer --expiry $expiretime --permissions r --output tsv --connection-string $connection)
template_url="$(az storage blob url --container-name $templateContainer --name azuredeploy.json --output tsv --connection-string $connection)?$token"
blob_base_url="$(az storage account show -n $storageAccountName  --query primaryEndpoints.blob)"
container_base_url=${blob_base_url//\"/}$templateContainer

echo "# Deploy infrastructure"
echo "az deployment sub create -n $deployId ... > ${deploymentOut} "

# deploy infra
az deployment sub create -n $deployId  -l ${location} --template-uri $template_url \
    --parameters @"${azudeDeployParameters}"  \
    --parameters @"${spAzudeDeployParameters}"  \
    --parameters _artifactsLocation=$container_base_url   \
    --parameters _artifactsLocationSasToken="?$token" > ${deploymentOut}

function getOutput() {
  jq -r '.properties.outputs.'${1}'.value' ${deploymentOut}
}

# Enable HDInsight monitor
$(getOutput hdInsightEnableMonitor)

./setup-aks.sh --subscription "${subscriptionName}" --azure-output-file "${deploymentOut}" --helm-file "${helmDeployParameters}"