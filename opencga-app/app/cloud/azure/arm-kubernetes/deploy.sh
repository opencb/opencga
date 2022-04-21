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
  echo "   * -s     --subscription            STRING      Subscription name or subscription id"
  echo "   * --af   --azure-file              FILE        Azure deploy parameters file [azuredeploy.parameters.private.json]"
  echo "   * --spf  --service-principal-file  FILE        Azure service principal deploy parameters file. Execute createsp.sh to obtain the service principal parameters"
  echo "            --skip-k8s-deployment     FLAG        Skip k8s deployment. Skip 'setup-k8s.sh' "
  echo "     -c     --k8s-context             STRING      Kubernetes context"
  echo "            --k8s-namespace           STRING      Kubernetes namespace"
  echo "     --hf   --helm-file               FILE        Helm values file. Used when calling to 'setup-k8s.sh' "
  echo "     -o     --outdir                  DIRECTORY   Output directory where to write the generated manifests. Default: \$PWD"
  echo "            --opencga-conf-dir        DIRECTORY   OpenCGA configuration folder. Default: build/conf/ "
  echo "            --keep-tmp-files          FLAG        Do not remove any temporary file generated in the outdir"
  echo "            --verbose                 FLAG        Verbose mode. Print debugging messages about the progress."
  echo "     -h     --help                    FLAG        Print this help"
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
    echo "Missing file ${key} : '${file}' : No such file or directory"
    printUsage
    exit 1
  fi
}

function requiredDirectory() {
  key=$1
  dir=$2
  if [ ! -d "${dir}" ]; then
    echo "Missing directory ${key} : '${dir}' : No such directory"
    printUsage
    exit 1
  fi
}
#subscriptionName
#azudeDeployParameters
#spAzudeDeployParameters
setupAksOpts=()
keepTmpFiles=false
outputDir="$(pwd)"

while [[ $# -gt 0 ]]; do
  key="$1"
  value="$2"
  case $key in
  -h | --help)
    printUsage
    exit 0
    ;;
  -s | --subscription)
    subscriptionName="$value"
    shift # past argument
    shift # past value
    ;;
  --af | --azure-file)
    azudeDeployParameters="$value"
    shift # past argument
    shift # past value
    ;;
  --spf | --service-principal-file)
    spAzudeDeployParameters="$value"
    shift # past argument
    shift # past value
    ;;
  --skip-k8s-deployment)
    setupAksOpts+=("$key")
    shift # past argument
    ;;
  --hf | --helm-file)
    requiredFile "$key" "$value"
    value=$(realpath "$value")
    setupAksOpts+=("$key" "$value")
    shift # past argument
    shift # past value
    ;;
  -c | --k8s-context)
    setupAksOpts+=("$key" "$value")
    shift # past argument
    shift # past value
    ;;
  --k8s-namespace)
    setupAksOpts+=("$key" "$value")
    shift # past argument
    shift # past value
    ;;
  --keep-tmp-files)
    keepTmpFiles=true
    setupAksOpts+=("$key")
    shift # past argument
    ;;
  -o | --outdir)
    requiredDirectory "$key" "$value"
    value=$(realpath "$value")
    outputDir=$value
    shift # past argument
    shift # past value
    ;;
  --opencga-conf-dir)
    requiredDirectory "$key" "$value"
    value=$(realpath "$value")
    setupAksOpts+=("$key" "$value")
    shift # past argument
    shift # past value
    ;;
  --verbose)
    setupAksOpts+=("$key")
    set -x
    shift # past argument
    ;;
  *) # unknown option
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

# Don't move the PWD until we found out the realpath. It could be a relative path.
cd "$(dirname "$0")"

outputDir="${outputDir}/deploy-$(date "+%Y%m%d%H%M%S")"
mkdir -p "$outputDir"

# Location parameter is mandatory in the parameters file.
location=$(jq -r '.parameters.location.value' "${azudeDeployParameters}")

# Set account subscription
az account set --subscription "${subscriptionName}"

# Run validation to get final parameters.
finalParameters=$(az deployment sub validate \
  --template-file <(grep -v "^//" azuredeploy.json | jq '{parameters:.parameters, "$schema":."$schema" , resources:[], contentVersion:.contentVersion}') \
  --location "${location}" \
  --parameters @"${azudeDeployParameters}" \
  --parameters @"${spAzudeDeployParameters}" \
  --parameters _artifactsLocation="_artifactsLocation" \
  --parameters _artifactsLocationSasToken="?_artifactsLocationSasToken" </dev/null | jq .properties.parameters)

if [ -z "${finalParameters}" ]; then
  echo "Error executing validation"
  exit 1
fi

deploymentOut="${outputDir}/deployment-outputs-$(date "+%Y%m%d%H%M%S").json"
rgName=$(jq -r '.rgPrefix.value' <<<${finalParameters})
clusterName=$(jq -r '.clusterName.value' <<<${finalParameters})
deployId=${clusterName}-$(date "+%Y-%m-%d-%H.%M.%S")-R${RANDOM}
storageNamePrefix=$(jq -r '.storageNamePrefix.value' <<<${finalParameters})
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
  --name "$storageAccountName"

connection=$(az storage account show-connection-string \
  --resource-group "${rgName}" \
  --name "$storageAccountName" \
  --query connectionString)

az storage container create \
  --name $templateContainer \
  --connection-string "$connection" \
  --public-access blob

artifactsBlobUpdate="${outputDir}/ARTIFACTS_BLOB_UPDATE"
# ensure folder exists and it's empty
mkdir -p "${artifactsBlobUpdate}"/foo
rm -rf "${artifactsBlobUpdate:?}"/*
# Delete folder at EXIT if required
if [ "$keepTmpFiles" == "false" ]; then
  trap 'rm -rf "${artifactsBlobUpdate:?}"' EXIT
fi
cp -r $(ls | grep -v "ARTIFACTS_BLOB_UPDATE\|parameters\|deployment-outputs") "${artifactsBlobUpdate}"

az storage blob upload-batch \
  --destination $templateContainer \
  --connection-string "$connection" \
  --source "$artifactsBlobUpdate" \
  --no-progress --overwrite

echo "Files uploaded"

expiretime=$(date -u -d '30 minutes' +%Y-%m-%dT%H:%MZ)
token=$(az storage container generate-sas --name $templateContainer --expiry $expiretime --permissions r --output tsv --connection-string $connection)
template_url="$(az storage blob url --container-name $templateContainer --name azuredeploy.json --output tsv --connection-string $connection)?$token"
blob_base_url="$(az storage account show -n $storageAccountName --query primaryEndpoints.blob)"
container_base_url=$(tr -d '"' <<< "${blob_base_url}")$templateContainer

echo "# Deploy infrastructure"
echo "az deployment sub create -n $deployId ... > ${deploymentOut} "

# deploy infra
az deployment sub create -n $deployId -l ${location} --template-uri $template_url \
  --parameters @"${azudeDeployParameters}" \
  --parameters @"${spAzudeDeployParameters}" \
  --parameters _artifactsLocation=$container_base_url \
  --parameters _artifactsLocationSasToken="?$token"

az deployment sub show -n $deployId -o json > "$deploymentOut"

function getOutput() {
  jq -r '.properties.outputs.'${1}'.value' ${deploymentOut}
}

function getParameter() {
  jq -r '.properties.parameters.'${1}'.value' ${deploymentOut}
}

# Enable HDInsight monitor
hdInsightClusterName=$(getParameter hdInsightClusterName)
hdInsightResourceGroup=$(getOutput hdInsightResourceGroup)
hdInsightMonitorEnabled=$(az hdinsight monitor show --name "$hdInsightClusterName" --resource-group "$hdInsightResourceGroup" --query clusterMonitoringEnabled)
if [ "$hdInsightMonitorEnabled" != "true" ]; then
  $(getOutput hdInsightEnableMonitor)
fi

echo "${PWD}/setup-aks.sh --subscription ${subscriptionName} --azure-output-file ${deploymentOut} --outdir $outputDir ${setupAksOpts[*]}" | tee -a "$outputDir/setup-aks-$(date "+%Y%m%d%H%M%S").sh"
./setup-aks.sh --subscription "${subscriptionName}" --azure-output-file "${deploymentOut}" --outdir "$outputDir" "${setupAksOpts[@]}"
