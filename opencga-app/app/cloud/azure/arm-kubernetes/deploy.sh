#!/bin/bash
#
# Please be aware this script uploads artifacts to public blob storage with no SAS token. 
# 
# If the script is modifed to use a SAS token, be aware if the SAS token later changes then operations that depend on the storage and solution redeployment will fail.
# Given dependencies on the storage within OpenCGA a SAS token with a long lifetime needs to be created and used each time the solution is deployed.

cd $(dirname "$0")

set -e


if [[ "$#" -ne 4 ]]; then
  echo "Usage: $0 <subscription_name> <aksServicePrincipalAppId> <aksServicePrincipalClientSecret> <aksServicePrincipalObjectId>"
  echo " * Execute createsp.sh to obtain the service principal values"
  exit 1
fi

subscriptionName=$1
aksServicePrincipalAppId=$2
aksServicePrincipalClientSecret=$3
aksServicePrincipalObjectId=$4

if [ ! -f azuredeploy.parameters.private.json ]; then
  echo "Missing file azuredeploy.parameters.private.json"
  exit 1
fi

templateContainer="templates"
location=$(cat azuredeploy.parameters.private.json | jq -r '.parameters.rgLocation.value')
rgName=$(cat azuredeploy.parameters.private.json | jq -r '.parameters.rgPrefix.value')
storageAccountName=`echo "${rgName,,}artifacts" | tr -d "_-"`
deployID=${rgName}-`date "+%Y-%m-%d-%H.%M.%S"`-R${RANDOM}

az account set --subscription "${subscriptionName}"
az group create --name "${rgName}" --location "${location}"

echo "# Uploading file templates"

az storage account create \
    --resource-group "${rgName}" \
    --location "${location}" \
    --sku Standard_LRS \
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
cp -r `ls | grep -v "ARTIFACTS_BLOB_UPDATE\|parameters\|deployment-outputs.json"` ARTIFACTS_BLOB_UPDATE

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
echo "az deployment sub create -n $deployID ... "
DEPLOYMENT_OUT=deployment-outputs.json
# deploy infra
az deployment sub create -n $deployID  -l ${location} --template-uri $template_url \
    --parameters @azuredeploy.parameters.private.json  \
    --parameters _artifactsLocation=$container_base_url   \
    --parameters _artifactsLocationSasToken="?$token"  \
    --parameters aksServicePrincipalAppId=$aksServicePrincipalAppId \
    --parameters aksServicePrincipalClientSecret=$aksServicePrincipalClientSecret \
    --parameters aksServicePrincipalObjectId=$aksServicePrincipalObjectId > ${DEPLOYMENT_OUT}


# Enable HDInsight monitor
`jq -r '.properties.outputs.hdInsightEnableMonitor.value' ${DEPLOYMENT_OUT}`

echo "# Deploy kubernetes"

# deploy opencga
az aks get-credentials -n $(jq -r '.properties.outputs.aksClusterName.value' ${DEPLOYMENT_OUT}) -g $(jq -r '.properties.outputs.aksResourceGroupName.value' ${DEPLOYMENT_OUT})

K8S_NAMESPACE=${rgName}
# Create a namespace for opencga
if ! kubectl get namespace $K8S_NAMESPACE; then
    kubectl create namespace $K8S_NAMESPACE
fi

kubectl config set-context --current --namespace=$K8S_NAMESPACE

# Use Helm to deploy an NGINX ingress controller
## Deploy in the same namespace

helm repo add stable https://kubernetes-charts.storage.googleapis.com/
helm repo update

helm install opencga-nginx stable/nginx-ingress \
    --namespace ${K8S_NAMESPACE} --version 1.27.0 \
    -f ../../kubernetes/charts/nginx/values.yaml \
     --wait --timeout 10m


if ! kubectl get secret azure-files-secret -n ${K8S_NAMESPACE}; then
   kubectl create secret generic azure-files-secret -n ${K8S_NAMESPACE} --from-literal=azurestorageaccountname=$(jq -r '.properties.outputs.storageAccountName.value' ${DEPLOYMENT_OUT}) --from-literal=azurestorageaccountkey=$(echo $deployment_details | jq -r '.properties.outputs.storageAccountKey.value')
fi


helm upgrade opencga ../../kubernetes/charts/opencga \
    --set init.catalogSecretKey=$(cat azuredeploy.parameters.private.json | jq -r '.parameters.catalogSecretKey.value') \
    --set openCGApassword=$(jq -r '.properties.outputs.openCgaAdminPassword.value' ${DEPLOYMENT_OUT}) \
    --set hadoop.sshDns=$(jq -r '.properties.outputs.hdInsightSshDns.value' ${DEPLOYMENT_OUT})  \
    --set hadoop.sshUsername=$(jq -r '.properties.outputs.hdInsightSshUsername.value' ${DEPLOYMENT_OUT}) \
    --set hadoop.sshPassword=$(jq -r '.properties.outputs.hdInsightSshPassword.value' ${DEPLOYMENT_OUT})  \
    --set catalog.database.hosts=$(jq -r '.properties.outputs.mongoDbHostsCSV.value' ${DEPLOYMENT_OUT})  \
    --set catalog.database.user=$(jq -r '.properties.outputs.mongoDbUser.value' ${DEPLOYMENT_OUT})  \
    --set catalog.database.password=$(jq -r '.properties.outputs.mongoDbPassword.value' ${DEPLOYMENT_OUT})   \
    --set solr.hosts=$(jq -r '.properties.outputs.solrHostsCSV.value' ${DEPLOYMENT_OUT}) \
    --set analysis.execution.options.k8s.masterNode=https://$(jq -r '.properties.outputs.aksApiServerAddress.value' ${DEPLOYMENT_OUT}):443 \
    --set analysis.execution.options.k8s.namespace=$K8S_NAMESPACE \
    --set analysis.index.variant.maxConcurrentJobs="100" \
    --install --wait -n $K8S_NAMESPACE --timeout 10m





