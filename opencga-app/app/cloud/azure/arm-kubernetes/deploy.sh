#!/bin/bash
#
# Please be aware this script uploads artifacts to public blob storage with no SAS token. 
# 
# If the script is modifed to use a SAS token, be aware if the SAS token later changes then operations that depend on the storage and solution redeployment will fail.
# Given dependencies on the storage within OpenCGA a SAS token with a long lifetime needs to be created and used each time the solution is deployed.

cd "$(dirname "$0")"

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
location=$(jq -r '.parameters.rgLocation.value' azuredeploy.parameters.private.json)
rgName=$(jq -r '.parameters.rgPrefix.value' azuredeploy.parameters.private.json)
storageAccountName=$(echo "${rgName}artifacts" | tr '[:upper:]' '[:lower:]' | tr -d "_-")
deployID=${rgName}-$(date "+%Y-%m-%d-%H.%M.%S")-R${RANDOM}

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
cp -r $(ls | grep -v "ARTIFACTS_BLOB_UPDATE\|parameters\|deployment-outputs.json") ARTIFACTS_BLOB_UPDATE

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


function getOutput() {
  jq -r '.properties.outputs.'${1}'.value' ${DEPLOYMENT_OUT}
}

# Enable HDInsight monitor
$(getOutput "hdInsightEnableMonitor")

echo "# Deploy kubernetes"

# deploy opencga
az aks get-credentials -n "$(getOutput "aksClusterName")" -g "$(getOutput "aksResourceGroupName")"

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

## Register manually the nginx external IP for ingress
EXTERNAL_IP=$(kubectl get services opencga-nginx-nginx-ingress-controller -o "jsonpath={.status.loadBalancer.ingress[0].ip}")
az network private-dns record-set a add-record          \
  --resource-group ${rgName}                            \
  --zone-name "$(getHelmParam "privateDnsZonesName")"   \
  --record-set-name opencga                             \
  --ipv4-address ${EXTERNAL_IP}

if ! kubectl get secret azure-files-secret -n ${K8S_NAMESPACE} &> /dev/null ; then
   kubectl create secret generic azure-files-secret -n ${K8S_NAMESPACE} \
       --from-literal=azurestorageaccountname=$(getOutput "storageAccountName") \
       --from-literal=azurestorageaccountkey=$(getOutput "storageAccountKey")
fi

if ! kubectl get secret opencga-secrets -n ${K8S_NAMESPACE} &> /dev/null ; then
   kubectl create secret generic opencga-secrets -n ${K8S_NAMESPACE} \
       --from-literal=openCgaAdminPassword=$(getOutput "openCgaAdminPassword") \
       --from-literal=hdInsightSshPassword=$(getOutput "hdInsightSshPassword") \
       --from-literal=mongoDbPassword=$(getOutput "mongoDbPassword")
fi

function getHelmParam() {
  # Commas must be scaped when passed as helm parameter
  getOutput ${1} | sed 's/,/\\,/g'
}

helm upgrade opencga ../../kubernetes/charts/opencga \
    --set hadoop.sshDns=$(getHelmParam "hdInsightSshDns")  \
    --set hadoop.sshUsername=$(getHelmParam "hdInsightSshUsername") \
    --set catalog.database.hosts=$(getHelmParam "mongoDbHostsCSV")  \
    --set catalog.database.user=$(getHelmParam "mongoDbUser")  \
    --set solr.hosts=$(getHelmParam "solrHostsCSV") \
    --set analysis.execution.options.k8s.masterNode=https://$(getHelmParam "aksApiServerAddress"):443 \
    --set analysis.execution.options.k8s.namespace=$K8S_NAMESPACE \
    --set analysis.index.variant.maxConcurrentJobs="100" \
    --set rest.ingress.host="opencga.$(getHelmParam "privateDnsZonesName")" \
    --install --wait -n $K8S_NAMESPACE --timeout 10m


helm upgrade iva ../../kubernetes/charts/iva \
    --set iva.opencga.host="http://opencga.$(getHelmParam "privateDnsZonesName")/opencga" \
    --set iva.ingress.host="opencga.$(getHelmParam "privateDnsZonesName")" \
    --install --wait -n $K8S_NAMESPACE --timeout 10m


