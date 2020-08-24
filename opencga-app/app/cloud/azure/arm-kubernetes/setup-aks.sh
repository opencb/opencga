#!/bin/bash

set -e

function requiredFile() {
  if [ ! -f $1 ]; then
    echo "Missing file $1"
    exit 1
  fi
}

if [[ "$#" -ne 2 ]]; then
  echo "Usage: $0 <subscription_name> <deployment-output.json>"
  exit 1
fi

subscriptionName=$1
deploymentOut=$2
requiredFile $deploymentOut
deploymentOut=$(realpath "${deploymentOut}")

# Don't move the PWD until we found out the realpath. It could be a relative path.
cd "$(dirname "$0")"


function getOutput() {
  jq -r '.properties.outputs.'${1}'.value' ${deploymentOut}
}

function getHelmParam() {
  # Commas must be scaped when passed as helm parameter
  getOutput ${1} | sed 's/,/\\,/g'
}


echo "# Deploy kubernetes"

az account set --subscription "${subscriptionName}"
# deploy opencga
az aks get-credentials -n "$(getOutput "aksClusterName")" -g "$(getOutput "aksResourceGroupName")" --overwrite-existing

K8S_NAMESPACE=$(getOutput "aksResourceGroupName")
# Create a namespace for opencga
if ! kubectl get namespace $K8S_NAMESPACE; then
    kubectl create namespace $K8S_NAMESPACE
fi

kubectl config set-context --current --namespace=$K8S_NAMESPACE

# Use Helm to deploy an NGINX ingress controller
## Deploy in the same namespace

helm repo add stable https://kubernetes-charts.storage.googleapis.com/
helm repo update

helm upgrade opencga-nginx stable/nginx-ingress \
    --namespace ${K8S_NAMESPACE} --version 1.27.0 \
    -f ../../kubernetes/charts/nginx/values.yaml \
    --install --wait --timeout 10m

## Register manually the nginx external IP for ingress
EXTERNAL_IP=$(kubectl get services opencga-nginx-nginx-ingress-controller -o "jsonpath={.status.loadBalancer.ingress[0].ip}")
ACTUAL_IP=$(az network private-dns record-set a show --resource-group $(getOutput "aksResourceGroupName") --zone-name $(getOutput "privateDnsZonesName") --name opencga 2> /dev/null | jq .aRecords[].ipv4Address -r)
if [ ! $ACTUAL_IP = "" ] && [ ! $ACTUAL_IP = $EXTERNAL_IP ] ; then
  echo "Delete outdated A record: opencga.$(getOutput "privateDnsZonesName") : ${ACTUAL_IP}"
  az network private-dns record-set a delete          \
    --resource-group $(getOutput "aksResourceGroupName")  \
    --zone-name "$(getOutput "privateDnsZonesName")"   \
    --name opencga
fi

if [ $ACTUAL_IP = "" ] || [ ! $ACTUAL_IP = $EXTERNAL_IP ] ; then
  echo "Create A record: opencga.$(getOutput "privateDnsZonesName") : ${EXTERNAL_IP}"
  az network private-dns record-set a add-record          \
    --resource-group $(getOutput "aksResourceGroupName")  \
    --zone-name "$(getOutput "privateDnsZonesName")"   \
    --record-set-name opencga                             \
    --ipv4-address ${EXTERNAL_IP}
fi

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
    --set opencga.host="http://opencga.$(getHelmParam "privateDnsZonesName")/opencga" \
    --set iva.ingress.host="opencga.$(getHelmParam "privateDnsZonesName")" \
    --install --wait -n $K8S_NAMESPACE --timeout 10m

