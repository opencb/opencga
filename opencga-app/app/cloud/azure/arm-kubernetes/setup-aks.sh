#!/bin/bash

set -e

function printUsage() {
  echo ""
  echo "Usage:   $(basename $0) --subscription <subscriotion_name> [options]"
  echo ""
  echo "Options:"
  echo "   * -s     --subscription              Subscription name or subscription id"
  echo "     --aof  --azure-output-file         Azure deployment output file"
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
    echo "Missing file ${key} : '${file}' : No such file or directory"
    printUsage
    exit 1
  fi
}


#subscriptionName
#deploymentOut
#userHelmValues

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
    --aof|--azure-output-file)
    deploymentOut="$value"
    shift # past argument
    shift # past value
    ;;
    --hf|--helm-file)
    userHelmValues="$value"
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


requiredParam "--subscription" "$subscriptionName"
requiredFile "--azure-output-file" "$deploymentOut"

if [ -n "$userHelmValues" ]; then
  requiredFile "--helm-file" "${userHelmValues}"
  userHelmValues=$(realpath "${userHelmValues}")
fi

deploymentOut=$(realpath "${deploymentOut}")
helmValues="$(dirname "${deploymentOut}")/deployment-values-$(date "+%Y%m%d%H%M%S").yaml"

# Don't move the PWD until we found out the realpath. It could be a relative path.
cd "$(dirname "$0")"


function getOutput() {
  jq -r '.properties.outputs.'${1}'.value' ${deploymentOut}
}

function getParameter() {
  jq -r '.properties.parameters.'${1}'.value' ${deploymentOut}
}

function configureContext() {
  K8S_CONTEXT="$(getParameter clusterName)"

  az account set --subscription "${subscriptionName}"
  az aks get-credentials -n "$(getOutput "aksClusterName")" -g "$(getOutput "aksResourceGroupName")" --overwrite-existing --context "$K8S_CONTEXT"
}

function generateHelmValuesFile() {

  ## Generate helm values file
  cat >> "${helmValues}" << EOF
# $(date "+%Y%m%d%H%M%S")
# Auto generated file from deployment output ${deploymentOut}

solr:
  hosts: "$(getOutput "solrHostsCSV")"

hadoop:
  sshDns: "$(getOutput "hdInsightSshDns")"
  sshUsername: "$(getOutput "hdInsightSshUsername")"
  sshPassword: "$(getOutput "hdInsightSshPassword")"

azureStorageAccount:
  name: "$(getOutput "storageAccountName")"
  key: "$(getOutput "storageAccountKey")"

opencga:
  host: "http://opencga.$(getOutput "privateDnsZonesName")/opencga"
  admin:
    password: "$(getOutput "openCgaAdminPassword")"

catalog:
  database:
    hosts: "$(getOutput "mongoDbHostsCSV")"
    user: "$(getOutput "mongoDbUser")"
    password: "$(getOutput "mongoDbPassword")"

analysis:
  execution:
    options:
      k8s:
        masterNode: "https://$(getOutput "aksApiServerAddress"):443"
        namespace:  "$K8S_NAMESPACE"
  index:
    variant:
      maxConcurrentJobs: "100"

rest:
  ingress:
    host: "opencga.$(getOutput "privateDnsZonesName")"

iva:
  ingress:
    host: "opencga.$(getOutput "privateDnsZonesName")"

EOF

  if [ -n "${userHelmValues}" ]; then
    allHelmValues="${helmValues},${userHelmValues}"
  else
    allHelmValues="${helmValues}"
  fi

}

function registerIngressDomainName() {
  ## Register manually the nginx external IP for ingress
  EXTERNAL_IP=$(kubectl get services \
             --context "${K8S_CONTEXT}" \
             -o "jsonpath={.status.loadBalancer.ingress[0].ip}" \
             opencga-nginx-nginx-ingress-controller)

  ACTUAL_IP=$(az network private-dns record-set a show \
             --subscription "${subscriptionName}" \
             --resource-group $(getOutput "aksResourceGroupName") \
             --zone-name $(getOutput "privateDnsZonesName")       \
             --name opencga 2> /dev/null | jq .aRecords[].ipv4Address -r)

  if [ ! $ACTUAL_IP = "" ] && [ ! $ACTUAL_IP = $EXTERNAL_IP ] ; then
    echo "Delete outdated A record: opencga.$(getOutput "privateDnsZonesName") : ${ACTUAL_IP}"
    az network private-dns record-set a delete              \
      --subscription "${subscriptionName}"                  \
      --resource-group $(getOutput "aksResourceGroupName")  \
      --zone-name "$(getOutput "privateDnsZonesName")"      \
      --name opencga
  fi

  if [ $ACTUAL_IP = "" ] || [ ! $ACTUAL_IP = $EXTERNAL_IP ] ; then
    echo "Create A record: opencga.$(getOutput "privateDnsZonesName") : ${EXTERNAL_IP}"
    az network private-dns record-set a add-record          \
      --subscription "${subscriptionName}"                  \
      --resource-group $(getOutput "aksResourceGroupName")  \
      --zone-name "$(getOutput "privateDnsZonesName")"      \
      --record-set-name opencga                             \
      --ipv4-address ${EXTERNAL_IP}
  fi
}


echo "# Configure K8s context"
configureContext

echo "# Generate helm values file ${helmValues}"
generateHelmValuesFile

echo "setup-k8s.sh --context \"${K8S_CONTEXT}\" --values \"${allHelmValues}\""
../../kubernetes/setup-k8s.sh --context "${K8S_CONTEXT}" --values "${allHelmValues}"

echo "# Register Ingress domain name (if needed)"
registerIngressDomainName