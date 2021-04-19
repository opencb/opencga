#!/bin/bash

set -e

function printUsage() {
  echo ""
  echo "Usage:   $(basename $0) --subscription <subscriotion_name> [options]"
  echo ""
  echo "Options:"
  echo "   * -s     --subscription            STRING      Subscription name or subscription id"
  echo "     -c     --k8s-context             STRING      Kubernetes context"
  echo "            --k8s-namespace           STRING      Kubernetes namespace"
  echo "     --aof  --azure-output-file       FILE        Azure deployment output file"
  echo "     --hf   --helm-file               FILE        Helm values file. Used when calling to 'setup-k8s.sh' "
  echo "     -o     --outdir                  DIRECTORY   Output directory where to write the generated manifests. Default: \$PWD"
  echo "            --opencga-conf-dir        DIRECTORY   OpenCGA configuration folder. Default: build/conf/ "
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
#deploymentOut
#userHelmValues
#setupK8sOpts
#k8sContext
#k8sNamespace
#outputDir

while [[ $# -gt 0 ]]
do
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
  --aof | --azure-output-file)
    deploymentOut="$value"
    shift # past argument
    shift # past value
    ;;
  --hf | --helm-file)
    userHelmValues="$value"
    requiredFile "--helm-file" "${userHelmValues}"
    userHelmValues=$(realpath "${userHelmValues}")
    shift # past argument
    shift # past value
    ;;
  -c | --k8s-context)
    k8sContext="$value"
    shift # past argument
    shift # past value
    ;;
  --k8s-namespace)
    k8sNamespace="$value"
    shift # past argument
    shift # past value
    ;;
  -o | --outdir)
    requiredDirectory "$key" "$value"
    value=$(realpath "$value")
    setupK8sOpts="${setupK8sOpts} --outdir $value "
    outputDir=$value
    shift # past argument
    shift # past value
    ;;
  --opencga-conf-dir)
    requiredDirectory "$key" "$value"
    value=$(realpath "$value")
    setupK8sOpts="${setupK8sOpts} --opencga-conf-dir $value "
    shift # past argument
    shift # past value
    ;;
  --verbose)
    set -x
    setupK8sOpts="${setupK8sOpts} --verbose "
    shift # past argument
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

deploymentOut=$(realpath "${deploymentOut}")
outputDir=${outputDir:-$(dirname "${deploymentOut}")}
helmValues="${outputDir}/deployment-values-$(date "+%Y%m%d%H%M%S").yaml"

# Don't move the PWD until we found out the realpath. It could be a relative path.
cd "$(dirname "$0")"

function getOutput() {
  jq -r '.properties.outputs.'${1}'.value' "${deploymentOut}"
}

function getParameter() {
  jq -r '.properties.parameters.'${1}'.value' "${deploymentOut}"
}

clusterName="$(getParameter clusterName)"
k8sContext=${k8sContext:-$clusterName}
k8sNamespace=${k8sNamespace:-$k8sContext}

function configureContext() {
  az account set --subscription "${subscriptionName}"
  az aks get-credentials -n "$(getOutput "aksClusterName")" -g "$(getOutput "aksResourceGroupName")" --overwrite-existing --context "$k8sContext"
}

function generateHelmValuesFile() {
  if [ "$(getParameter "deploySolrAksPool")" == "true" ]; then
    solrAgentPool=solr
  else
    solrAgentPool=default
  fi
  if [ "$(getParameter "deployMongoDBAksPool")" == "true" ]; then
    mongodbAgentPool=mongodb
  else
    mongodbAgentPool=default
  fi
  ## Generate helm values file
  cat >> "${helmValues}" << EOF
# $(date "+%Y%m%d%H%M%S")
# Auto generated file from deployment output ${deploymentOut}

solr:
  external:
    hosts: "$(getOutput "solrHostsCSV")"
  deploy:
    enabled: "$(getParameter "deploySolr")"
    nodeSelector:
      agentpool: "$solrAgentPool"
    zookeeper:
      nodeSelector:
        agentpool: "$mongodbAgentPool"

mongodb:
  user: "$(getOutput "mongoDbUser")"
  password: "$(getOutput "mongoDbPassword")"
  external:
    hosts: "$(getOutput "mongoDbHostsCSV")"
  deploy:
    enabled: "$(getParameter "deployMongoDB")"
    nodeSelector:
      agentpool: "$mongodbAgentPool"

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

analysis:
  execution:
    options:
      k8s:
        masterNode: "https://$(getOutput "aksApiServerAddress"):443"

rest:
  ingress:
    hosts:
     - ""
     - "opencga.$(getOutput "privateDnsZonesName")"

iva:
  ingress:
    hosts:
     - ""
     - "opencga.$(getOutput "privateDnsZonesName")"

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
             --context "${k8sContext}" \
             --namespace "${k8sNamespace}" \
             -o "jsonpath={.status.loadBalancer.ingress[0].ip}" \
             opencga-nginx-ingress-nginx-controller)

  ACTUAL_IP=$(az network private-dns record-set a show \
             --subscription "${subscriptionName}" \
             --resource-group $(getOutput "aksResourceGroupName") \
             --zone-name $(getOutput "privateDnsZonesName")       \
             --name opencga 2> /dev/null | jq .aRecords[].ipv4Address -r)

  if [ "$ACTUAL_IP" != "" ] && [ "$ACTUAL_IP" != "$EXTERNAL_IP" ] ; then
    echo "Delete outdated A record: opencga.$(getOutput "privateDnsZonesName") : ${ACTUAL_IP}"
    az network private-dns record-set a delete              \
      --subscription "${subscriptionName}"                  \
      --resource-group $(getOutput "aksResourceGroupName")  \
      --zone-name "$(getOutput "privateDnsZonesName")"      \
      --name opencga
  fi

  if [ "$ACTUAL_IP" == "" ] || [ "$ACTUAL_IP" != "$EXTERNAL_IP" ] ; then
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

echo "$(realpath "$(pwd)/../../kubernetes/setup-k8s.sh") --context \"${k8sContext}\" --namespace \"${k8sNamespace}\" --values \"${allHelmValues}\" $setupK8sOpts" | tee -a "$outputDir/setup-k8s-$(date "+%Y%m%d%H%M%S").sh"
../../kubernetes/setup-k8s.sh --context "${k8sContext}" --namespace "${k8sNamespace}" --values "${allHelmValues}" $setupK8sOpts

echo "# Register Ingress domain name (if needed)"
registerIngressDomainName