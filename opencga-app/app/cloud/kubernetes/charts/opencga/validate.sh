#!/bin/bash


function printUsage() {
  echo ""
  echo "Validates opencga chart templates"
  echo ""
  echo "Usage:   $(basename $0) "
  echo ""
  echo "Options:"
  echo "     -f     --values              FILE       Helm values file"
  echo "     -h     --help                FLAG       Print this help"
  echo ""
}


while [[ $# -gt 0 ]]; do
  key="$1"
  value="$2"
  case $key in
  -h | --help)
    printUsage
    exit 0
    ;;
  -f | --values)
    VALUES=$(realpath "$value")
    shift # past argument
    shift # past value
    ;;
  *) # unknown option
    echo "Unknown option $key"
    printUsage
    exit 1
    ;;
  esac
done

cd "$(dirname "$0")"

set -x
if [[ -z "$VALUES" ]]; then
  helm template opencga .  --debug --dry-run \
    --set skipChartAppVersionCheck=true \
    --set azureStorageAccount.name=dummy-azurestorageaccount-name \
    --set azureStorageAccount.key=dummy-azurestorageaccount-key \
    --set mongodb.user=dummy-user \
    --set mongodb.password=dummy-password \
    --set mongodb.external.hosts=mongodb_hosts \
    --set solr.external.hosts=solr_hosts \
    --set analysis.execution.options.k8s.masterNode=k8s_masternode \
    --set hadoop.sshDns=dummy_hadoop_sshDns \
    --set hadoop.sshUsername=dummy_hadoop_sshUsername \
    --set hadoop.sshPassword=dummy_hadoop_sshPassword \
    --set opencga.admin.password=dummy_opencga_admin_password
else
  helm template opencga .  --debug --dry-run --values "$VALUES"
fi