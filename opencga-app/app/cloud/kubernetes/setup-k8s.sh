#!/bin/bash

set -e


function printUsage() {
  echo ""
  echo "Usage:   $(basename $0) --context <context> [options]"
  echo ""
  echo "Options:"
  echo "   * -c     --context                   Kubernetes context"
  echo "     -n     --namespace                 Kubernetes namespace"
  echo "   * -f     --values                    Helm values file"
  echo "     -h     --help                      Print this help"
  echo "            --verbose                   Verbose mode. Print debugging messages about the progress."
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

#K8S_CONTEXT
#K8S_NAMESPACE
#HELM_VALUES_FILE


while [[ $# -gt 0 ]]
do
key="$1"
value="$2"
case $key in
    -h|--help)
    printUsage
    exit 0
    ;;
    -c|--context)
    K8S_CONTEXT="$value"
    shift # past argument
    shift # past value
    ;;
    -n|--namespace)
    K8S_NAMESPACE="$value"
    shift # past argument
    shift # past value
    ;;
    -f|--values)
    HELM_VALUES_FILE="$value"
    shift # past argument
    shift # past value
    ;;
    --verbose)
    set -x
    shift # past argument
    ;;
    *)    # unknown option
    echo "Unknown option $key"
    printUsage
    exit 1
    ;;
esac
done

K8S_NAMESPACE="${K8S_NAMESPACE:-$K8S_CONTEXT}"
requiredParam "--context" "${K8S_CONTEXT}"
requiredParam "--values" "${HELM_VALUES_FILE}"

for f in $(echo "${HELM_VALUES_FILE}" | tr "," "\n"); do
  requiredFile "--values" "${f}"
  if [ -z "$REAL_HELM_VALUES_FILE" ]; then
    REAL_HELM_VALUES_FILE=$(realpath "${f}")
  else
    REAL_HELM_VALUES_FILE="${REAL_HELM_VALUES_FILE},$(realpath "${f}")"
  fi
done

HELM_VALUES_FILE="${REAL_HELM_VALUES_FILE}"

# Don't move the PWD until we found out the realpath. It could be a relative path.
cd "$(dirname "$0")"

function configureContext() {
  kubectl config use-context "$K8S_CONTEXT"

  # Create a namespace for opencga
  if ! kubectl get namespace "${K8S_NAMESPACE}"; then
      kubectl create namespace "${K8S_NAMESPACE}"
  fi

  kubectl config set-context "${K8S_CONTEXT}" --namespace="${K8S_NAMESPACE}"
}

function deployNginx() {
  # Use Helm to deploy an NGINX ingress controller
  ## Deploy in the same namespace

  helm repo add stable https://kubernetes-charts.storage.googleapis.com/
  helm repo update

  helm upgrade opencga-nginx stable/nginx-ingress \
      --kube-context "${K8S_CONTEXT}" --namespace "${K8S_NAMESPACE}" --version 1.27.0 \
      -f charts/nginx/values.yaml \
      --values "${HELM_VALUES_FILE}" \
      --install --wait --timeout 10m
}

function deployOpenCGA() {
  helm upgrade opencga charts/opencga \
      --values "${HELM_VALUES_FILE}" \
      --install --wait --kube-context "${K8S_CONTEXT}" -n "${K8S_NAMESPACE}" --timeout 10m
}

function deployIVA() {
  helm upgrade iva charts/iva \
      --values "${HELM_VALUES_FILE}" \
      --install --wait --kube-context "${K8S_CONTEXT}" -n "${K8S_NAMESPACE}" --timeout 10m
}


echo "# Deploy kubernetes"
echo "# Configuring context $K8S_CONTEXT"
configureContext

echo "# Deploy NGINX"
deployNginx

echo "# Deploy OpenCGA"
deployOpenCGA

echo "# Deploy IVA"
deployIVA