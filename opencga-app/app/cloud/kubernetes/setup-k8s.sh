#!/bin/bash

set -e

function printUsage() {
  echo ""
  echo "Deploy required Helm charts for a fully working OpenCGA installation"
  echo " - solr-operator"
  echo " - zk-operator"
  echo " - mongodb-operator"
  echo " - opencga-nginx"
  echo " - opencga"
  echo " - iva"
  echo ""
  echo "Usage:   $(basename $0) --context <context> [options]"
  echo ""
  echo "Options:"
  echo "   * -c     --context             STRING     Kubernetes context"
  echo "     -n     --namespace           STRING     Kubernetes namespace"
  echo "   * -f     --values              FILE       Helm values file"
  echo "     -o     --outdir              DIRECTORY  Output directory where to write the generated manifests. Default: \$PWD"
  echo "            --name-suffix         STRING     Helm deployment name suffix. e.g. '-test' : opencga-nginx-test, opencga-test, iva-test"
  echo "            --what                STRING     What to deploy. [nginx, iva, opencga, all]. Default: all"
  echo "            --opencga-conf-dir    DIRECTORY  OpenCGA configuration folder. Default: build/conf/ "
  echo "            --keep-tmp-files      FLAG       Do not remove any temporary file generated in the outdir"
  echo "            --dry-run             FLAG       Simulate an installation."
  echo "     -h     --help                FLAG       Print this help"
  echo "            --verbose             FLAG       Verbose mode. Print debugging messages about the progress."
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
    echo "Missing file ${key} : '${file}' : No such file"
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

#K8S_CONTEXT
#K8S_NAMESPACE
#HELM_VALUES_FILE
WHAT=ALL
DRY_RUN=false
#NAME_SUFFIX
HELM_OPTS="${HELM_OPTS} --debug "
OUTPUT_DIR="$(pwd)"
#OPENCGA_CONF_DIR
KEEP_TMP_FILES=false
DATE=$(date "+%Y%m%d%H%M%S")
#FILE_NAME_SUFFIX="-${DATE}"
FILE_NAME_SUFFIX=

while [[ $# -gt 0 ]]; do
  key="$1"
  value="$2"
  case $key in
  -h | --help)
    printUsage
    exit 0
    ;;
  -c | --context)
    K8S_CONTEXT="$value"
    shift # past argument
    shift # past value
    ;;
  -n | --namespace)
    K8S_NAMESPACE="$value"
    shift # past argument
    shift # past value
    ;;
  -f | --values)
    HELM_VALUES_FILE="$value"
    shift # past argument
    shift # past value
    ;;
  -o | --outdir)
    OUTPUT_DIR="$value"
    shift # past argument
    shift # past value
    ;;
  --what)
    WHAT="${value^^}" # Upper case
    shift             # past argument
    shift             # past value
    ;;
  --name-suffix)
    NAME_SUFFIX="${value}"
    shift # past argument
    shift # past value
    ;;
  --opencga-conf-dir)
    OPENCGA_CONF_DIR="${value}"
    shift # past argument
    shift # past value
    ;;
  --keep-tmp-files)
    KEEP_TMP_FILES=true
    shift # past argument
    ;;
  --verbose)
    set -x
    shift # past argument
    ;;
  --dry-run)
    DRY_RUN=true
    HELM_OPTS="${HELM_OPTS} --dry-run "
    shift # past argument
    ;;
  *) # unknown option
    echo "Unknown option $key"
    printUsage
    exit 1
    ;;
  esac
done

K8S_NAMESPACE="${K8S_NAMESPACE:-$K8S_CONTEXT}"
requiredParam "--context" "${K8S_CONTEXT}"
requiredParam "--values" "${HELM_VALUES_FILE}"
requiredDirectory "--outdir" "${OUTPUT_DIR}"

OUTPUT_DIR=$(realpath "${OUTPUT_DIR}")
if [ -n "${OPENCGA_CONF_DIR}" ]; then
  requiredDirectory "--opencga-conf-dir" "${OPENCGA_CONF_DIR}"
  OPENCGA_CONF_DIR=$(realpath "${OPENCGA_CONF_DIR}")
  CONF_DIR_FILTER="-not -iname '*.xml' -a -not -iname '*.yml' -a -not -iname '*.yaml' -a -not -iname '*.sh'"
  CONF_FILES=$(eval find "$OPENCGA_CONF_DIR" ${CONF_DIR_FILTER} | wc -l)
  if [ "$CONF_FILES" -gt 1 ]; then
    echo "Unexpected files in opencga configuration directory:"
    eval find "$OPENCGA_CONF_DIR" ${CONF_DIR_FILTER}
    exit 1
  fi
else
  OPENCGA_CONF_DIR=$(realpath "$(dirname "$0")/../../conf")
fi

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
  if ! kubectl get namespace "${K8S_NAMESPACE}" --context "$K8S_CONTEXT"; then
    kubectl create namespace "${K8S_NAMESPACE}" --context "$K8S_CONTEXT"
  fi

  kubectl config set-context "${K8S_CONTEXT}" --namespace="${K8S_NAMESPACE}"
}

function deployNginx() {
  # Use Helm to deploy an NGINX ingress controller
  ## Deploy in the same namespace
  ## https://docs.nginx.com/nginx-ingress-controller/installation/installation-with-helm/

#  helm repo add stable https://kubernetes-charts.storage.googleapis.com/
#  helm repo add nginx-stable https://helm.nginx.com/stable
  helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
  helm repo update

  NAME="opencga-nginx${NAME_SUFFIX}"
  echo "# Deploy NGINX ${NAME}"
  helm upgrade ${NAME} ingress-nginx/ingress-nginx \
    --kube-context "${K8S_CONTEXT}" --namespace "${K8S_NAMESPACE}" \
    -f charts/nginx/values.yaml \
    --values "${HELM_VALUES_FILE}" \
    --install --wait --timeout 10m ${HELM_OPTS}
}

function deployZkOperator() {
  # https://github.com/pravega/zookeeper-operator/tree/master/charts/zookeeper#installing-the-chart
  # Manual installation of ZK operator. Won't be needed when using solr-operator v0.3.0
  #  See https://github.com/apache/solr-operator/pull/231

  NAME="zk-operator"
  helm repo add pravega https://charts.pravega.io
  helm repo update

  helm upgrade ${NAME} pravega/zookeeper-operator \
    --kube-context "${K8S_CONTEXT}" --namespace "${K8S_NAMESPACE}" --version=v0.2.9 \
    --install --wait --timeout 10m ${HELM_OPTS}
}

function deploySolrOperator() {
  # Solr operator version 0.2.8 and below was developed by bloomberg.
  # Since version 0.3.0 (TBA), the operator was moved to apache fundation.
  #  See https://github.com/apache/solr-operator/issues/183

  # https://artifacthub.io/packages/helm/apache-solr/solr-operator
  # https://github.com/apache/solr-operator/tree/master/helm/solr-operator
  NAME="solr-operator"
  #helm repo add solr-operator https://bloomberg.github.io/solr-operator/charts
  helm repo add apache-solr https://solr.apache.org/charts

  SOLR_OPERATOR_VERSION="${SOLR_OPERATOR_VERSION:-v0.5.0}"

  helm upgrade ${NAME} apache-solr/solr-operator \
    --kube-context "${K8S_CONTEXT}" --namespace "${K8S_NAMESPACE}" --version "${SOLR_OPERATOR_VERSION}" \
    -f charts/solr-operator/values.yaml \
    --values "${HELM_VALUES_FILE}" \
    --install --wait --timeout 10m ${HELM_OPTS}
}

function deployCertManager() {
  NAME="cert-manager${NAME_SUFFIX}"

    # Add the Jetstack Helm repository
  helm repo add jetstack https://charts.jetstack.io

  # Update your local Helm chart repository cache
  helm repo update

  CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-1.6.1}"

  # Install the cert-manager Helm chart
  helm upgrade "${NAME}" jetstack/cert-manager \
    -f charts/cert-manager/values.yaml \
    --version "${CERT_MANAGER_VERSION}" \
    --values "${HELM_VALUES_FILE}" \
    --install --wait --kube-context "${K8S_CONTEXT}" -n "${K8S_NAMESPACE}" --timeout 10m ${HELM_OPTS}

  if [ $DRY_RUN == "false" ]; then
    helm get manifest "${NAME}" --kube-context "${K8S_CONTEXT}" -n "${K8S_NAMESPACE}" >"${OUTPUT_DIR}/helm-${NAME}-manifest${FILE_NAME_SUFFIX}.yaml"
  fi
}

function deployMongodbOperator() {
  NAME="mongodb-community-operator${NAME_SUFFIX}"
  helm repo add mongodb https://mongodb.github.io/helm-charts
  helm repo update
  MONGODB_OPERATOR_VERSION="${MONGODB_OPERATOR_VERSION:-v0.7.2}"

  helm upgrade "${NAME}" mongodb/community-operator \
    -f charts/mongodb-operator/values.yaml \
    --set "namespace=${K8S_NAMESPACE}" \
    --values "${HELM_VALUES_FILE}" \
    --install --wait --kube-context "${K8S_CONTEXT}" -n "${K8S_NAMESPACE}" --timeout 10m ${HELM_OPTS}

  if [ $DRY_RUN == "false" ]; then
    helm get manifest "${NAME}" --kube-context "${K8S_CONTEXT}" -n "${K8S_NAMESPACE}" >"${OUTPUT_DIR}/helm-${NAME}-manifest${FILE_NAME_SUFFIX}.yaml"
  fi
}

function deployOpenCGA() {
  CONF_MD5=
  if [[ -n "$OPENCGA_CONF_DIR" ]]; then
    find "$OPENCGA_CONF_DIR" -iname "*.xml" -o -iname "*.yml" -o -iname "*.yaml" -o -iname "*.sh"
    NAME="opencga${NAME_SUFFIX}"
    CONF_MD5=$(find "$OPENCGA_CONF_DIR" -type f -exec md5sum {} \; | sort -k 2 | md5sum | cut -d " " -f 1 )
    echo "# Deploy OpenCGA ${NAME}"
    OPENCGA_CHART="${OUTPUT_DIR:?}/${NAME}_${DATE}_tmp/"
    if [ $KEEP_TMP_FILES == "false" ]; then
      trap 'rm -rf "${OPENCGA_CHART:?}"' EXIT
    fi

    mkdir "$OPENCGA_CHART"
    mkdir "$OPENCGA_CHART"/conf
    cp -r charts/opencga/* "${OPENCGA_CHART:?}"
    cp "${OPENCGA_CONF_DIR:?}"/* "$OPENCGA_CHART"/conf
  else
    CONF_MD5="NA"
    OPENCGA_CHART=charts/opencga/
  fi

  helm upgrade "${NAME}" "${OPENCGA_CHART}" \
    --set "kubeContext=${K8S_CONTEXT}" \
    --set "confMd5=${CONF_MD5}" \
    --values "${HELM_VALUES_FILE}" \
    --install --wait --kube-context "${K8S_CONTEXT}" -n "${K8S_NAMESPACE}" --timeout 10m ${HELM_OPTS}
  if [ $DRY_RUN == "false" ]; then
    helm get manifest "${NAME}" --kube-context "${K8S_CONTEXT}" -n "${K8S_NAMESPACE}" >"${OUTPUT_DIR}/helm-${NAME}-manifest${FILE_NAME_SUFFIX}.yaml"
    helm get notes "${NAME}" --kube-context "${K8S_CONTEXT}" -n "${K8S_NAMESPACE}" >"${OUTPUT_DIR}/helm-${NAME}-notes${FILE_NAME_SUFFIX}.md"
  fi
}

function deployIVA() {
  NAME="iva${NAME_SUFFIX}"
  echo "# Deploy IVA ${NAME}"
  helm upgrade ${NAME} charts/iva \
    --values "${HELM_VALUES_FILE}" \
    --install --wait --kube-context "${K8S_CONTEXT}" -n "${K8S_NAMESPACE}" --timeout 10m ${HELM_OPTS}
  if [ $DRY_RUN == "false" ]; then
    helm get manifest "${NAME}" --kube-context "${K8S_CONTEXT}" -n "${K8S_NAMESPACE}" >"${OUTPUT_DIR}/helm-${NAME}-manifest${FILE_NAME_SUFFIX}.yaml"
  fi
}

echo "# Deploy kubernetes"
echo "# Configuring context $K8S_CONTEXT"
configureContext


if [[ "$WHAT" == "CERTMANAGER" || "$WHAT" == "ALL" ]]; then
  deployCertManager
fi

if [[ "$WHAT" == "NGINX" || "$WHAT" == "ALL" ]]; then
  deployNginx
fi

if [[ "$WHAT" == "SOLROPERATOR" || "$WHAT" == "ALL" ]]; then
  deploySolrOperator
fi

# Don't deploy zookeeper operator by default
if [[ "$WHAT" == "ZKOPERATOR" ]]; then
  deployZkOperator
fi

if [[ "$WHAT" == "MONGODBOPERATOR" || "$WHAT" == "ALL" ]]; then
  deployMongodbOperator
fi

if [[ "$WHAT" == "OPENCGA" || "$WHAT" == "ALL" ]]; then
  deployOpenCGA
fi

if [[ "$WHAT" == "IVA" || "$WHAT" == "ALL" ]]; then
  deployIVA
fi
