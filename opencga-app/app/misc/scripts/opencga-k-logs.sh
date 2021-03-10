#!/bin/sh

#LOG_LEVEL=INFO
#OPENCB_ONLY=FALSE
#COLOR=TRUE
OPENCGA_LOGS_OPTS=""
KUBECTL_LOG_OPTS=""
KUBECTL_OPTS=""
DEPLOYMENT=rest
ALL_CONTAINERS=FALSE
CONTAINER=""

printUsage() {
  echo ""
  echo "Usage:   $(basename $0) [OPTION]..."
  echo ""
  echo "Options:"
  echo "            --context         STRING    Kubernetes context to use"
  echo "            --namespace       STRING    Kubernetes namespace"
  echo "     -d     --deployment      STRING    Deployment name"
  echo "     -c     --container       STRING    Print logs from this container"
  echo "     -A     --all-containers            Get all containers' logs in the pod(s)"
  echo "     -n     --tail            INT       Output the last NUM lines"
  echo "     -f     --follow                    Output appended data as the file grows"
  echo "     -l     --log-level       STRING    Log level filter. [debug, info, warn, error]. Default: info"
  echo "     --ocb  --opencb                    Output org.opencb.* loggers only"
  echo "            --color           BOOL      Use color output. Default: true"
  echo "     -h     --help                      Print this help"
  echo "            --verbose                   Verbose mode. Print debugging messages about the progress."
  echo ""
}

while [ $# -gt 0 ]
do
key="$1"
value="$2"
case $key in
    -h|--help)
    printUsage
    exit 0
    ;;
    --verbose)
    set -x
    OPENCGA_LOGS_OPTS="${OPENCGA_LOGS_OPTS} --verbose"
    shift # past argument
    ;;
    --context)
    KUBECTL_OPTS="${KUBECTL_OPTS} --context ${value} "
    shift # past argument
    shift # past value
    ;;
    -A|--all-containers)
    ALL_CONTAINERS="TRUE"
    shift # past argument
    ;;
    -c|--container)
    CONTAINER="${value}"
    shift # past argument
    shift # past value
    ;;
    -d|--deployment)
    DEPLOYMENT="${value}"
    shift # past argument
    shift # past value
    ;;
    --namespace)
    KUBECTL_OPTS="${KUBECTL_OPTS} --namespace ${value} "
    shift # past argument
    shift # past value
    ;;
    -n|--tail)
    KUBECTL_LOG_OPTS="${KUBECTL_LOG_OPTS} --tail ${value}"
    shift # past argument
    shift # past value
    ;;
    -f|--follow)
    KUBECTL_LOG_OPTS="${KUBECTL_LOG_OPTS} -f"
    shift # past argument
    ;;
    *)    # unknown option, assume opencga_logs
    OPENCGA_LOGS_OPTS="${OPENCGA_LOGS_OPTS} $key"
    shift # past argument
    ;;
esac
done

IS_MASTER_DEPLOYMENT=FALSE
IS_REST_DEPLOYMENT=FALSE
IS_IVA_DEPLOYMENT=FALSE
case "$DEPLOYMENT" in
  *master*)
    CONTAINER=${CONTAINER:-"$DEPLOYMENT"}
    IS_MASTER_DEPLOYMENT=TRUE
    ;;
  *rest*)
    CONTAINER=${CONTAINER:-"opencga"}
    IS_REST_DEPLOYMENT=TRUE
    ;;
  *iva*)
    CONTAINER=${CONTAINER:-"iva"}
    IS_IVA_DEPLOYMENT=TRUE
    ;;
  *)
    CONTAINER=${CONTAINER:-"opencga"}
    ;;
esac

if [ $ALL_CONTAINERS = "TRUE" ]; then
  KUBECTL_LOG_OPTS="${KUBECTL_LOG_OPTS} --all-containers"
else
  KUBECTL_LOG_OPTS="${KUBECTL_LOG_OPTS} --container ${CONTAINER}"
fi

cd "$(dirname "$0")" || (echo "ERROR MOVING" && exit)

# See https://stackoverflow.com/a/22644006/2073398
trap "exit" INT TERM
trap "kill 0" EXIT

PODS=$(kubectl get pods --selector=app=${DEPLOYMENT} ${KUBECTL_OPTS} -o name)
if [ -z "$PODS" ]; then
  if [ "$IS_MASTER_DEPLOYMENT" = TRUE ]; then
    PODS="deployment/$DEPLOYMENT"
  else
    echo "No pods found for deployment ${DEPLOYMENT}"
    exit 1;
  fi
fi

for pod in $PODS ; do
    kubectl logs ${KUBECTL_OPTS} ${KUBECTL_LOG_OPTS} "${pod}" | ./opencga-logs.sh --prefix "${pod}" ${OPENCGA_LOGS_OPTS} - &
done

wait