#!/bin/sh

#LOG_LEVEL=INFO
#OPENCB_ONLY=FALSE
#COLOR=TRUE
OPENCGA_LOGS_OPTS=""
KUBECTL_LOG_OPTS=""
DEPLOYMENT=rest
CONTAINER=opencga

printUsage() {
  echo ""
  echo "Usage:   $(basename $0) [OPTION]..."
  echo ""
  echo "Options:"
  echo "            --context         STRING    Kubernetes context to use"
  echo "     -d     --deployment      STRING    Deployment name"
  echo "     -c     --container       STRING    Print logs from this container"
  echo "            --all-containers            Get all containers' logs in the pod(s)"
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
    return 0
    ;;
    --verbose)
    set -x
    shift # past argument
    ;;
    --context)
    CONTEXT="--context ${value}"
    shift # past argument
    shift # past value
    ;;
    --all-containers)
    KUBECTL_LOG_OPTS="${KUBECTL_LOG_OPTS} --all-containers"
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

cd "$(dirname "$0")" || (echo "ERROR MOVING" && exit)

# See https://stackoverflow.com/a/22644006/2073398
trap "exit" INT TERM
trap "kill 0" EXIT

for pod in $(kubectl get pods --selector=app=${DEPLOYMENT} $CONTEXT -o name) ; do
    kubectl logs $CONTEXT -c ${CONTAINER} ${KUBECTL_LOG_OPTS} "${pod}" | ./opencga-logs.sh --prefix "${pod}" ${OPENCGA_LOGS_OPTS} - &
done

wait