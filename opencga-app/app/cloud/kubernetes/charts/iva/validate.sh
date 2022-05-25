#!/bin/bash


function printUsage() {
  echo ""
  echo "Validates iva chart templates"
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
  helm template iva .  --debug --dry-run \
    --set skipChartAppVersionCheck=true
else
  helm template iva .  --debug --dry-run --values "$VALUES"
fi