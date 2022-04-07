#!/bin/bash

function printUsage() {
  cat << EOF
Solr utils
Usage:   $(basename $0) [command] --host <context> [options]

Common options
    * -H     --host                STRING     Solr host url. If missing, will check var "SOLR_HOST". e.g. http://localhost:18983
             --dry-run             FLAG       Simulate execution.
             --verbose             FLAG       Verbose mode. Print debugging messages about the progress.
      -h     --help                FLAG       Print this help

Commands
  list - Fetch the names of the collections in the cluster.
             --filter              STRING     Regex to filter collections

  status - Request the status and response of an already submitted Asynchronous Collection API call.
      --id   --request-id          STRING     Request id

  backup - Backs up Solr collections and associated configurations to a shared filesystem - for example a Network File System.
    * -c     --collection          STRING     The name of the collection to be backed up.
      -n     --backup-name         STRING     Backup folder name at location. Default: {date}_{collection}
      -l     --location            DIRECTORY  Server side backup location. Default: /var/solr/data/backup/

  restore - Restores Solr indexes and associated configurations.
    * -c     --collection          STRING     The collection where the indexes will be restored into.
    * -n     --backup-name         STRING     The name of the existing backup that you want to restore.
      -l     --location            DIRECTORY  Server side backup location. Default: /var/solr/data/backup/
EOF
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


function solr-admin() {
  RESOURCE=${1}
  shift
  while [[ $# -gt 0 ]]; do
    key="$1"
    value="$2"
    ARGS="${ARGS}&$key=$value"
    shift
    shift
  done

  REQUEST="${SOLR_HOST}/solr/admin/${RESOURCE}?${ARGS}"
  if [ $DRY_RUN == "true" ]; then
    echo "${REQUEST}" 1>&2
  else
    curl "${REQUEST}" 2>/dev/null
  fi
}

function solr-admin-collections() {
  # shellcheck disable=SC2068
  solr-admin collections $@
}

function main() {
  COMMAND="${1:-help}"

  shift
  if [ "$COMMAND" == "help" ]; then
      printUsage
      exit 0
  fi

  DRY_RUN=false

  # Process common args
  declare -a args
  while [[ $# -gt 0 ]]; do
    key="$1"
    value="$2"
    case $key in
    -h | --help)
      printUsage
      exit 0
      ;;
    --verbose)
      set -x
      shift # past argument
      ;;
    --dry-run)
      DRY_RUN=true
      shift # past argument
      ;;
    -H | --host)
      SOLR_HOST="$value"
      shift # past argument
      shift # past value
      ;;
    *) # unknown command
      args+=("$key")
      shift # past one
      ;;
    esac
  done


  requiredParam "--host" "$SOLR_HOST"

  case "${COMMAND}" in
    list)
      list_collections ${args[@]}
      ;;
    backup)
      backup_collection ${args[@]}
      ;;
    restore)
      restore_collection ${args[@]}
      ;;
    status)
      request_status ${args[@]}
      ;;
    *) # unknown command
      echo "Unknown command $COMMAND"
      printUsage
      exit 1
      ;;
  esac
}



function list_collections() {
  COLL_FILTER=

  while [[ $# -gt 0 ]]; do
    key="$1"
    value="$2"
    case $key in
    --filter)
      COLL_FILTER="$value"
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

  solr-admin-collections action LIST | jq -r .collections[] | grep "${COLL_FILTER}"
}

function request_status() {
  REQUEST=

  while [[ $# -gt 0 ]]; do
    key="$1"
    value="$2"
    case $key in
    --id | --request-id)
      REQUEST="$value"
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

  requiredParam "--request-id" "$REQUEST"

  solr-admin-collections action REQUESTSTATUS requestid "$REQUEST" | jq .
}


function backup_collection() {
  COLLECTION=
  LOCATION=
  BACKUP_NAME=

  while [[ $# -gt 0 ]]; do
    key="$1"
    value="$2"
    case $key in
    -c | --collection)
      COLLECTION=$value
      shift
      shift
      ;;
    -n | --backup-name)
      BACKUP_NAME="$value"
      shift
      shift
      ;;
    -l | --location)
      LOCATION="$value"
      shift
      shift
      ;;
    *) # unknown option
      echo "Unknown option $key"
      printUsage
      exit 1
      ;;
    esac
  done

  requiredParam "--collection" "$COLLECTION"
  LOCATION=${LOCATION:-"/var/solr/data/backup/"}
  BACKUP_NAME=${BACKUP_NAME:-$(date "+%Y%m%d%H%M%S")_${COLLECTION}_backup}
  solr-admin-collections \
      action BACKUP \
      name "${BACKUP_NAME}" \
      collection "${COLLECTION}" \
      async "${BACKUP_NAME}" \
      location "${LOCATION}" | jq .
}

function restore_collection() {
  COLLECTION=
  LOCATION=
  BACKUP_NAME=

  while [[ $# -gt 0 ]]; do
    key="$1"
    value="$2"
    case $key in
    -c | --collection)
      COLLECTION=$value
      shift
      shift
      ;;
    -n | --backup-name)
      BACKUP_NAME=$value
      shift
      shift
      ;;
    -l | --location)
      LOCATION=$value
      shift
      shift
      ;;
    *) # unknown option
      echo "Unknown option $key"
      printUsage
      exit 1
      ;;
    esac
  done

  requiredParam "--collection" "$COLLECTION"
  requiredParam "--backup-name" "$BACKUP_NAME"
  LOCATION=${LOCATION:-"/var/solr/data/backup/"}
  solr-admin-collections \
      action RESTORE \
      name "${BACKUP_NAME}" \
      collection "${COLLECTION}" \
      async "${BACKUP_NAME}_restore" \
      location "${LOCATION}" | jq .
}

main $@