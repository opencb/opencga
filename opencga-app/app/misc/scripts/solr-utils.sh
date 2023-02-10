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

  delete - Delete a collection
    * -c     --collection          STRING     The name of the collection to be deleted.

  status - Request the status and response of an already submitted Asynchronous Collection API call.
      --id   --request-id          STRING     Request id

  del-status - Delete the status of an already submitted Asynchronous Collection API call.
      --id   --request-id          STRING     Request id

  wait   - Waits for an asynchronous job to finish
      --id   --request-id          STRING     Request id
             --fail-on-missing     FLAG       Fail if the request is not found
             --sleep               NUMBER     Seconds to sleep between request status check

  backup - Backs up Solr collections and associated configurations to a shared filesystem - for example a Network File System.
    * -c     --collection          STRING     The name of the collection to be backed up.
      -n     --backup-name         STRING     Backup folder name at location. Default: {date}_{collection}
      -l     --location            DIRECTORY  Server side backup location. Default: /var/solr/data/backup/
             --request-id          STRING     User defined request id to identify this asynchronous request.
                                                By default, {backup-name}

  restore - Restores Solr indexes and associated configurations.
    * -c     --collection          STRING     The collection where the indexes will be restored into.
    * -n     --backup-name         STRING     The name of the existing backup that you want to restore.
      -l     --location            DIRECTORY  Server side backup location. Default: /var/solr/data/backup/
             --request-id          STRING     User defined request id to identify this asynchronous request.
                                                By default, {backup-name}_restore
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
  ARGS=""
  shift
  while [[ $# -gt 0 ]]; do
    key="$1"
    value="$2"
    ARGS="${ARGS}&$key=$value"
    shift
    shift
  done

  REQUEST="${SOLR_HOST}/solr/admin/${RESOURCE}?${ARGS}"
  if [ "$DRY_RUN" == "true" ]; then
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
  if ! [[ "$SOLR_HOST" =~ ^http.* ]]; then
    SOLR_HOST="http://${SOLR_HOST}"
  fi
  if ! [[ "$SOLR_HOST" =~ :[0-9]+$ ]]; then
    SOLR_HOST="${SOLR_HOST}:8983"
  fi

  is_alive

  case "${COMMAND}" in
    is-alive | isalive)
      # Alive status already checked
      exit 0
      ;;
    list)
      list_collections ${args[@]}
      ;;
    delete)
      delete_collection ${args[@]}
      ;;
    backup)
      backup_collection ${args[@]}
      ;;
    wait)
      wait_async_job ${args[@]}
      ;;
#    del-backup)
#      del_backup_collection ${args[@]}
#      ;;
    restore)
      restore_collection ${args[@]}
      ;;
    status)
      request_status ${args[@]}
      ;;
    del-status)
      del_status ${args[@]}
      ;;
    *) # unknown command
      echo "Unknown command $COMMAND"
      printUsage
      exit 1
      ;;
  esac
}

function is_alive() {
  SOLR_HOSTNAME=$(sed -E 's/(.*:\/\/)?([^:/?]*).*/\2/g' <<< "$SOLR_HOST")

  if getent hosts "$SOLR_HOSTNAME" >/dev/null ; then
      ## Check solr is available
      LIVE_NODES=$(solr-admin-collections action CLUSTERSTATUS | jq '.cluster.live_nodes | length')
      if [ -z "$LIVE_NODES" ] || [ "$LIVE_NODES" -eq 0 ]; then
        echo "Solr host '$SOLR_HOST' not available"
        exit 1
      fi
  else
    echo "Solr host $SOLR_HOSTNAME not found"
    exit 1;
  fi
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

function delete_collection() {
  COLLECTION=

  while [[ $# -gt 0 ]]; do
    key="$1"
    value="$2"
    case $key in
    --collection)
      COLLECTION="$value"
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

  solr-admin-collections action DELETE name "${COLLECTION:?}" | jq .
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

function wait_async_job() {
  REQUEST=
  FAIL_ON_MISSING="false"
  SLEEP=60

  while [[ $# -gt 0 ]]; do
    key="$1"
    value="$2"
    case $key in
    --id | --request-id)
      REQUEST="$value"
      shift # past argument
      shift # past value
      ;;
    --fail-on-missing)
      FAIL_ON_MISSING="true"
      shift # past argument
      ;;
    --sleep)
      SLEEP="$value"
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

  while true; do
    STATE=$(request_status --request-id "$REQUEST" | jq -r .status.state)
    case "$STATE" in
    completed)
        echo "Solr job completed: $REQUEST"
        return 0;
      ;;
    running|submitted)
        echo "Wait for '$REQUEST' ..."
        sleep "$SLEEP";
      ;;
    failed)
        echo "Error executing: $REQUEST"
        return 1;
      ;;
    notfound)
        if [[ "$FAIL_ON_MISSING" == "true" ]]; then
          echo "Error executing: $REQUEST"
          return 1;
        else
          echo "Ignoring request not found: $REQUEST"
          return 0;
        fi
      ;;
    esac
  done
}

function del_status() {
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

  solr-admin-collections action DELETESTATUS requestid "$REQUEST" | jq .
}


function backup_collection() {
  COLLECTION=
  LOCATION=
  BACKUP_NAME=
  REQUEST_ID=

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
    --request-id)
      REQUEST_ID=$value
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
      async "${REQUEST_ID:-$BACKUP_NAME}" \
      location "${LOCATION}" | jq .
}

function restore_collection() {
  COLLECTION=
  LOCATION=
  BACKUP_NAME=
  REQUEST_ID=

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
    --request-id)
      REQUEST_ID=$value
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
      async "${REQUEST_ID:-${BACKUP_NAME}_restore}" \
      location "${LOCATION}" | jq .
}

main $@