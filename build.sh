#!/bin/bash

set -e
set -o pipefail
set -o nounset

function printUsage() {
  echo ""
  echo "Build opencga-enterprise."
  echo ""
  echo "Usage:   $(basename $0)  [options]"
  echo ""
  echo "Options:"
  echo "     -o     --opencga-home        STRING         Opencga project repo directory. By default, ./opencga-home"
  echo "     -H     --storage-hadoop      STRING         Hadoop flavour. hdp3.1, hdi5.1, emr6.1, emr6.13 ..."
  echo "            --skip-opencga-build  FLAG           Assume opencga is already build in the correct version. Use with caution"
  echo "            --verbose             FLAG           Print verbose logs"
  echo "            --help                FLAG           Print this help and exit"
}

function log() {
    if [[ "$#" -gt 0 ]]; then
      echo "$@" | log
      return
    fi
    if [[ -n "$LOG_FILE" ]]; then
      tee -a $LOG_FILE
    else
      cat >&2
    fi
}

function error() {
  log "=========================="
  log "[ERROR] - " "$@"
  log "=========================="
}

OPENCGA_HOME_DIR="$PWD/opencga-home/"
STORAGE_HADOOP_DEPS="hdp3.1"
SKIP_OPENCGA_BUILD=false
LOG_FILE=""

while [[ $# -gt 0 ]]; do
  key="$1"
  value="${2:-}"
  case $key in
  -h | --help)
    printUsage
    exit 0
    ;;
  --verbose)
    set -x
    shift # past argument
    ;;
  -o | --opencga-home)
    OPENCGA_HOME_DIR=$(realpath "$value")
    shift # past argument
    shift # past value
    ;;
  -H | --storage-hadoop )
    STORAGE_HADOOP_DEPS="$value"
    shift # past argument
    shift # past value
    ;;
  --skip-opencga-build)
    SKIP_OPENCGA_BUILD=true
    shift # past argument
    ;;
  *) # unknown option
    echo "Unknown option $key"
    printUsage
    exit 1
    ;;
  esac
done


cd "$(dirname "$0")" || exit 2
OPENCGA_ENTERPRISE_HOME_DIR=$PWD

if [ -d "$OPENCGA_HOME_DIR" ]; then

  OPENCGA_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=opencga.version -q -DforceStdout)"
  cd "$OPENCGA_HOME_DIR" || exit 2
  OPENCGA_CURRENT_VERSION="$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)"

  log "OPENCGA_DEPENDENCY_VERSION= $OPENCGA_DEPENDENCY_VERSION"
  log "OPENCGA_CURRENT_VERSION= $OPENCGA_CURRENT_VERSION"

  if [ "$OPENCGA_DEPENDENCY_VERSION" == "$OPENCGA_CURRENT_VERSION" ]; then
    log "Compiling opencga..."
    if ! (mvn enforcer:enforce -Denforcer.rules=requireProfileIdsExist -P"$STORAGE_HADOOP_DEPS" -pl :opencga) ; then
      log OpenCGA storage hadoop "$STORAGE_HADOOP_DEPS" not found!
      exit 1
    fi

    if [ "$SKIP_OPENCGA_BUILD" = true ] ; then
      log "Skipping opencga build"
    else
      mvn clean install -DskipTests -P"$STORAGE_HADOOP_DEPS" -T 2 || (error "Opencga compilation ERROR" && exit 1)
    fi
  else
    cd "$OPENCGA_ENTERPRISE_HOME_DIR" || exit 2
    OPENCGA_EXPECTED_BRANCH="$(.github/workflows/scripts/opencga_branch.sh)"
    OPENCGA_EXPECTED_TAG="$(.github/workflows/scripts/opencga_branch.sh true)"

    REF_TYPE=
    REF=
    if git -C "$OPENCGA_HOME_DIR" tag --list  | grep "^${OPENCGA_EXPECTED_TAG}$" >/dev/null ; then
      REF_TYPE="tag"
      REF="$OPENCGA_EXPECTED_TAG"
    else
      REF_TYPE="branch"
      REF="$OPENCGA_EXPECTED_BRANCH"
    fi
    log "Opencga version no match! You must checkout $REF_TYPE \"$REF\" to build from version \"$OPENCGA_DEPENDENCY_VERSION\" of opencga"
    log "Please, execute bellow command and retry:"
    log "  git -C \"$OPENCGA_HOME_DIR\" checkout $REF"
    exit 1
  fi
else
  log "ERROR OPENCGA HOME NOT FOUND!!!"
  log "You must create in the current directory a symbolic link to the directory where you have downloaded opencga and call it opencga-home"
  log "         ln -s /path/to/opencga $OPENCGA_HOME_DIR    "
  printUsage
  exit 1
fi

cd "$OPENCGA_ENTERPRISE_HOME_DIR" || exit 2

mvn clean install -DskipTests -T 2 \
    -Dopencga.build.dir="${OPENCGA_HOME_DIR}/build/" \
    -Dopencga-hadoop-shaded.id="$STORAGE_HADOOP_DEPS" \
    -Dopencga.war.name=opencga || (error "Opencga enterprise compilation ERROR" && exit 1)
