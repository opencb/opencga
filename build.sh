#!/bin/bash

function printUsage() {
  echo ""
  echo "Build opencga-enterprise."
  echo ""
  echo "Usage:   $(basename $0)  [options]"
  echo ""
  echo "Options:"
  echo "     -o     --opencga-home        STRING         Opencga project repo directory. By default, ./opencga-home"
  echo "     -H     --storage-hadoop      STRING         Hadoop flavour. hdp3.1, emr6.1, ..."
  echo "            --verbose             FLAG           Print verbose logs"
  echo "            --help                FLAG           Print this help and exit"
}

OPENCGA_HOME_DIR="$PWD/opencga-home/"
STORAGE_HADOOP_DEPS="emr6.1"

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
  *) # unknown option
    echo "Unknown option $key"
    printUsage
    exit 1
    ;;
  esac
done

echo "OPENCGA_HOME_DIR= $OPENCGA_HOME_DIR"
echo "STORAGE_HADOOP_DEPS= $STORAGE_HADOOP_DEPS"
echo "PWD= $PWD"
echo "OPENCGA_ENTERPRISE_HOME_DIR= $OPENCGA_ENTERPRISE_HOME_DIR"

cd "$(dirname "$0")" || exit 2
OPENCGA_ENTERPRISE_HOME_DIR=$PWD

if [ -d "$OPENCGA_HOME_DIR" ]; then

  OPENCGA_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=opencga.version -q -DforceStdout)"
  cd "$OPENCGA_HOME_DIR" || exit 2
  OPENCGA_CURRENT_VERSION="$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)"

  echo "OPENCGA_DEPENDENCY_VERSION= $OPENCGA_DEPENDENCY_VERSION"
  echo "OPENCGA_CURRENT_VERSION= $OPENCGA_CURRENT_VERSION"

  if [ "$OPENCGA_DEPENDENCY_VERSION" == "$OPENCGA_CURRENT_VERSION" ]; then
    echo "Compiling opencga..."
    if ! (mvn enforcer:enforce -Denforcer.rules=requireProfileIdsExist -P"$STORAGE_HADOOP_DEPS" -pl :opencga) ; then
      echo OpenCGA storage hadoop "$STORAGE_HADOOP_DEPS" not found!
      exit 1
    fi

    #mvn clean install -DskipTests -Pstorage-hadoop -P"$STORAGE_HADOOP_DEPS" -T 2
    mvn clean install -DskipTests --no-transfer-progress -P storage-hadoop,"$STORAGE_HADOOP_DEPS",opencga-storage-hadoop-deps -Dopencga.war.name=opencga -Dcheckstyle.skip -pl ':opencga-app' --also-make -T 2 --no-transfer-progress
    # shellcheck disable=SC2181
    if [ $? -eq 0 ]; then
      echo "Opencga compilation success!"
    else
      echo "Opencga compilation ERROR"
      exit 1
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
    echo "Opencga version no match! You must checkout $REF_TYPE \"$REF\" to build from version \"$OPENCGA_DEPENDENCY_VERSION\" of opencga"
    echo "Please, execute bellow command and retry:"
    echo "  git -C \"$OPENCGA_HOME_DIR\" checkout $REF"
    exit 1
  fi
else
  echo "ERROR OPENCGA HOME NOT FOUND!!!"
  echo "You must create in the current directory a symbolic link to the directory where you have downloaded opencga and call it opencga-home"
  echo "         ln -s /path/to/opencga $OPENCGA_HOME_DIR    "
  printUsage
  exit 1
fi

cd "$OPENCGA_ENTERPRISE_HOME_DIR" || exit 2

mvn clean install -DskipTests --no-transfer-progress -T 2 -Dopencga.build.dir="${OPENCGA_HOME_DIR}/build/" -Dopencga-storage-hadoop-deps.id="$STORAGE_HADOOP_DEPS" -Dopencga.war.name=opencga
