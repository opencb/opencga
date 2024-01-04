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
  echo "     -t     --tags                STRING         Level of test we must to execute, the values could be LOW, MEDIUM or HIGH"
  echo "     -p     --publish             FLAG           Publish the results in a reports server."
  echo "     -b     --prepare_branches    FLAG           Previous to run tests, it will download and compile all branches of the dependencies."
  echo "     -s     --skip_tests          FLAG           Publish the results on a reports server without rerunning the test."
  echo "     -v     --verbose             FLAG           Print verbose logs"
  echo "     -h     --help                FLAG           Print this help and exit"
}


OPENCGA_HOME_DIR="$PWD/opencga-home/"
STORAGE_HADOOP_DEPS="hdp3.1"

while [[ $# -gt 0 ]]; do
  key="$1"
  value="$2"
  case $key in
  -h | --help)
    printUsage
    exit 0
    ;;
  -v | --verbose)
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
  -t | --tags )
      TAGS="$value"
      shift # past argument
      shift # past value
      ;;
  -p | --push )
      PUSH="true"
      shift # past argument
      shift # past value
      ;;
  -b | --prepare_branches )
      PREPARE_BRANCHES="true"
      shift # past argument
      shift # past value
      ;;
  -s | --skip_tests )
      SKIP_TESTS="true"
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


function calculate_branch(){
  CURRENT_BRANCH="$(git branch --show-current)"
  if [[ "$CURRENT_BRANCH" != "release"* ]];then
    echo "$CURRENT_BRANCH"
  else
    local VERSION=$(echo "$1" | cut -d "-" -f 1)
    local MAJOR=$(echo "$VERSION" | cut -d "." -f 1)
    local MINOR=$(echo "$VERSION" | cut -d "." -f 2)
    local PATCH=$(echo "$VERSION" | cut -d "." -f 3)
    local HOTFIX=$(echo "$VERSION" | cut -d "." -f 4)
    if [ -z "$HOTFIX" ]; then
      echo "release-$MAJOR.$MINOR.x"
    else
      echo "release-$MAJOR.$MINOR.$PATCH.x"
    fi
  fi
}

function install(){
  CURRENT_DIR=$PWD
  local REPO=$1
  local BRANCH_NAME="$(calculate_branch $2)"
  echo "Version of $REPO $2 should be in $BRANCH_NAME"
  cd /tmp/ || exit 2
  git clone https://github.com/opencb/"$REPO".git -b "$BRANCH_NAME"
  if [ -d "./$REPO" ]; then
    cd "$REPO" || exit 2
    echo "Branch name $BRANCH_NAME already exists."
    #mvn clean install -DskipTests
  else
    echo "$CURRENT Branch is NOT EQUALS $BRANCH_NAME "
  fi
  cd "$CURRENT_DIR" || exit
}

if [ $PREPARE_BRANCHES == "true" ]; then
  JCL_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=java-common-libs.version -q -DforceStdout)"
  install "java-common-libs" $JCL_DEPENDENCY_VERSION
  BIODATA_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=biodata.version -q -DforceStdout)"
  install "biodata" $BIODATA_DEPENDENCY_VERSION
  CELLBASE_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=cellbase.version -q -DforceStdout)"
  install "cellbase" $CELLBASE_DEPENDENCY_VERSION
  OPENCGA_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=opencga.version -q -DforceStdout)"
  install "opencga" $OPENCGA_DEPENDENCY_VERSION
fi


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

    mvn clean install -DskipTests -Pstorage-hadoop -P"$STORAGE_HADOOP_DEPS" -T 2
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

mvn clean install -DskipTests -T 2 -Dopencga.build.dir="${OPENCGA_HOME_DIR}/build/" -Dopencga-storage-hadoop-deps.id="$STORAGE_HADOOP_DEPS" -Dopencga.war.name=opencga
