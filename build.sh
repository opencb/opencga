#!/bin/bash
function yellow (){
   echo "$(tput setaf 3)$*$(tput sgr0)"
}
function green (){
   echo "$(tput setaf 2)$*$(tput sgr0)"
}
function cyan (){
   echo "$(tput setaf 6)$*$(tput sgr0)"
}
function red(){
   echo "$(tput setaf 1)$*$(tput sgr0)"
}



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
STORAGE_HADOOP_DEPS="hdp3.1"

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
    red "Unknown option $key"
    printUsage
    exit 1
    ;;
  esac
done


cd "$(dirname "$0")" || exit 2

if [ -d "$OPENCGA_HOME_DIR" ]; then

  OPENCGA_DENDENCY_VERSION="$(mvn help:evaluate -Dexpression=opencga.version -q -DforceStdout)"
  cd "$OPENCGA_HOME_DIR" || exit 2
  OPENCGA_CURRENT_VERSION="$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)"

  if [ "$OPENCGA_DENDENCY_VERSION" == "$OPENCGA_CURRENT_VERSION" ]; then
    green "Compiling opencga..."
    if ! (mvn enforcer:enforce -Denforcer.rules=requireProfileIdsExist -P"$STORAGE_HADOOP_DEPS" -pl :opencga) ; then
      red OpenCGA storage hadoop "$STORAGE_HADOOP_DEPS" not found!
      exit 1
    fi

    mvn clean install -DskipTests -Pstorage-hadoop -P"$STORAGE_HADOOP_DEPS" -T 2
    # shellcheck disable=SC2181
    if [ $? -eq 0 ]; then
      green "Opencga compilation success!"
    else
      red "Opencga compilation ERROR"
      exit 1
    fi
  else
    red "Opencga version no match! You must checkout the $OPENCGA_DENDENCY_VERSION of opencga"
    exit 1
  fi
else
  red "ERROR OPENCGA HOME NOT FOUND!!!"
  yellow "You must create in the current directory a symbolic link to the directory where you have downloaded opencga and call it opencga-home"
  cyan "         ln -s /path/to/opencga $OPENCGA_HOME_DIR    "
  printUsage
  exit 1
fi

cd - || exit 2

mvn clean install -DskipTests -T 2 -Dopencga.build.dir="${OPENCGA_HOME_DIR}/build/" -Dopencga-storage-hadoop-deps.id="$STORAGE_HADOOP_DEPS"
