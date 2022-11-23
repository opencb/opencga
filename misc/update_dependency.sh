#!/bin/bash

function yellow () {
   echo "$(tput setaf 3)$1$(tput setaf 7)"
}
function green () {
   echo "$(tput setaf 2)$1$(tput setaf 7)"
}
function cyan () {
   echo "$(tput setaf 6)$1$(tput setaf 7)"
}

function printUsage() {
  echo ""
  yellow "Release an OpenCB project."
  echo ""
  echo "Usage:   $(basename $0) --biodata|-b|--java-common-libs|-j"
  echo ""
  cyan "Options:"
  green "     -j     --java-common-libs    STRING         Update java-common-libs dependency"
  green "     -b     --biodata             STRING         Update biodata dependency"
  echo ""

}


if [ -z "$1" ]; then
  printUsage
  exit 1
fi


while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
  -h | --help)
    printUsage
    exit 0
    ;;
  -j | --java-common-libs)
    LIB="JAVA_COMMONS_LIB"
    shift # past argument
    ;;
  -b | --biodata)
     LIB="BIODATA"
    shift # past argument
    ;;
  *) # unknown option
    echo "Unknown option $key"
    printUsage
    exit 1
    ;;
  esac
done

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CURRENT_DIR=$PWD
cd $SCRIPT_DIR
cd ..
if [ "$LIB" = "JAVA_COMMONS_LIB" ];then
  CURRENT_VERSION=$(grep -m 1 java-common-libs.version pom.xml | cut -d ">" -f 2 | cut -d "<" -f 1)
fi
if [ "$LIB" = "BIODATA" ];then
  CURRENT_VERSION=$(grep -m 1 biodata.version pom.xml | cut -d ">" -f 2 | cut -d "<" -f 1)
fi
BRANCH_NAME=$(git branch --show-current)

if [[ "$CURRENT_VERSION" == *"$BRANCH_NAME"* ]]; then
  NEW_VERSION=${CURRENT_VERSION/"$BRANCH_NAME-"}
else
  CLEAN_RELEASE_VERSION=$(echo "$CURRENT_VERSION" | cut -d "-" -f 1)
  TAGS_VERSION=$(echo "$CURRENT_VERSION" | cut -d "-" -f 2)
  NEW_VERSION="$CLEAN_RELEASE_VERSION-$BRANCH_NAME-$TAGS_VERSION"
fi

if [ "$LIB" = "JAVA_COMMONS_LIB" ];then
  mvn versions:set-property -Dproperty=java-common-libs.version -DnewVersion="$NEW_VERSION" -DgenerateBackupPoms=false
fi
if [ "$LIB" = "BIODATA" ];then
  mvn versions:set-property -Dproperty=biodata.version -DnewVersion="$NEW_VERSION" -DgenerateBackupPoms=false
fi

yellow "The NEW_VERSION in pom.xml is $NEW_VERSION"
cd "$CURRENT_DIR" || exit 2
