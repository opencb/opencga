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

function check_repo(){
  GIT_STATUS=$(git status --short)
  if [ -n "$GIT_STATUS" ]; then
  	echo "Repository is not clean:"
  	echo "$GIT_STATUS"
    exit
  else
    git pull
  fi
}

function get_new_version(){
  if [[ "$CURRENT_VERSION" == *"$1"* ]]; then
    NEW_VERSION=${CURRENT_VERSION/"$1-"}
  else
    CLEAN_RELEASE_VERSION=$(echo "$CURRENT_VERSION" | cut -d "-" -f 1)
    TAGS_VERSION=$(echo "$CURRENT_VERSION" | cut -d "-" -f 2)
    NEW_VERSION="$CLEAN_RELEASE_VERSION-$1-$TAGS_VERSION"
  fi
}

function update_dependency(){
  cd "$1" || exit 2
  check_repo
  git co "$3"
  check_repo
  mvn versions:set -DnewVersion="$2" -DgenerateBackupPoms=false
  git commit -am "Update version to $2"
}

if [ -z "$1" ]; then
  printUsage
  exit 1
fi

while [[ $# -gt 0 ]]; do
  key="$1"
  if [ -n "$2" ]; then
    DEPENDENCY_REPO="$2"
  fi
  case $key in
  -h | --help)
    printUsage
    exit 0
    ;;
  -j | --java-common-libs)
    LIB="JAVA_COMMONS_LIB"
    if [ -z "$DEPENDENCY_REPO" ]; then
      DEPENDENCY_REPO="../java-common-libs"
    fi
    shift # past argument
    ;;
  -b | --biodata)
     LIB="BIODATA"
     if [ -z "$DEPENDENCY_REPO" ]; then
       DEPENDENCY_REPO="../biodata"
     fi
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
cd "$SCRIPT_DIR" || exit 2
cd ..
BRANCH_NAME=$(git branch --show-current)
check_repo


if [ "$LIB" = "JAVA_COMMONS_LIB" ];then
  CURRENT_VERSION=$(grep -m 1 java-common-libs.version pom.xml | cut -d ">" -f 2 | cut -d "<" -f 1)
  get_new_version "$BRANCH_NAME"
  update_dependency "$DEPENDENCY_REPO" "$NEW_VERSION" "$BRANCH_NAME"
  cd "$SCRIPT_DIR" || exit 2
  cd ..
  mvn versions:set-property -Dproperty=java-common-libs.version -DnewVersion="$NEW_VERSION" -DgenerateBackupPoms=false
  git commit -am "Update java-common-libs dependency to $NEW_VERSION"
fi
if [ "$LIB" = "BIODATA" ];then
  CURRENT_VERSION=$(grep -m 1 biodata.version pom.xml | cut -d ">" -f 2 | cut -d "<" -f 1)
  get_new_version "$BRANCH_NAME"
  update_dependency "$DEPENDENCY_REPO" "$NEW_VERSION" "$BRANCH_NAME"
  cd "$SCRIPT_DIR" || exit 2
  cd ..
  mvn versions:set-property -Dproperty=biodata.version -DnewVersion="$NEW_VERSION" -DgenerateBackupPoms=false
  git commit -am "Update biodata dependency to $NEW_VERSION"
fi

yellow "The new dependency version is $NEW_VERSION"
cd "$CURRENT_DIR" || exit 2
