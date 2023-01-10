#!/bin/bash

function yellow (){
   echo "$(tput setaf 3)$1$(tput setaf 7)"
}
function green (){
   echo "$(tput setaf 2)$1$(tput setaf 7)"
}
function cyan (){
   echo "$(tput setaf 6)$1$(tput setaf 7)"
}

function printUsage(){
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

## Check if the repo status is clean.
function check_repo_clean() {
  GIT_STATUS=$(git status --short)
  if [ -n "$GIT_STATUS" ]; then
  	yellow "Repository is not clean:"
  	yellow "$GIT_STATUS"
    exit
  fi
}

## This function removes TASK-XXX- if exists, otherwise it adds it.
function toggle_version() {
  local BRANCH=$1
  if [[ "$POM_DEPENDENCY_VERSION" == *"$BRANCH"* ]]; then
    ## Remove TASK-XXX- from the current version
    ## Example:  remove 'TASK-1234-' from 2.6.0-TASK-1234-SNAPSHOT
    NEW_VERSION=${POM_DEPENDENCY_VERSION/"$BRANCH-"}
  else
    ## Add 'TASK-XXX-' to the current version
    ## Example:  2.6.0-SNAPSHOT  -->  2.6.0-TASK-1234-SNAPSHOT
    CLEAN_RELEASE_VERSION=$(echo "$POM_DEPENDENCY_VERSION" | cut -d "-" -f 1)
    TAG_VERSION=$(echo "$POM_DEPENDENCY_VERSION" | cut -d "-" -f 2)
    NEW_VERSION="$CLEAN_RELEASE_VERSION-$BRANCH-$TAG_VERSION"
  fi
}

## Change version in the dependency.
## Usage: update_dependency "$DEPENDENCY_REPO" "$NEW_VERSION" "$BRANCH_NAME"
function update_dependency() {
  ## Save current directory
  local pwd=$PWD
  cd "$1" || exit 2
  check_repo_clean
  git checkout "$3"
  ## Check branch exists
  local BRANCH=$(git branch --show-current)
  if [ "$BRANCH" != "$3" ]; then
    yellow "Branch '$3' does not exist"
    exit
  fi
  ## Rename and commit new version
  mvn versions:set -DnewVersion="$2" -DgenerateBackupPoms=false
  git commit -am "Update version to $2"
  ## Restore directory
  cd "$pwd" || exit 2
}

## At least one parameter is required.
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
    else
      shift
    fi
    shift # past argument
    ;;
  -b | --biodata)
     LIB="BIODATA"
     if [ -z "$DEPENDENCY_REPO" ]; then
       DEPENDENCY_REPO="../biodata"
     else
       shift
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
if [[ "$BRANCH_NAME" == "TASK-"* ]]; then
  check_repo_clean "$BRANCH_NAME"
else
   	yellow "[$BRANCH_NAME] The branch name must start with TASK-"
    yellow "$GIT_STATUS"
    exit
fi

function update_library(){
  local LIBRARY="$1"
    POM_DEPENDENCY_VERSION=$(grep -m 1 "$LIBRARY" pom.xml | cut -d ">" -f 2 | cut -d "<" -f 1)
    toggle_version "$BRANCH_NAME"
    update_dependency "$DEPENDENCY_REPO" "$NEW_VERSION" "$BRANCH_NAME"
    mvn versions:set-property -Dproperty=java-common-libs.version -DnewVersion="$NEW_VERSION" -DgenerateBackupPoms=false
    git commit -am "Update '$LIBRARY' dependency to $NEW_VERSION"
}


if [ "$LIB" = "JAVA_COMMONS_LIB" ];then
 update_library java-common-libs.version
fi
if [ "$LIB" = "BIODATA" ];then
   update_library biodata.version
fi

yellow "The new dependency version is $NEW_VERSION"
cd "$CURRENT_DIR" || exit 2
