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
  yellow "Create Hotfix Branch."
  echo ""
  echo "Usage:   $(basename $0) [--repo|-r repopath] [--tag|-t tagversion]"
  echo ""
  cyan "Options:"
  green "     -r     --repo    STRING         Repo to create the new branch"
  green "     -t     --tag     STRING         Tag to create the branch from"
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


## At least one parameter is required.
if [ -z "$1" ]; then
  printUsage
  exit 1
fi

while [[ $# -gt 0 ]]; do
  key="$1"
  value="$2"
  case $key in
  -h | --help)
    printUsage
    exit 0
    ;;
  -r | --repo)
    REPO_DIR="$value"
    shift # past argument
    shift # past value
    ;;
  -t | --tag)
   TAG_VERSION="$value"
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

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CURRENT_DIR=$PWD
cd "$REPO_DIR" || exit 2

if [[ "$TAG_VERSION" == "v"* ]]; then
  git checkout "$TAG_VERSION"
  RELEASE_BRANCH="release-${TAG_VERSION//v}.x"
  git checkout -b "$RELEASE_BRANCH"
  POM_VERSION="${TAG_VERSION//v}.1-SNAPSHOT"
  mvn versions:set -DnewVersion="$POM_VERSION" -DgenerateBackupPoms=false
  git commit -am "Prepare new development branch $RELEASE_BRANCH"
  git push --set-upstream origin "$RELEASE_BRANCH"
else
  yellow "[$TAG_VERSION] The tag name must start with v"
  exit
fi

yellow "The new dependency version is $NEW_VERSION"
cd "$CURRENT_DIR" || exit 2
