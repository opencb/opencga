#!/bin/bash

BRANCH_NAME=$1

if [[ -z $BRANCH_NAME  ]]; then
  echo "The first parameter is mandatory and must be a valid branch name."
  exit 1
fi

if [[ $BRANCH_NAME != "TASK-"*   ]]; then
  echo "No need to check dependencies."
  exit 0
fi

function install(){
  local REPO=$1
  cd /home/runner/work/ || exit 2
  git clone https://github.com/opencb/"$REPO".git -b "$BRANCH_NAME"
  cd "$REPO" || exit
  echo "Branch name $BRANCH_NAME already exists."
  mvn clean install -DskipTests
}

install "java-common-libs"
install "biodata"
install "cellbase"
