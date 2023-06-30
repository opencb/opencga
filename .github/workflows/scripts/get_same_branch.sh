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
  if [ -d "./$REPO" ]; then
    cd "$REPO" || exit 2
    echo "Branch name $BRANCH_NAME already exists."
    mvn clean install -DskipTests
  else
    echo "$CURRENT Branch is NOT EQUALS $BRANCH_NAME "
  fi
}

install "java-common-libs"
install "biodata"
install "cellbase"
