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
  #git clone git@github.com:opencb/"$REPO".git
  git clone https://github.com/opencb/"$REPO".git
  cd "$REPO" || exit 2
  git checkout "$BRANCH_NAME"
  CURRENT=$(git branch --show-current)
  echo "Current branch name $CURRENT"
  if [[ "$CURRENT" == "$BRANCH_NAME"  ]]; then
    echo "Branch name $BRANCH_NAME already exists."
    mvn clean install -DskipTests
  else
    echo "$CURRENT Branch is NOT EQUALS $BRANCH_NAME "
  fi
}

PWD=$(pwd)
echo "The absolute exec path is $PWD"

if [[ "$PWD" != *"java-common-libs"* ]]; then
  ## biodata
  if [[ "$PWD" == *"biodata"* ]]; then
    echo "It's biodata."
    install "java-common-libs"
  fi
  ## cellbase
  if [[ "$PWD" == *"cellbase"* ]]; then
    echo "It's cellbase."
    install "java-common-libs"
    install "biodata"
  fi
  ## opencga
  if [[ "$PWD" == *"opencga"* ]]; then
    echo "It's opencga."
    install "java-common-libs"
    install "biodata"
    install "cellbase"
  fi
fi