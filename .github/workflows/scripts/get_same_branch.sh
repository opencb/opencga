#!/bin/bash

BRANCH_NAME=$1
HADOOP=${2:-hdi5.1}

if [[ -z "$BRANCH_NAME"  ]]; then
  echo "The first parameter is mandatory and must be a valid branch name."
  exit 1
fi

function install(){
  local REPO=$1
  echo "::group::Installing '$REPO' project from branch $BRANCH_NAME"
  cd /home/runner/work/ || exit 2
  git clone https://github.com/opencb/"$REPO".git -b "$BRANCH_NAME"
  if [ -d "./$REPO" ]; then
    cd "$REPO" || exit 2
    echo "Branch name $BRANCH_NAME already exists."
    mvn clean install -DskipTests --no-transfer-progress
  else
    echo "Branch name $BRANCH_NAME does not exist in $REPO repository. Skipping installation."
  fi
  echo "::endgroup::"
}

install "java-common-libs"
install "biodata"