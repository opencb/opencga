#!/bin/bash

BRANCH_NAME=$1
DEPENDENCIES_SHA=${DEPENDENCIES_SHA:-""}
WORKSPACE=${WORKSPACE:-/home/runner/work/}

if [[ -z "$BRANCH_NAME"  ]]; then
  echo "The first parameter is mandatory and must be a valid branch name."
  exit 1
fi

function checkout(){
  local REPO=$1
  echo "::group::Installing '$REPO' project from branch $BRANCH_NAME"
  cd "${WORKSPACE}" || exit 2
  git clone https://github.com/opencb/"$REPO".git -b "$BRANCH_NAME"
  if [ -d "./$REPO" ]; then
    cd "$REPO" || exit 2
    DEPENDENCIES_SHA=${DEPENDENCIES_SHA}:$(git rev-parse HEAD)
    echo "Branch name $BRANCH_NAME already exists."
  else
    echo "Branch name $BRANCH_NAME does not exist in $REPO repository. Skipping installation."
  fi
  echo "::endgroup::"
}

checkout "java-common-libs"
checkout "biodata"

## Apply sha1 to DEPENDENCIES_SHA if contains `:`
if [[ "$DEPENDENCIES_SHA" == *":"* ]]; then
  DEPENDENCIES_SHA=$(echo -n "$DEPENDENCIES_SHA" | sha1sum | awk '{print $1}')
fi

## Export DEPENDENCIES_SHA as github output
echo "dependencies_sha=$DEPENDENCIES_SHA" >> "$GITHUB_OUTPUT"