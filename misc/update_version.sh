#!/bin/bash

function yellow () {
   echo "$(tput setaf 3)$1$(tput setaf 7)"
}
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CURRENT_DIR=$PWD
cd $SCRIPT_DIR
cd ..
BRANCH_NAME=$(git branch --show-current)
CURRENT_VERSION="$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)"
if [[ "$CURRENT_VERSION" == *"$BRANCH_NAME"* ]]; then
  NEW_VERSION=${CURRENT_VERSION/"$BRANCH_NAME-"}
else
  CLEAN_RELEASE_VERSION=$(echo "$CURRENT_VERSION" | cut -d "-" -f 1)
  TAGS_VERSION=$(echo "$CURRENT_VERSION" | cut -d "-" -f 2)
  NEW_VERSION="$CLEAN_RELEASE_VERSION-$BRANCH_NAME-$TAGS_VERSION"
fi

mvn versions:set -DnewVersion="$NEW_VERSION" -DgenerateBackupPoms=false

yellow "The NEW_VERSION in pom.xml is $NEW_VERSION"
cd "$CURRENT_DIR" || exit 2
