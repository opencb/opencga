#!/bin/bash

IS_RELEASE=$1

## Navigate to the root folder where the pom.xml is
cd "$(dirname "$0")"/../../../ || exit 2

## Read the opencga version from the pom.xml
BUILD_VERSION=$(mvn help:evaluate -Dexpression=opencga.version -q -DforceStdout)

if [[ "$IS_RELEASE" == "true" ]]; then
  echo "v$BUILD_VERSION"
  exit 0
fi

## Check if this is a TASK branch, and exists on opencga. If so, use that branch
GIT_BRANCH="$(git branch --show-current)"
if [[ "$GIT_BRANCH" == TASK-* ]]; then
  if [ "$(git ls-remote https://github.com/opencb/opencga.git "$GIT_BRANCH" )" ] ; then
    echo "$GIT_BRANCH";
    exit 0;
  fi
fi

## We remove the -SNAPSHOT if it exists
CLEAN_BUILD_VERSION=$(echo "$BUILD_VERSION" | cut -d "-" -f 1)

## Read the numbers separately to compose the name of the branch
MAJOR=$(echo "$CLEAN_BUILD_VERSION" | cut -d "." -f 1)
MINOR=$(echo "$CLEAN_BUILD_VERSION" | cut -d "." -f 2)
PATCH=$(echo "$CLEAN_BUILD_VERSION" | cut -d "." -f 3)

## it's a HOTFIX. Patch is great than 0

if [ "$PATCH" -gt 0 ]; then
  echo "release-$MAJOR.$MINOR.x"
  exit 0
fi

## It's develop branch
if [[ "$MINOR" ==  "0" ]]; then
  echo "develop"
  exit 0
else #Is release branch
  echo "release-$MAJOR.x.x"
  exit 0
fi