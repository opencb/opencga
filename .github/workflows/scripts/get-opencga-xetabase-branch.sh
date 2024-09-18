#!/bin/bash

#The script receives a branch name as a parameter and returns the branch name to be used in the workflow
#If the branch name is TASK-*, and exists in opencga it returns the branch name
#If not and the branch name is a release branch, it calculate the branch name based in pom.xml enterprise dependency and returns it
#If not and the branch name is a hotfix branch, it calculate the branch name based in pom.xml enterprise dependency and returns it
#If not and patch is 0, it returns the develop branch name

## Navigate to the root folder where the pom.xml is
cd "$(dirname "$0")"/../../../ || exit 2

## Read the branch passed as parameter
if [[ -n $1 ]]; then
  GIT_BRANCH=$1
else
  GIT_BRANCH="$(git branch --show-current)"
fi

## Check if this branch exists on opencga and its name starts with TASK-. If so, use that branch
if [[ "$(git ls-remote https://github.com/opencb/opencga.git "$GIT_BRANCH" )" && "$GIT_BRANCH" == TASK-* ]]; then
  echo "$GIT_BRANCH";
  exit 0;
fi

## Read the opencga version from the pom.xml
BUILD_VERSION=$(mvn help:evaluate -Dexpression=opencga.version -q -DforceStdout)

## We remove the -SNAPSHOT if it exists
CLEAN_BUILD_VERSION=$(echo "$BUILD_VERSION" | cut -d "-" -f 1)

## Read the numbers separately to compose the name of the branch
MAJOR=$(echo "$CLEAN_BUILD_VERSION" | cut -d "." -f 1)
MINOR=$(echo "$CLEAN_BUILD_VERSION" | cut -d "." -f 2)
PATCH=$(echo "$CLEAN_BUILD_VERSION" | cut -d "." -f 3)


if [ "$PATCH" -gt 0 ]; then
  echo "release-$MAJOR.$MINOR.x"
  exit 0
fi

## It's develop branch
if [[ "$PATCH" ==  "0" ]]; then
  echo "develop"
  exit 0
else #Is release branch
  echo "release-$MAJOR.x.x"
  exit 0
fi