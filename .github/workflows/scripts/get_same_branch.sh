#!/bin/bash

#########################################################
##### FUNCTIONS TO PRINT COLOURED MESSAGES  #############
#########################################################


ENTERPRISE_BRANCH_NAME=$1

#function calculate_branch(){
#  if [[ $ENTERPRISE_BRANCH_NAME == "v"* ]]; then
#    echo "v$1"
#  else
#    CURRENT_BRANCH="$(git branch --show-current)"
#    if [[ "$CURRENT_BRANCH" != "release"* ]];then
#      echo "$CURRENT_BRANCH"
#    else
#      local VERSION=$(echo "$1" | cut -d "-" -f 1)
#      local MAJOR=$(echo "$VERSION" | cut -d "." -f 1)
#      local MINOR=$(echo "$VERSION" | cut -d "." -f 2)
#      local PATCH=$(echo "$VERSION" | cut -d "." -f 3)
#      local HOTFIX=$(echo "$VERSION" | cut -d "." -f 4)
#      if [ -z "$HOTFIX" ]; then
#        echo "release-$MAJOR.x.x"
#      else
#        echo "release-$MAJOR.$MINOR.x"
#      fi
#    fi
#  fi
#}
function calculate_branch(){
  if [[ $ENTERPRISE_BRANCH_NAME == "v"* ]]; then
    echo "v$1"
  else
    CURRENT_BRANCH="$(git branch --show-current)"
    if [[ "$CURRENT_BRANCH" != "release"* ]];then
      echo "$CURRENT_BRANCH"
    else
      local VERSION=$(echo "$1" | cut -d "-" -f 1)
      local MAJOR=$(echo "$VERSION" | cut -d "." -f 1)
      local MINOR=$(echo "$VERSION" | cut -d "." -f 2)
      local PATCH=$(echo "$VERSION" | cut -d "." -f 3)

      # Comprobar si es hotfix: el PATCH es mayor que 0
      if [ "$PATCH" -gt 0 ]; then
        echo "release-$MAJOR.$MINOR.x"
      else
        echo "release-$MAJOR.x.x"
      fi
    fi
  fi
}

function install(){
  CURRENT_DIR=$PWD
  local REPO=$1
  local BRANCH_NAME="$(calculate_branch $2)"
  echo "Version of $REPO to download correct $2 should be in $BRANCH_NAME"
  cd /tmp/ || exit 2
  git clone https://github.com/opencb/"$REPO".git -b "$BRANCH_NAME"
  if [ -d "./$REPO" ]; then
    cd "$REPO" || exit 2
    echo "Branch name $BRANCH_NAME already exists."
    mvn clean install -DskipTests --no-transfer-progress
    if [ $? -eq 0 ]; then
      echo "$REPO Compilation Successful!!!"
    fi
  else
   if [[ "$BRANCH_NAME" != "TASK"*  ]]; then
      echo "The $REPO branch $BRANCH_NAME cloning process has failed!"
      exit 1
    else
      echo "The $REPO branch $BRANCH_NAME not exists we use version $2 from maven repo"
    fi
  fi
  cd "$CURRENT_DIR" || exit
}

echo "Calculating dependencies branches for $ENTERPRISE_BRANCH_NAME"
JCL_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=java-common-libs.version -q -DforceStdout)"
install "java-common-libs" $JCL_DEPENDENCY_VERSION
BIODATA_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=biodata.version -q -DforceStdout)"
install "biodata" $BIODATA_DEPENDENCY_VERSION
CELLBASE_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=cellbase.version -q -DforceStdout)"
install "cellbase" $CELLBASE_DEPENDENCY_VERSION
OPENCGA_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=opencga.version -q -DforceStdout)"
install "opencga" $OPENCGA_DEPENDENCY_VERSION
