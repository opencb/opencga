#!/bin/bash

#########################################################
##### FUNCTIONS TO PRINT COLOURED MESSAGES  #############
#########################################################
function red(){
   echo "$(tput setaf 1)$1$(tput setaf 7)"
}

function green(){
   echo "$(tput setaf 2)$1$(tput setaf 7)"
}
function yellow(){
   echo "$(tput setaf 3)$1$(tput setaf 7)"
}

#BRANCH_NAME=$1
#
#if [[ -z $BRANCH_NAME  ]]; then
#  echo "The first parameter is mandatory and must be a valid branch name."
#  exit 1
#fi
#
#if [[ $BRANCH_NAME != "TASK-"*   ]]; then
#  echo "No need to check dependencies."
#  exit 0
#fi

#function install(){
#  local REPO=$1
#  cd /home/runner/work/ || exit 2
#  git clone https://github.com/opencb/"$REPO".git -b "$BRANCH_NAME"
#  if [ -d "./$REPO" ]; then
#    cd "$REPO" || exit 2
#    echo "Branch name $BRANCH_NAME already exists."
#    mvn clean install -DskipTests
#  else
#    echo "$CURRENT Branch is NOT EQUALS $BRANCH_NAME "
#  fi
#}
#
#install "java-common-libs"
#install "biodata"
#install "cellbase"


function calculate_branch(){
  CURRENT_BRANCH="$(git branch --show-current)"
  if [[ "$CURRENT_BRANCH" != "release"* ]];then
    echo "$CURRENT_BRANCH"
  else
    local VERSION=$(echo "$1" | cut -d "-" -f 1)
    local MAJOR=$(echo "$VERSION" | cut -d "." -f 1)
    local MINOR=$(echo "$VERSION" | cut -d "." -f 2)
    local PATCH=$(echo "$VERSION" | cut -d "." -f 3)
    local HOTFIX=$(echo "$VERSION" | cut -d "." -f 4)
    if [ -z "$HOTFIX" ]; then
      echo "release-$MAJOR.$MINOR.x"
    else
      echo "release-$MAJOR.$MINOR.$PATCH.x"
    fi
  fi
}

function install(){
  CURRENT_DIR=$PWD
  local REPO=$1
  local BRANCH_NAME="$(calculate_branch $2)"
  green "Version of $REPO to download correct $2 should be in $BRANCH_NAME"
  cd /tmp/ || exit 2
  git clone https://github.com/opencb/"$REPO".git -b "$BRANCH_NAME"
  if [ -d "./$REPO" ]; then
    cd "$REPO" || exit 2
    green "Branch name $BRANCH_NAME already exists."
    mvn clean install -DskipTests
    if [ $? -eq 0 ]; then
      green "$REPO Compilation Successful!!!"
    fi
  else
   if [[ "$BRANCH_NAME" != "TASK"*  ]]; then
      red "The $REPO branch $BRANCH_NAME cloning process has failed!"
      exit 1
    else
      yellow "The $REPO branch $BRANCH_NAME not exists we use version $2 from maven repo"
    fi
  fi
  cd "$CURRENT_DIR" || exit
}


JCL_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=java-common-libs.version -q -DforceStdout)"
install "java-common-libs" $JCL_DEPENDENCY_VERSION
BIODATA_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=biodata.version -q -DforceStdout)"
install "biodata" $BIODATA_DEPENDENCY_VERSION
CELLBASE_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=cellbase.version -q -DforceStdout)"
install "cellbase" $CELLBASE_DEPENDENCY_VERSION
#OPENCGA_DEPENDENCY_VERSION="$(mvn help:evaluate -Dexpression=opencga.version -q -DforceStdout)"
#install "opencga" $OPENCGA_DEPENDENCY_VERSION

