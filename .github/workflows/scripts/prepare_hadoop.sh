#!/bin/bash

set -e
set -o pipefail


function main() {
  HADOOP_FLAVOUR="hbase2.0"

  while [[ $# -gt 0 ]]; do
    key="$1"
    value="$2"
    case $key in
    --hadoop-flavour)
      HADOOP_FLAVOUR="$value"
      shift # past argument
      shift # past value
      ;;
    --hadoop-thirdparty-version)
      HADOOP_THIRDPARTY_VERSION="$value"
      shift # past argument
      shift # past value
      ;;
    --verbose)
      set -x
      shift # past key
      ;;
    *) # unknown option
      echo "Unknown option $key"
      return 1
      ;;
    esac
  done

  if [ -z "$HADOOP_THIRDPARTY_VERSION" ]; then
    HADOOP_THIRDPARTY_VERSION=$(mvn help:evaluate -Dexpression=opencga.hadoop.thirdparty.version -q -DforceStdout)
  fi

  # Check if HADOOP_THIRDPARTY can be download
  ARTIFACT="org.opencb.opencga.hadoop.thirdparty:opencga-hadoop-shaded-${HADOOP_FLAVOUR}:${HADOOP_THIRDPARTY_VERSION}"
  echo "Looking for artifact:"
  echo " - $ARTIFACT"
  if mvn dependency:get "-Dartifact=${ARTIFACT}" &> /dev/null; then
    echo "Hadoop thirdparty jar found."
    return 0;
  fi

  echo "Hadoop thirdparty jar not found in local maven repository. Building opencga-hadoop-thirdparty..."
  local GIT_REF=
  if [[ "$HADOOP_THIRDPARTY_VERSION" == *"-SNAPSHOT" ]]; then
    local VERSION=$(echo "$HADOOP_THIRDPARTY_VERSION" | cut -d "-" -f 1)
    local MAJOR=$(echo "$VERSION" | cut -d "." -f 1)
    local MINOR=$(echo "$VERSION" | cut -d "." -f 2)
    local PATCH=$(echo "$VERSION" | cut -d "." -f 3)
    if [ $PATCH -gt 0 ]; then ## It's a hotfix
      GIT_REF="release-$MAJOR.$MINOR.x"
    elif [ $MINOR -eq 0 ]; then ## It's a develop branch
      GIT_REF="develop"
    else  ## It's a release branch
      GIT_REF="release-$MAJOR.x.x"
    fi
  else
    GIT_REF="v$HADOOP_THIRDPARTY_VERSION"
  fi
  install "$GIT_REF" "$HADOOP_FLAVOUR"
  if [ $? -ne 0 ]; then
    echo "Failed to build opencga-hadoop-thirdparty."
    return 1
  fi

  return 0;
}


function install(){
  local GIT_REF=${1:?"Git reference (branch, tag) is required"}
  local HADOOP=${2:?"Hadoop flavour is required"}
  echo "Installing $HADOOP hadoop flavour from $GIT_REF"
  local REPO="opencga-hadoop-thirdparty"
  local TMP_DIR_HOME="dependency-checkouts/"
  mkdir -p "$TMP_DIR_HOME"
  rm -rf "${TMP_DIR_HOME:?}"/*
  TEMP_DIR="$(mktemp -d --tmpdir="$TMP_DIR_HOME" --suffix="$(date +%Y%m%d%H%M%S)-$REPO")"
  cd "$TEMP_DIR" || return 2
  echo "Cloning repository $REPO with ref $GIT_REF"

  # Build HTTPS clone URL using optional token for private access
  local CLONE_URL
  if [[ -n "${THIRDPARTY_READ_TOKEN:-}" ]]; then
    CLONE_URL="https://x-access-token:${THIRDPARTY_READ_TOKEN}@github.com/opencb/${REPO}.git"
  else
    CLONE_URL="git@github.com:opencb/${REPO}.git"
  fi

  # Shallow clone at the requested ref
  git clone --depth 1 -b "$GIT_REF" "$CLONE_URL"
  PROJECT_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
  echo "Cloned repository $REPO with ref $GIT_REF and version $PROJECT_VERSION"
  cd "$REPO" || return 2
  ./dev/build.sh "$HADOOP"
  cd - || return 2
}



main "$@"
exit $?

