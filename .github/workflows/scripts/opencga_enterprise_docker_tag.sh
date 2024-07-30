#!/bin/bash

set -e
set -x

if [ -n "$1" ]; then
  echo "$1"
else
  ## Navigate to the root folder where the pom.xml is
  cd "$(dirname "$0")"/../../../ || exit 2

  ## Read the opencga version from the pom.xml
  BUILD_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)

  HADOOP_LIB=$(find build/libs/ -name 'opencga-storage-hadoop-lib*.jar')

  if [ "$(wc -l <<< "$HADOOP_LIB")" -gt 1 ]; then
    echo "ERROR: Multiple hadoop dependencies found:" 1>&2
    echo "$HADOOP_LIB" 1>&2
    exit 1
  elif [ -z "$HADOOP_LIB" ]; then
    echo "$BUILD_VERSION"
  else
    HADOOP_FLAVOUR=$(basename "$HADOOP_LIB" | cut -d "-" -f 5)
    echo "$BUILD_VERSION"-"$HADOOP_FLAVOUR"
  fi
fi