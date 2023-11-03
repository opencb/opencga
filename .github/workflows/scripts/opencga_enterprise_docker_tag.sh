#!/bin/bash

set -e

## Navigate to the root folder where the pom.xml is
cd "$(dirname "$0")"/../../../ || exit 2

## Read the opencga version from the pom.xml
BUILD_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)

HADOOP_DEPS=$(find build/libs/ -name 'opencga-storage-hadoop-deps*.jar')

if [ "$(wc -l <<< "$HADOOP_DEPS")" -gt 1 ]; then
  echo "ERROR: Multiple hadoop dependencies found:" 1>&2
  echo "$HADOOP_DEPS" 1>&2
  exit 1
elif [ "$(wc -l <<< "$HADOOP_DEPS")" -eq 0 ]; then
  echo "$BUILD_VERSION"
else
  HADOOP_FLAVOUR=$(basename "$HADOOP_DEPS" | cut -d "-" -f 5)
  echo "$BUILD_VERSION"-"$HADOOP_FLAVOUR"
fi