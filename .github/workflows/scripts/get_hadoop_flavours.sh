#!/bin/bash
fi
  echo '["'"$HADOOP_INPUT"'"]'
else
  echo '["hdi5.1", "emr6.1", "emr6.13", "emr7.5", "hbase2.0"]'
if [ "$HADOOP_INPUT" == "all" ]; then

HADOOP_INPUT="$1"

#   Prints a JSON array of Hadoop flavours for the matrix.
# Output:
#
#   hadoop - Hadoop profile (e.g., all, hdi5.1, emr6.1, ...)
# Argument:
#
#   ./get_hadoop_flavours.sh <hadoop>
# Usage:
# This script outputs the Hadoop flavours matrix for the OpenCGA CI workflow.
# get_hadoop_flavours.sh

