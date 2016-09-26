#!/usr/bin/env bash

# Variable BASEDIR is defined by the main script

#Set log4j properties file
export JAVA_OPTS="${JAVA_OPTS} -Dlog4j.configuration=file:${BASEDIR}/conf/log4j.properties"

# export OPENCGA_HOME=${BASEDIR}

# see http://stackoverflow.com/questions/592620/check-if-a-program-exists-from-a-bash-script
# Check if the command hbase exists
if `command -v hbase >/dev/null 2>&1` ; then
    export CLASSPATH_PREFIX=${CLASSPATH_PREFIX}:`hbase classpath | tr ":" "\n" | grep "conf" | tr "\n" ":"`
    export HADOOP_CLASSPATH=${BASEDIR}/libs/protobuf-java-*:${BASEDIR}/libs/avro-*:${CLASSPATH_PREFIX}
    export HADOOP_USER_CLASSPATH_FIRST=true
fi