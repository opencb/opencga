#!/usr/bin/env bash

# Variable BASEDIR is defined by the main script

#Set log4j properties file
export JAVA_OPTS="${JAVA_OPTS} -Dlog4j.configuration=file:${BASEDIR}/conf/log4j.properties"

# export OPENCGA_HOME=${BASEDIR}

## TODO We must make sure we load any existing JAR file, only one can exist.
if [ -e "${BASEDIR}/monitor/dd-java-agent.jar" ]; then
    export JAVA_OPTS="${JAVA_OPTS} -javaagent:${BASEDIR}/monitor/dd-java-agent.jar"
fi

phoenix=""
if `command -v phoenix_utils.py > /dev/null 2>&1`; then
    phoenix=$(phoenix_utils.py | grep phoenix_client_jar | cut -f 2 -d " ")
fi

# see http://stackoverflow.com/questions/592620/check-if-a-program-exists-from-a-bash-script
# Check if the command hbase exists
if `command -v hbase >/dev/null 2>&1` ; then
    EMR_DEPENDENCIES=`find  $(hadoop classpath | tr ":" " ") -type f -name "hadoop-common-*-amzn-3.jar" -o -name "emrfs-hadoop-assembly-*.jar"  2> /dev/null | tr "\n" ":"`
    export CLASSPATH_PREFIX=${CLASSPATH_PREFIX}:${EMR_DEPENDENCIES}:`hbase classpath | tr ":" "\n" | grep "conf" | tr "\n" ":"`
    proto=$(find ${BASEDIR}/libs/ -name "protobuf-java-*.jar" | tr "\n" ":")
    avro=$(find ${BASEDIR}/libs/ -name "avro-*.jar" | tr "\n" ":")
    export HADOOP_CLASSPATH="${phoenix}:${proto}:${avro}:${CLASSPATH_PREFIX}"
    export HADOOP_USER_CLASSPATH_FIRST=true
fi