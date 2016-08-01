#!/usr/bin/env bash

#Set log4j properties file
export JAVA_OPTS="${JAVA_OPTS} -Dlog4j.configuration=file:${BASEDIR}/conf/log4j.properties"

# export OPENCGA_HOME=${BASEDIR}
