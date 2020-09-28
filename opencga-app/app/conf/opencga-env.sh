#!/usr/bin/env bash

# Variables defined in main script
# BASEDIR
# PRGDIR
# JAVA_OPTS
# CLASSPATH_PREFIX


# Increase Java Heap if needed
if [ -z "$JAVA_HEAP" ]; then
  case `basename $PRG` in
  "opencga-admin.sh")
    JAVA_HEAP="8192m"
    OPENCGA_LOG4J_CONFIGURATION_FILE=log4j2.xml
    OPENCGA_LOG_DIR=${OPENCGA_LOG_DIR:-$(grep "logDir" "${BASEDIR}/conf/configuration.yml" | cut -d ":" -f 2 | tr -d '" ')}
    ;;
  "opencga-internal.sh")
    JAVA_HEAP="12288m"
    OPENCGA_LOG4J_CONFIGURATION_FILE=log4j2.internal.xml
    OPENCGA_LOG_DIR=${OPENCGA_LOG_DIR:-$(grep "logDir" "${BASEDIR}/conf/configuration.yml" | cut -d ":" -f 2 | tr -d '" ')}
    ;;
#  "opencga.sh")
  *)
    JAVA_HEAP="2048m"
    OPENCGA_LOG4J_CONFIGURATION_FILE=log4j2.xml
    OPENCGA_LOG_DIR=""
    ;;
  esac
fi

if [ -n "$OPENCGA_LOG_DIR" ]; then
    export JAVA_OPTS="${JAVA_OPTS} -Dopencga.log.dir=${OPENCGA_LOG_DIR}"
fi

MONITOR_AGENT=""
## TODO We must make sure we load any existing JAR file, only one can exist.
if [ -e "${BASEDIR}/monitor/dd-java-agent.jar" ]; then
    MONITOR_AGENT="-javaagent:${BASEDIR}/monitor/dd-java-agent.jar"
fi

#Set log4j properties file
export JAVA_OPTS="${JAVA_OPTS} -Dlog4j2.configurationFile=file:${BASEDIR}/conf/${OPENCGA_LOG4J_CONFIGURATION_FILE}"
export JAVA_OPTS="${JAVA_OPTS} ${MONITOR_AGENT}"
#export JAVA_OPTS="${JAVA_OPTS} -Ddd.logs.injection=true -Ddd.agent.host=datadog-agent -Ddd.agent.port=8126 -javaagent:${BASEDIR}/misc/dd-java-agent.jar"
export JAVA_OPTS="${JAVA_OPTS} -Dfile.encoding=UTF-8"
export JAVA_OPTS="${JAVA_OPTS} -Xms256m -Xmx${JAVA_HEAP}"

export COLUMNS=`tput cols 2> /dev/null`
export LINES=`tput lines 2> /dev/null`

# export OPENCGA_HOME=${BASEDIR}

if [ -f "${BASEDIR}"/libs/opencga-storage-hadoop-core-*.jar ] ; then

    # Add the folder conf/hadoop to the classpath.
    # Add first the user defined hadoop configuration, and then the system hadoop conf, to allow the user to overwrite the configuration.
    if [ -z "${CLASSPATH_PREFIX}" ]; then
        export CLASSPATH_PREFIX=${BASEDIR}/conf/hadoop/
    else
        export CLASSPATH_PREFIX=${CLASSPATH_PREFIX}:${BASEDIR}/conf/hadoop/
    fi

    # Add the system hadoop configuration folders to the classpath.
    # Check if the command hbase exists
    #   see http://stackoverflow.com/questions/592620/check-if-a-program-exists-from-a-bash-script
    if `command -v hbase >/dev/null 2>&1` ; then
        hbase_conf=$(hbase classpath | tr ":" "\n" | grep "/conf" | tr "\n" ":")
        export CLASSPATH_PREFIX=${CLASSPATH_PREFIX}:${hbase_conf}
    fi

    # Add specific Amazon EMR dependencies
    if `command -v hadoop >/dev/null 2>&1` ; then
        EMR_DEPENDENCIES=`find  $(hadoop classpath | tr ":" " ") -type f -name "hadoop-common-*-amzn-3.jar" -o -name "emrfs-hadoop-assembly-*.jar"  2> /dev/null | tr "\n" ":"`
        export CLASSPATH_PREFIX=${CLASSPATH_PREFIX}:${EMR_DEPENDENCIES}
    fi

    phoenix=""
    if `command -v phoenix_utils.py > /dev/null 2>&1`; then
        phoenix=$(phoenix_utils.py | grep phoenix_client_jar | cut -f 2 -d " ")
    fi

    jackson=$(find ${BASEDIR}/libs/ -name "jackson-*-2.[0-9].[0-9].jar" | tr "\n" ":")
    proto=$(find ${BASEDIR}/libs/ -name "protobuf-java-*.jar" | tr "\n" ":")
    avro=$(find ${BASEDIR}/libs/ -name "avro-*.jar" | tr "\n" ":")
    export HADOOP_CLASSPATH="${phoenix}:${proto}:${avro}:${jackson}:${CLASSPATH_PREFIX}"
    export HADOOP_USER_CLASSPATH_FIRST=true
fi