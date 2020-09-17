#!/usr/bin/env bash

# Variables defined in main script
# BASEDIR
# PRGDIR
# JAVA_OPTS
# CLASSPATH_PREFIX


# Increase Java Heap if needed
if [ -z "$JAVA_HEAP" ]; then
  case `basename $PRG` in
  "opencga.sh")
    JAVA_HEAP="2048m"
    ;;
  "opencga-admin.sh")
    JAVA_HEAP="8192m"
    ;;
  "opencga-internal.sh")
    JAVA_HEAP="12288m"
    ;;
  *)
    JAVA_HEAP="2048m"
    ;;
  esac
fi

case `basename $PRG` in
  "opencga.sh")
    LOGFILE="log4j2.xml"
    ;;
  "opencga-admin.sh")
    LOGFILE="log4j2-server.xml"
    ;;
  "opencga-internal.sh")
    LOGFILE="log4j2.xml"
    ;;
  *)
    LOGFILE="log4j2.xml"
    ;;
esac

#Set log4j properties file
export JAVA_OPTS="${JAVA_OPTS} -Dlog4j2.configurationFile=file:${BASEDIR}/conf/${LOGFILE}"
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