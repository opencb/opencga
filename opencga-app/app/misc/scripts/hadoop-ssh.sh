#!/usr/bin/env bash


if [ -z "${HADOOP_SSH_USER}" ] ; then
  echo "Undefined HADOOP_SSH_USER" 1>&2
  exit 1
fi

if [ -z "${HADOOP_SSH_HOST}" ] ; then
  echo "Undefined HADOOP_SSH_HOST" 1>&2
  exit 1
fi

SSHPASS_CMD=
if [ -z "${SSHPASS}" ] ; then
  # If empty, assume ssh-key exists in the system
  SSHPASS_CMD=""
else
  # If non zero, use sshpass command
  SSHPASS_CMD="sshpass -e "
fi

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=60"
if [ -n "${HADOOP_SSH_KEY}" ] && [ -f "${HADOOP_SSH_KEY}" ] ; then
  SSH_OPTS="${SSH_OPTS} -i ${HADOOP_SSH_KEY}"
fi

trap 'echo "SSH Process interrupted! Run time : ${SECONDS}s" && exit 1 1>&2 ' INT TERM

echo "Connect to Hadoop edge node ${HADOOP_SSH_USER}@${HADOOP_SSH_HOST}" 1>&2

echo "${SSHPASS_CMD}ssh ${SSH_OPTS} ${HADOOP_SSH_USER}@${HADOOP_SSH_HOST}" 1>&2

# Escape args with single quotes
CMD=
for arg in "$@" ; do
    # Escape single quote, if any :
#    arg=`echo $arg | sed "s/'/'\"'\"'/g"`   # aaa'aaa --> 'aaa'"'"'aaa'
    arg=$(echo "$arg" | sed "s/'/'\\\\\\''/g") # aaa'aaa --> 'aaa'\''aaa'
    CMD="${CMD}'${arg}' "
done
echo ${CMD} 1>&2

# Create a script to be executed on the remote server
## -d '' : read until EOF
## -r : do not interpret backslash escapes
## SSH_SCRIPT variable to store the script
read -d '' -r SSH_SCRIPT << EOF
echo "PID=\$\$" >&2

export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}
export HADOOP_USER_CLASSPATH_FIRST=${HADOOP_USER_CLASSPATH_FIRST}

if \$(command -v hbase >/dev/null 2>&1) ; then
    hbase_conf=\$(hbase classpath | tr ":" "\n" | grep "/conf" | tr "\n" ":")

    if [ -z "\${HADOOP_CLASSPATH}" ]; then
        export HADOOP_CLASSPATH=\${hbase_conf}
    else
        export HADOOP_CLASSPATH=\${HADOOP_CLASSPATH}:\${hbase_conf}
    fi
fi


exec ${CMD}

EOF

STDIN_CONCAT=
## Check if stdin comes from a terminal or a pipe
if [ -t 0 ]; then
    # stdin is a terminal.
    # Interactive mode not supported. Don't concatenate stdin
    STDIN_CONCAT=""
else
    # stdin is a pipe
    # Concatenate stdin with the script
    STDIN_CONCAT="-"
fi

## Concatenate the script with stdin (if any) and execute it on the remote server
cat <(echo "${SSH_SCRIPT}") ${STDIN_CONCAT} | ${SSHPASS_CMD} ssh ${SSH_OPTS} "${HADOOP_SSH_USER}@${HADOOP_SSH_HOST}" /bin/bash

EXIT_CODE=$?

echo "SSH Process completed!" 1>&2
echo " - Run time : ${SECONDS}s" 1>&2
echo " - Exit code: ${EXIT_CODE}" 1>&2

exit ${EXIT_CODE}