#!/usr/bin/env bash


if [ -z ${HADOOP_SSH_USER} ] ; then
  echo "Undefined HADOOP_SSH_USER" 1>&2
  exit 1
fi

if [ -z ${HADOOP_SSH_HOST} ] ; then
  echo "Undefined HADOOP_SSH_HOST" 1>&2
  exit 1
fi

SSHPASS_CMD=
if [ -z ${SSHPASS} ] ; then
  # If empty, assume ssh-key exists in the system
  SSHPASS_CMD=""
else
  # If non zero, use sshpass command
  SSHPASS_CMD="sshpass -e"
fi

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=60"
if [ ! -z ${HADOOP_SSH_KEY} ] && [ -f ${HADOOP_SSH_KEY} ] ; then
  SSH_OPTS="${SSH_OPTS} -i ${HADOOP_SSH_KEY}"
fi

echo "Connect to Hadoop edge node ${HADOOP_SSH_USER}@${HADOOP_SSH_HOST}" 1>&2

echo "${SSHPASS_CMD} ssh ${SSH_OPTS} ${HADOOP_SSH_USER}@${HADOOP_SSH_HOST}"

${SSHPASS_CMD} ssh ${SSH_OPTS} ${HADOOP_SSH_USER}@${HADOOP_SSH_HOST} /bin/bash << EOF

export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}
export HADOOP_USER_CLASSPATH_FIRST=${HADOOP_USER_CLASSPATH_FIRST}

exec $@

EOF

