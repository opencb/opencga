#!/usr/bin/env sh


if [ -z ${HADOOP_SSH_USER} ] ; then
  echo "Undefined HADOOP_SSH_USER" 1>&2
  exit 1
fi

if [ -z ${HADOOP_SSH_HOST} ] ; then
  echo "Undefined HADOOP_SSH_HOST" 1>&2
  exit 1
fi

SRC_FILE=$1
TARGET_FILE=$2

if [ -z ${SRC_FILE} ] ; then
  echo "Undefined SRC_FILE" 1>&2
  exit 1
fi

if [ -z ${TARGET_FILE} ] ; then
  echo "Undefined TARGET_FILE" 1>&2
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

## From this point, the script should fail if any command fails
set -e

echo "--------"
echo "Copy file from Hadoop edge node : ${HADOOP_SSH_USER}@${HADOOP_SSH_HOST}:${SRC_FILE} to ${TARGET_FILE}" 1>&2

CMD="${SSHPASS_CMD} scp -r ${SSH_OPTS} ${HADOOP_SSH_USER}@${HADOOP_SSH_HOST}:${SRC_FILE} ${TARGET_FILE}"
#CMD="${SSHPASS_CMD} rsync -avz --remove-source-files -e ssh ${HADOOP_SSH_USER}@${HADOOP_SSH_HOST}:${SRC_FILE} ${TARGET_FILE}"
echo $CMD 1>&2
$CMD 1>&2

echo "Remove file from Hadoop edge node" 1>&2
CMD="${SSHPASS_CMD} ssh ${SSH_OPTS} ${HADOOP_SSH_USER}@${HADOOP_SSH_HOST} rm -rf ${SRC_FILE}"
echo $CMD 1>&2
$CMD 1>&2

