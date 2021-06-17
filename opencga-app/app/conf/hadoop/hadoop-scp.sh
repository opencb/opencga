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
  SSHPASS_CMD="sshpass -e "
fi

SSH_OPTS="-q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=60"
if [ ! -z ${HADOOP_SSH_KEY} ] && [ -f ${HADOOP_SSH_KEY} ] ; then
  SSH_OPTS="${SSH_OPTS} -i ${HADOOP_SSH_KEY}"
fi

SSH="${SSHPASS_CMD}ssh ${SSH_OPTS} ${HADOOP_SSH_USER}@${HADOOP_SSH_HOST}"

function printAndRun() {
  CMD=$@
  echo " $ "$CMD 1>&2
  $CMD 1>&2
}

echo "--------" 1>&2
echo "Copy file from Hadoop edge node : ${HADOOP_SSH_HOST}" 1>&2
echo " * SOURCE : ${HADOOP_SSH_USER}@${HADOOP_SSH_HOST}:${SRC_FILE}" 1>&2
echo " * TARGET : ${USER}@${HOSTNAME}:${TARGET_FILE}" 1>&2

## From this point, the script should fail if any command fails
set -e

echo "Check if source file exists" 1>&2
printAndRun "${SSH} test -f ${SRC_FILE}" || (echo "Source file does not exist!" 1>&2 && exit 1)

if [ -f ${TARGET_FILE} ]; then
  target_inode=`ls -i ${TARGET_FILE} | cut -d " " -f 1`
  src_inode=`${SSH} ls -i ${SRC_FILE} | cut -d " " -f 1`
  if [ $target_inode == $src_inode ]; then
    target_md5=`md5sum ${TARGET_FILE} | cut -d " " -f 1`
    src_md5=`${SSH} md5sum ${SRC_FILE} | cut -d " " -f 1`
    if [ $target_md5 == $src_md5 ]; then
      echo "Files are in the same file system. Skip copy" 1>&2
      exit 0;
    fi
  fi
fi

echo "Copy file"
CMD="${SSHPASS_CMD} scp -r ${SSH_OPTS} ${HADOOP_SSH_USER}@${HADOOP_SSH_HOST}:${SRC_FILE} ${TARGET_FILE}"
#CMD="${SSHPASS_CMD} rsync -avz --remove-source-files -e ssh ${HADOOP_SSH_USER}@${HADOOP_SSH_HOST}:${SRC_FILE} ${TARGET_FILE}"
printAndRun $CMD

echo "Remove file from Hadoop edge node" 1>&2
printAndRun "${SSH} rm -rf ${SRC_FILE}"

