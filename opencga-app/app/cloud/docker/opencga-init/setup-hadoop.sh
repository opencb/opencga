#!/bin/sh

# INIT_HADOOP_SSH_DNS
# INIT_HADOOP_SSH_USER
# INIT_HADOOP_SSH_PASS
# INIT_HADOOP_SSH_KEY
# INIT_HADOOP_SSH_REMOTE_OPENCGA_HOME


# Hadoop installation
SSHPASS_CMD=
if [ -z "${INIT_HADOOP_SSH_PASS}" ] ; then
  # If empty, assume ssh-key exists in the system
  SSHPASS_CMD=""
else
  export SSHPASS=$INIT_HADOOP_SSH_PASS
  # If non zero, use sshpass command
  SSHPASS_CMD="sshpass -e"
fi

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=60"
if [ -n "${INIT_HADOOP_SSH_KEY}" ] && [ -f "${INIT_HADOOP_SSH_KEY}" ] ; then
  SSH_OPTS="${SSH_OPTS} -i ${INIT_HADOOP_SSH_KEY}"
fi

HADOOP_USER_HOST="$INIT_HADOOP_SSH_USER@$INIT_HADOOP_SSH_DNS"

FILE=/opt/volume/conf/hadoop
if [ -d "$FILE" ]; then
    echo "$FILE already exists"
    echo "Copy jar-with-dependencies to hadoop"
    $SSHPASS_CMD scp ${SSH_OPTS} -r /opt/opencga/*.jar "$HADOOP_USER_HOST":"$INIT_HADOOP_SSH_REMOTE_OPENCGA_HOME"
else
    #$SSHPASS_CMD ssh ${SSH_OPTS} "$HADOOP_USER_HOST" "sudo sed -i '/<name>hbase.client.keyvalue.maxsize<\/name>/!b;n;c<value>0</value>' /etc/hbase/conf/hbase-site.xml"

    # copy conf files from Hadoop cluster (from /etc/hadoop/conf & /etc/hbase/conf) to opencga VM
    # place these files in /opt/opencga/conf/hadoop, by e.g.:
    echo "Fetching Hadoop configuration"
    $SSHPASS_CMD ssh ${SSH_OPTS} "$HADOOP_USER_HOST" hbase classpath | tr ":" "\n" | grep "/conf$" | grep "hadoop\|hbase" | sort | uniq | while read i ; do
      $SSHPASS_CMD scp ${SSH_OPTS} -r "$HADOOP_USER_HOST:${i}"/* /opt/opencga/conf/hadoop
    done

    # Copy the OpenCGA installation directory to the Hadoop cluster
    $SSHPASS_CMD ssh ${SSH_OPTS} "$HADOOP_USER_HOST" mkdir -p "$INIT_HADOOP_SSH_REMOTE_OPENCGA_HOME"
    # TODO - Optimize this down to only required jars
    $SSHPASS_CMD scp ${SSH_OPTS} -r /opt/opencga/* "$HADOOP_USER_HOST":"$INIT_HADOOP_SSH_REMOTE_OPENCGA_HOME"

    mkdir -p "$FILE"
    cp -r /opt/opencga/conf/hadoop/* "$FILE"
fi