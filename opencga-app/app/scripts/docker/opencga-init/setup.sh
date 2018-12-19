#!/bin/bash

# TODO
# Change hbase.client.keyvalue.maxsize to 0 in /etc/hbase/conf/hbase-site.xml on hadoop cluster to avoid 'KeyValue size too large' error
# by sshing to your (hdinsight cluster e.g. ssh sshuser@bartcga-ssh.azurehdinsight.net) and changing the xml (e.g. vi /etc/hbase/conf/hbase-site.xml)

# copy conf files from hdinsight cluster (from /etc/hadoop/conf & /etc/hbase/conf) to opencga VM
# place these files in /opt/opencga/conf/hadoop, by e.g.: (todo: change connection strings)
sshpass -p $HD_INSIGHTS_SSH_PASS scp -o StrictHostKeyChecking=no -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -r $HD_INSIGHTS_SSH_USER@$HD_INSIGHTS_SSH_DNS:/etc/hadoop/conf/* /opt/opencga/conf/hadoop
# same with /etc/hbase/conf, e.g.
sshpass -p $HD_INSIGHTS_SSH_PASS scp -o StrictHostKeyChecking=no  -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -r $HD_INSIGHTS_SSH_USER@$HD_INSIGHTS_SSH_DNS:/etc/hbase/conf/* /opt/opencga/conf/hadoop

# Copies the config files from our local directory into a
# persistent volume to be shared by the other containers.
mkdir -p /opt/volume/conf && cp -r /opt/opencga/conf/* /opt/volume/

/opt/opencga/bin/opencga-admin.sh catalog install --secret-key ${1}