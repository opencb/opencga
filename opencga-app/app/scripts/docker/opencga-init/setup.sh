#!/bin/bash

# TODO
# Change hbase.client.keyvalue.maxsize to 0 in /etc/hbase/conf/hbase-site.xml on hadoop cluster to avoid 'KeyValue size too large' error
# by sshing to your (hdinsight cluster e.g. ssh sshuser@bartcga-ssh.azurehdinsight.net) and changing the xml (e.g. vi /etc/hbase/conf/hbase-site.xml)

# copy conf files from hdinsight cluster (from /etc/hadoop/conf & /etc/hbase/conf) to opencga VM
# place these files in /opt/opencga/conf/hadoop, by e.g.: (todo: change connection strings)
echo "Fetching HDInsight configuration"
sshpass -p $HD_INSIGHTS_SSH_PASS scp -o StrictHostKeyChecking=no -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -r $HD_INSIGHTS_SSH_USER@$HD_INSIGHTS_SSH_DNS:/etc/hadoop/conf/* /opt/opencga/conf/hadoop
# same with /etc/hbase/conf, e.g.
sshpass -p $HD_INSIGHTS_SSH_PASS scp -o StrictHostKeyChecking=no  -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -r $HD_INSIGHTS_SSH_USER@$HD_INSIGHTS_SSH_DNS:/etc/hbase/conf/* /opt/opencga/conf/hadoop

echo "Initialising config"
OUT=$(python3 ../init-config.py \
--search-host "$SEARCH_HOST" \
--clinical-host "$CLINICAL_HOST" \
--catalog-database-host "$CATALOG_DATABASE_HOST" \
--catalog-database-user "$CATALOG_DATABASE_USER" \
--catalog-database-password "$CATALOG_DATABASE_PASSWORD" \
--catalog-search-host "$CATALOG_SEARCH_HOST" \
--catalog-search-user "$CATALOG_SEARCH_USER" \
--catalog-search-password "$CATALOG_SEARCH_PASSWORD" \
--rest-host "$REST_HOST" \
--grpc-host "$GRPC_HOST" \
--save)

# Copies the config files from our local directory into a
# persistent volume to be shared by the other containers.
echo "Initialising volume"
mkdir -p /opt/volume/conf /opt/volume/sessions
cp -r /opt/opencga/conf/* /opt/volume/conf

echo "Installing catalog"
/opt/opencga/bin/opencga-admin.sh catalog install --secret-key ${1}
