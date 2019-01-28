#!/bin/sh

sshpass -p "$HBASE_SSH_PASS" ssh -o StrictHostKeyChecking=no -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "$HBASE_SSH_USER@$HBASE_SSH_DNS" "sudo sed -i '/<name>hbase.client.keyvalue.maxsize<\/name>/!b;n;c<value>0</value>' /etc/hbase/conf/hbase-site.xml"

# copy conf files from hdinsight cluster (from /etc/hadoop/conf & /etc/hbase/conf) to opencga VM
# place these files in /opt/opencga/conf/hadoop, by e.g.: (todo: change connection strings)
echo "Fetching HDInsight configuration"
sshpass -p "$HBASE_SSH_PASS" scp -o StrictHostKeyChecking=no -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -r "$HBASE_SSH_USER@$HBASE_SSH_DNS":/etc/hadoop/conf/* /opt/opencga/conf/hadoop
# same with /etc/hbase/conf, e.g.
sshpass -p "$HBASE_SSH_PASS" scp -o StrictHostKeyChecking=no  -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -r "$HBASE_SSH_USER@$HBASE_SSH_DNS":/etc/hbase/conf/* /opt/opencga/conf/hadoop

echo "Initialising configs"
# Override Yaml configs
python3 /tmp/override-yaml.py \
--search-hosts "$SEARCH_HOSTS" \
--clinical-hosts "$CLINICAL_HOSTS" \
--cellbase-hosts "$CELLBASE_HOSTS" \
--catalog-database-hosts "$CATALOG_DATABASE_HOSTS" \
--catalog-database-user "$CATALOG_DATABASE_USER" \
--catalog-database-password "$CATALOG_DATABASE_PASSWORD" \
--catalog-search-hosts "$CATALOG_SEARCH_HOSTS" \
--catalog-search-user "$CATALOG_SEARCH_USER" \
--catalog-search-password "$CATALOG_SEARCH_PASSWORD" \
--rest-host "$REST_HOST" \
--grpc-host "$GRPC_HOST" \
--batch-execution-mode "$BATCH_EXEC_MODE" \
--batch-account-name "$BATCH_ACCOUNT_NAME" \
--batch-account-key "$BATCH_ACCOUNT_KEY" \
--batch-endpoint "$BATCH_ENDPOINT" \
--batch-pool-id "$BATCH_POOL_ID" \
--batch-docker-args "$BATCH_DOCKER_ARGS" \
--batch-docker-image "$BATCH_DOCKER_IMAGE" \
--batch-max-concurrent-jobs "$BATCH_MAX_CONCURRENT_JOBS" \
--save
# Override Js configs
python3 /tmp/override-js.py \
--cellbase-hosts "$CELLBASE_HOSTS" \
--rest-host "$REST_HOST" \
--save

# Copies the config files from our local directory into a
# persistent volume to be shared by the other containers.
echo "Initialising volume"
mkdir -p /opt/volume/conf /opt/volume/sessions /opt/volume/variants
cp -r /opt/opencga/conf/* /opt/volume/conf

echo "Installing catalog"
echo "${OPENCGA_PASS}" | /opt/opencga/bin/opencga-admin.sh catalog install --secret-key ${1}

# Catalog install will create a sub-folders in sessions
# that need copying to the volume.
cp -r /opt/opencga/sessions/* /opt/volume/sessions