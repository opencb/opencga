#!/bin/sh

sshpass -p "$INIT_HBASE_SSH_PASS" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "$INIT_HBASE_SSH_USER@$INIT_HBASE_SSH_DNS" "sudo sed -i '/<name>hbase.client.keyvalue.maxsize<\/name>/!b;n;c<value>0</value>' /etc/hbase/conf/hbase-site.xml"

# copy conf files from hdinsight cluster (from /etc/hadoop/conf & /etc/hbase/conf) to opencga VM
# place these files in /opt/opencga/conf/hadoop, by e.g.: (todo: change connection strings)
echo "Fetching HDInsight configuration"
sshpass -p "$INIT_HBASE_SSH_PASS" scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -r "$INIT_HBASE_SSH_USER@$INIT_HBASE_SSH_DNS":/etc/hadoop/conf/* /opt/opencga/conf/hadoop
# same with /etc/hbase/conf, e.g.
sshpass -p "$INIT_HBASE_SSH_PASS" scp -o StrictHostKeyChecking=no  -o UserKnownHostsFile=/dev/null -r "$INIT_HBASE_SSH_USER@$INIT_HBASE_SSH_DNS":/etc/hbase/conf/* /opt/opencga/conf/hadoop

# Copy the OpenCGA installation directory to the hdinsights cluster
# TODO - Optimize this down to only required jars
# Note> This is exported as it is used by the `override-yaml.py` script too
export INIT_HBASE_SSH_REMOTE_OPENCGA_HOME="/home/$INIT_HBASE_SSH_USER/opencga/"
sshpass -p "$INIT_HBASE_SSH_PASS" scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -r /opt/opencga/ "$INIT_HBASE_SSH_USER@$INIT_HBASE_SSH_DNS":"$INIT_HBASE_SSH_REMOTE_OPENCGA_HOME"

echo "Initialising configs"
# Override Yaml configs
python3 /tmp/override-yaml.py --save
# Override Js configs
python3 /tmp/override-js.py --save

# Copies the config files from our local directory into a
# persistent volume to be shared by the other containers.
echo "Initialising volume"
mkdir -p /opt/volume/conf /opt/volume/sessions /opt/volume/variants /opt/volume/ivaconf
cp -r /opt/opencga/conf/* /opt/volume/conf
cp -r /opt/opencga/ivaconf/* /opt/volume/ivaconf

echo "Installing catalog"
echo "${INIT_OPENCGA_PASS}" | /opt/opencga/bin/opencga-admin.sh catalog install --secret-key ${1}

# Catalog install will create a sub-folders in sessions
# that need copying to the volume.
cp -r /opt/opencga/sessions/* /opt/volume/sessions