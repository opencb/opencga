#!/bin/sh

echo "------------ OpenCGA INIT ------------"
/opt/opencga/bin/opencga.sh --version

set -x

FILE=/opt/volume/conf/hadoop
if [ -d "$FILE" ]; then
    echo "$FILE already exists"
    echo "Copy jar-with-dependencies to hadoop"
    sshpass -p "$INIT_HADOOP_SSH_PASS" scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -r /opt/opencga/*.jar "$INIT_HADOOP_SSH_USER@$INIT_HADOOP_SSH_DNS":"$INIT_HADOOP_SSH_REMOTE_OPENCGA_HOME"
else
    sshpass -p "$INIT_HADOOP_SSH_PASS" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "$INIT_HADOOP_SSH_USER@$INIT_HADOOP_SSH_DNS" "sudo sed -i '/<name>hbase.client.keyvalue.maxsize<\/name>/!b;n;c<value>0</value>' /etc/hbase/conf/hbase-site.xml"

    # copy conf files from hdinsight cluster (from /etc/hadoop/conf & /etc/hbase/conf) to opencga VM
    # place these files in /opt/opencga/conf/hadoop, by e.g.: (todo: change connection strings)
    echo "Fetching HDInsight configuration"
    sshpass -p "$INIT_HADOOP_SSH_PASS" scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -r "$INIT_HADOOP_SSH_USER@$INIT_HADOOP_SSH_DNS":/etc/hadoop/conf/* /opt/opencga/conf/hadoop
    # same with /etc/hbase/conf, e.g.
    sshpass -p "$INIT_HADOOP_SSH_PASS" scp -o StrictHostKeyChecking=no  -o UserKnownHostsFile=/dev/null -r "$INIT_HADOOP_SSH_USER@$INIT_HADOOP_SSH_DNS":/etc/hbase/conf/* /opt/opencga/conf/hadoop

    # Copy the OpenCGA installation directory to the hdinsights cluster
    # TODO - Optimize this down to only required jars
    sshpass -p "$INIT_HADOOP_SSH_PASS" scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -r /opt/opencga/ "$INIT_HADOOP_SSH_USER@$INIT_HADOOP_SSH_DNS":"$INIT_HADOOP_SSH_REMOTE_OPENCGA_HOME"

    mkdir -p /opt/volume/conf/hadoop
    cp -r /opt/opencga/conf/hadoop/* /opt/volume/conf/hadoop
fi

FILE=/opt/volume/conf/configuration.yml
if [ -f "$FILE" ]; then
    echo "$FILE already exists"
else
    echo "Copying default configs"
    cp -r -L -v /opt/opencga/default-conf/*  /opt/opencga/conf/

    echo "Initialising configs"
    # Override Yaml configs
    python3 /opt/opencga/init/override_yaml.py --save

    # Copies the config files from our local directory into a
    # persistent volume to be shared by the other containers.
    echo "Initialising volume"
    #mkdir -p /opt/volume/conf /opt/volume/variants
    cp -r /opt/opencga/conf/* /opt/volume/conf

    cp -r /opt/opencga/analysis/* /opt/volume/analysis
fi
# wait for mongodb
echo "About to wait for MongoDB"
until mongo mongodb://$(echo $INIT_CATALOG_DATABASE_HOSTS | cut -d',' -f1)/\?replicaSet=rs0  \
      -u $INIT_CATALOG_DATABASE_USER -p $INIT_CATALOG_DATABASE_PASSWORD --authenticationDatabase admin \
      --ssl --sslAllowInvalidHostnames --sslAllowInvalidCertificates --quiet \
      --eval 'db.getMongo().getDBNames().indexOf("admin")'
do
    echo "Waiting for Mongo DB"
done

DB_EXISTS=$(mongo mongodb://$(echo $INIT_CATALOG_DATABASE_HOSTS | cut -d',' -f1)/\?replicaSet=rs0  \
      -u $INIT_CATALOG_DATABASE_USER -p $INIT_CATALOG_DATABASE_PASSWORD --authenticationDatabase admin \
      --ssl --sslAllowInvalidHostnames --sslAllowInvalidCertificates --quiet \
      --eval 'db.getMongo().getDBNames().indexOf("opencga_catalog")' | tail -1)
echo "DB EXISTS: $DB_EXISTS"

if [ $(($DB_EXISTS)) == -1 ]; then

    echo "Installing catalog"
    echo "${INIT_OPENCGA_PASS}" | /opt/opencga/bin/opencga-admin.sh catalog install
    echo "catalog installed"
    echo "copying session files"
    cp -r /opt/opencga/sessions/* /opt/volume/sessions
     echo "copied session files"
else
    echo "DB opencga_catalog already exists"
fi
