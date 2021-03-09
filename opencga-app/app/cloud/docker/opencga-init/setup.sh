#!/bin/sh

echo "------------ OpenCGA INIT ------------"
/opt/opencga/bin/opencga.sh --version

set -x

if find /opt/opencga/libs/opencga-storage-hadoop-deps-* &> /dev/null ; then
    /opt/opencga/init/setup-hadoop.sh
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
