#!/bin/bash

# ------------ Start MongoDB ----------------
mongod --dbpath /data/opencga/mongodb --replSet rs0  &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start mongoDB: $status"
  exit $status
fi
sleep 10

mongo /opt/scripts/mongo-cluster-init.js
sleep 20

# ------------ Start SOLR ------------------
cat >> /opt/solr/bin/solr.in.sh << EOF
# OpenCGA
SOLR_DATA_HOME=/data/opencga/solr
EOF

/opt/solr/bin/solr start -cloud -force
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start Solr: $status"
  exit $status
fi

/opt/solr/bin/solr status
sleep 2
chmod +x /opt/opencga/misc/solr/install.sh
/opt/opencga/misc/solr/install.sh

CONTAINER_ALREADY_STARTED="CONTAINER_ALREADY_STARTED"
if [ ! -e $CONTAINER_ALREADY_STARTED ] && [ "$installCatalog" != "false" ]; then

    export INIT_SEARCH_HOSTS=http://localhost:8983/solr/
    export INIT_CLINICAL_HOSTS=http://localhost:8983/solr/
    export INIT_CATALOG_DATABASE_HOSTS=localhost:27017
    export INIT_CATALOG_DATABASE_USER=""
    export INIT_CATALOG_DATABASE_PASSWORD=""
    export INIT_CATALOG_DATABASE_SSL="false"
    export INIT_CATALOG_SEARCH_HOSTS=http://localhost:8983/solr/
    export INIT_REST_HOST="http://localhost:9090/`ls ../opencga*.war | rev | cut -d "." -f 2- | rev | xargs basename`"
    export INIT_GRPC_HOST="localhost:9091"
    export INIT_VARIANT_DEFAULT_ENGINE="mongodb"
    export INIT_HADOOP_SSH_DNS=""
    export INIT_HADOOP_SSH_USER=""
    export INIT_HADOOP_SSH_PASS=""
    export INIT_MAX_CONCURRENT_JOBS="1"
    python3 /opt/opencga/init/override_yaml.py --save

    echo "-- Installing Catalog --"
    /opt/opencga/bin/opencga-admin.sh catalog install --secret-key any_string_you_want  <<< demo
    status=$?
        if [ $status -ne 0 ]; then
          echo "Failed to install Catalog : $status"
          exit $status
        fi
    touch $CONTAINER_ALREADY_STARTED
    sleep 5
    echo 'demo' | /opt/opencga/bin/opencga-admin.sh server rest --start &
    status=$?
    if [ $status -ne 0 ]; then
      echo "Failed to start REST server: $status"
      exit $status
    fi
    until curl $INIT_REST_HOST'/webservices/rest/v2/meta/status' &> /dev/null
    do
      echo "Waiting for REST server"
      sleep 1
    done


    if [ "$load" == "true" ]; then
        echo "Creating user for OpenCGA Catalog ....."
        ./opencga-admin.sh users create -u demo --email demo@opencb.com --name "Demo User" --user-password demo <<< demo
        echo "Login user demo ...."
        ./opencga.sh users login -u demo <<< demo
        echo "Loading default template ...."
        ./opencga.sh users template --file /opt/opencga/misc/demo/main.yml --study corpasome
    fi
else
    echo 'demo' | /opt/opencga/bin/opencga-admin.sh server rest --start &
fi

./opencga-admin.sh catalog daemon --start <<< demo

