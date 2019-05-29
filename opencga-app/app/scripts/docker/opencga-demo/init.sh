#!/bin/bash

mongod --dbpath /data/opencga/mongodb &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start mongoDB: $status"
  exit $status
fi

sleep 2

/opt/solr-*/bin/solr start -force &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start Solr: $status"
  exit $status
fi

sleep 2

CONTAINER_ALREADY_STARTED="CONTAINER_ALREADY_STARTED_PLACEHOLDER"
if [ ! -e $CONTAINER_ALREADY_STARTED ]; then
    touch $CONTAINER_ALREADY_STARTED
    echo "-- Installing Catalog --"
    /opt/opencga/bin/opencga-admin.sh catalog install --secret-key any_string_you_want  <<< admin_P@ssword
    status=$?
        if [ $status -ne 0 ]; then
          echo "Failed to install Catalog : $status"
          exit $status
        fi
fi

sleep 5

/opt/opencga/bin/opencga-admin.sh server rest --start <<< admin_P@ssword  

status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start REST server: $status"
  exit $status
fi
