#!/bin/sh -x

# Mongo client is mounted from local system for now.
/usr/bin/mongo /opt/opencga/conf/mongodb-replica-set-init.js

# Deploy new catalog
sleep 10
echo "Installing OpenCGA"
echo "adminOpencga2021." | ./opencga-admin.sh catalog install --force

