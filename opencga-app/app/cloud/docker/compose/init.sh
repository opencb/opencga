#!/bin/sh -x

# Mongo client is mounted from local system for now.
sleep 3
echo "================================================================================================="
mongo mongodb://mongodb:27017 /opt/mongodb/mongodb-replica-set-init.js

echo "================================================================================================="

# Deploy new catalog
sleep 5
echo "Installing OpenCGA"
echo "adminOpencga2021." | ./opencga-admin.sh catalog install

echo "===================================== OK ====================================================="