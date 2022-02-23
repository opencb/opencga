#!/bin/bash -x

# Mongo client is mounted from local system for now.
sleep 3
echo "================================================================================================="
mongo mongodb://mongodb:27017 /opt/mongodb/mongodb-replica-set-init.js

echo "================================================================================================="

# Deploy new catalog
sleep 5
echo "Installing OpenCGA"
echo "adminOpencga2021." | ./bin/opencga-admin.sh catalog install
echo "Creating user for OpenCGA Catalog ....."
echo "adminOpencga2021." | /opt/opencga/bin/opencga-admin.sh users create --email demo@opencga.com -u demo --name Demo --user-password demoOpencga2021.
echo "===================================== OK ====================================================="