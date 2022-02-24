#!/bin/bash -x

# Deploy new catalog
sleep 10
echo "Starting REST"
/opt/opencga/bin/opencga-admin.sh server rest --start --port 9090

