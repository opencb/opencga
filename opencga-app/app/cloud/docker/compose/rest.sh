#!/bin/sh -x

# Deploy new catalog
sleep 10
echo "Starting REST"
./bin/opencga-admin.sh server rest --start --port 9090

