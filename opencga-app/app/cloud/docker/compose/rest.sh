#!/bin/sh -x

# Deploy new catalog
sleep 15
echo "Starting REST"
./opencga-admin.sh server rest --start --port 9090

