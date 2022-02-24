#!/bin/sh -x

# Deploy new catalog
sleep 20
echo "Starting REST"
echo "adminOpencga2021." | ./bin/opencga-admin.sh catalog daemon --start

