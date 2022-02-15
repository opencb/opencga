#!/bin/sh -x

# Deploy new catalog
sleep 10
echo "Installing OpenCGA"
echo "adminOpencga2021." | ./opencga-admin.sh catalog install --force

