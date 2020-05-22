#!/bin/bash

set -e

# Define variables
APP_DNS_NAME=$1

# Generate self-signed cert
openssl req -x509 -newkey rsa:4096 \
    -keyout key.pem -out cert.pem -days 365 \
    -subj "/C=GB/O=OpenCB/CN=${APP_DNS_NAME}" -nodes

# combine to mongo.pem
cat key.pem cert.pem | tee /etc/ssl/mongo.pem

# set rights for mongo.pem
chmod 600 /etc/ssl/mongo.pem
chown mongodb:mongodb /etc/ssl/mongo.pem

# restart mongo
service mongod restart
