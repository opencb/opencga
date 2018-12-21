#!/bin/bash

#this script is wrapped into the mongodb template in base64
#just here for reference

set -e

echo $APP_DNS_NAME
echo $MONGODB_USERNAME
echo $MONGODB_PASSWORD
apt-get update
apt-get upgrade -y
apt-get install software-properties-common -y
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2930ADAE8CAF5059EE73BB4B58712A2291FA4AD5
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.6 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-3.6.list
apt-get update
apt-get install -y mongodb-org
systemctl enable mongod.service
add-apt-repository -y universe
add-apt-repository -y ppa:certbot/certbot
apt-get update
apt-get install -y nginx certbot python-certbot-nginx
sleep 30
sed -i -e 's/# server_names_hash_bucket_size 64/server_names_hash_bucket_size 128/g' /etc/nginx/nginx.conf
certbot --nginx -d ${APP_DNS_NAME} -m opencga@test.com --agree-tos -q
nginx -t && nginx -s reload
cat /etc/letsencrypt/live/${APP_DNS_NAME}/privkey.pem /etc/letsencrypt/live/${APP_DNS_NAME}/cert.pem > /etc/ssl/mongo.pem
cat /etc/letsencrypt/live/${APP_DNS_NAME}/chain.pem >> /etc/ssl/ca.pem
systemctl restart mongod
sleep 10
mongo admin --eval 'db.createUser({user: "'${MONGODB_USERNAME}'",pwd: "'${MONGODB_PASSWORD}'",roles: ["root"]})'
sed -i '/#security/csecurity:\n  authorization: "enabled"\n' /etc/mongod.conf
sed -i -e '/bindIp/ s/: .*/: ::,0.0.0.0\n  ssl:\n    mode: allowSSL\n    PEMKeyFile: \/etc\/ssl\/mongo.pem\n    CAFile: \/etc\/ssl\/ca.pem\n    allowConnectionsWithoutCertificates: true /' /etc/mongod.conf
systemctl restart mongod
