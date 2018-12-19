#!/bin/bash

#this script is wrapped into the mongodb template in base64
#just here for reference

echo $APP_DNS_NAME
sudo apt-get update
sudo apt-get upgrade -y
sudo apt-get install software-properties-common -y
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2930ADAE8CAF5059EE73BB4B58712A2291FA4AD5
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.6 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.6.list
sudo apt-get update
sudo apt-get install -y mongodb-org
sudo systemctl enable mongod.service
sudo add-apt-repository -y universe
sudo add-apt-repository -y ppa:certbot/certbot
sudo apt-get update
sudo apt-get install -y nginx certbot python-certbot-nginx
sleep 150
sudo certbot --nginx -d ${APP_DNS_NAME} -m test@test.com --agree-tos -q
sudo sh -c 'nginx -t && nginx -s reload'
sudo sh -c "cat /etc/letsencrypt/live/${APP_DNS_NAME}/privkey.pem /etc/letsencrypt/live/${APP_DNS_NAME}/cert.pem > /etc/ssl/mongo.pem"
sudo sh -c "cat /etc/letsencrypt/live/${APP_DNS_NAME}/chain.pem >> /etc/ssl/ca.pem"
sudo sed -i -e '/bindIp/ s/: .*/: ::,0.0.0.0\n  ssl:\n    mode: allowSSL\n    PEMKeyFile: \/etc\/ssl\/mongo.pem\n    CAFile: \/etc\/ssl\/ca.pem\n    allowConnectionsWithoutCertificates: true /' /etc/mongod.conf
sudo systemctl restart mongod
sudo reboot
