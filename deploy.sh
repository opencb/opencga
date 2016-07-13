#!/bin/bash

mvn clean install -DskipTests -Dopencga.war.name=opencga
rm /opt/opencga/libs/*
cp -r build/* /opt/opencga/
cp build/opencga.war /opt/tomcat/webapps/
