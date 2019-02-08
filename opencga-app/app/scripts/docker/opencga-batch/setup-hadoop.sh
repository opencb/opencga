#!/bin/sh
set -e 

# Get envvar HADOOP_HOME
. /opt/opencga/conf/hadoop-client/conf/hadoop-env.sh

PATH=$PATH:$HADOOP_HOME/bin

sudo mkdir -p `dirname $HADOOP_HOME`
sudo ln -s /opt/opencga/conf/hadoop-client $HADOOP_HOME