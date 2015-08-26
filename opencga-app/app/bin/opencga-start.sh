#!/bin/bash

PRGDIR=`dirname "$0"`
BASEDIR=`cd "$PRGDIR/.." >/dev/null; pwd`
OPENCGA_DAEMON_BIN=$BASEDIR'/bin/opencga-daemon.sh'

mkdir -p $BASEDIR/log
export OPENCGA_HOME=$BASEDIR

exec $OPENCGA_DAEMON_BIN $@ &>> $BASEDIR/logs/daemon.log &