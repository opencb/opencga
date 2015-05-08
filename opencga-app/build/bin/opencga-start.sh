#!/bin/bash

PRGDIR=`dirname "$0"`
BASEDIR=`cd "$PRGDIR/.." >/dev/null; pwd`
OPENCGA_DAEMON_BIN=$BASEDIR'/bin/opencga-daemon.sh'

mkdir $BASEDIR/log
export OPENCGA_HOME=$BASEDIR

$OPENCGA_DAEMON_BIN $@ &>> $BASEDIR/log/daemon.log &



