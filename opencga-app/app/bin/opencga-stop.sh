#!/usr/bin/env bash

PRGDIR=`dirname "$0"`
BASEDIR=`cd "$PRGDIR/.." >/dev/null; pwd`
daemon_port=`grep "OPENCGA.APP.DAEMON.PORT" ${BASEDIR}/conf/daemon.properties | cut -d "=" -f2`

curl "http://localhost:${daemon_port}/opencga/rest/admin/stop"

echo ""