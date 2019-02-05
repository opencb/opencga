FROM opencga

VOLUME /opt/opencga/conf
VOLUME /opt/opencga/sessions
VOLUME /opt/opencga/variants

USER 1001:1001

HEALTHCHECK --interval=5m --timeout=3s \
    CMD tail /opt/opencga/logs/daemon.log | grep IndexDaemon:108 > /dev/null && exit 0 || exit 1

CMD ["-c" ,"echo ${OPENCGA_PASS} | /opt/opencga/bin/opencga-admin.sh catalog daemon --start --log-file /opt/opencga/logs/daemon.log"]