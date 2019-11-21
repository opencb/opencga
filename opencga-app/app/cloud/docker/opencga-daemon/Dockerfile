FROM opencga

VOLUME /opt/opencga/conf
VOLUME /opt/opencga/sessions
VOLUME /opt/opencga/variants

USER 1001:1001

HEALTHCHECK --interval=5m --timeout=3s \
    CMD tail -n 200 /tmp/daemon.log | grep IndexDaemon: > /dev/null || killall java

CMD ["-c" ,"echo ${OPENCGA_PASS} | /opt/opencga/bin/opencga-admin.sh catalog daemon --start --log-file /tmp/daemon.log"]