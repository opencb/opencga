FROM opencga

VOLUME /opt/opencga/conf
VOLUME /opt/opencga/sessions

USER 1001:1001

CMD [". /opt/opencga/conf/opencga-env.sh && echo ${OPENCGA_PASS} | /opt/opencga/bin/opencga-admin.sh catalog daemon --start"]
