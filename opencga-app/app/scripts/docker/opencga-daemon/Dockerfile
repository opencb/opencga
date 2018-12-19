FROM opencga

VOLUME /opt/opencga/conf
VOLUME /opt/opencga/sessions

# should pass be in conf not env?
CMD ["sh", "-c", "echo ${OPENCGA_PASS} | /opt/opencga/bin/opencga-admin.sh catalog daemon --start"]
