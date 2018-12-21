FROM opencga

user 1000:1000

VOLUME /opt/opencga/conf
VOLUME /opt/opencga/sessions

USER 1000:1000

# should pass be in conf not env?
CMD ["sh", "-c", "echo ${OPENCGA_PASS} | /opt/opencga/bin/opencga-admin.sh catalog daemon --start"]
