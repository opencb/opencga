FROM opencga

VOLUME /opt/opencga/conf
VOLUME /opt/opencga/sessions
VOLUME /opt/opencga/variants

USER 1001:1001

ENTRYPOINT [ "/bin/bash","-c" ]

CMD ["echo ${OPENCGA_PASS} | /opt/opencga/bin/opencga-admin.sh catalog daemon --start"]