FROM opencga


COPY ./setup.sh /tmp/
 
CMD ["sh", "-c", "/tmp/config.sh && /tmp/setup.sh ${HD_INSIGHTS_SSH_PASS} &&  echo ${HD_INSIGHTS_SSH_PASS} | /opt/opencga/bin/opencga-admin.sh catalog daemon --start"] 
