FROM opencga

# Mount the volume at arbitary location and then
# symlink expected location to the shared volume.
VOLUME /opt/volume
RUN mkdir -p /opt/opencga/ && \
    ln -sf /opt/volume/conf /opt/opencga/conf && \
    ln -sf /opt/volume/sessions /opt/opencga/sessions

# should pass be in conf not env?
CMD ["sh", "-c", "echo ${OPENCGA_PASS} | /opt/opencga/bin/opencga-admin.sh catalog daemon --start"]
