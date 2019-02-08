FROM opencga

VOLUME /opt/opencga/conf
VOLUME /opt/opencga/sessions
VOLUME /opt/opencga/variants

# Copy hadoop setup script into the container
COPY ./opencga-app/app/scripts/docker/opencga-batch /tmp/
# This ensures that the script will run every time a new bash session is started
RUN chmod +x /tmp/setup-hadoop.sh && echo ". /tmp/setup-hadoop.sh" >> /etc/bash.bashrc

USER 1001:1001