FROM opencga

RUN  apt-get update && apt-get install sshpass -y

ENV HD_INSIGHTS_SSH_DNS=""

ENV HD_INSIGHTS_SSH_USER=""

ENV HD_INSIGHTS_SSH_USER=""

# copy conf, will be copied into mount location
COPY ./conf /opt/opencga/conf

VOLUME /opt/opencga/conf

COPY ./setup.sh /tmp/

CMD ["sh", "-c", "/tmp/setup.sh"] 
