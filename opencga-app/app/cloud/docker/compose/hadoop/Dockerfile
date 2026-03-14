FROM eclipse-temurin:11.0.24_8-jre-jammy

ENV HBASE_VERSION=2.5.10
ENV PHOENIX_VERSION=5.2.0
ENV HBASE_HOME=/opt/hbase

# Install HBase
RUN curl -fsSL https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz \
    | tar xz -C /opt && mv /opt/hbase-${HBASE_VERSION} ${HBASE_HOME}

# Install Phoenix server JAR (co-processors for HBase)
RUN curl -fsSL https://archive.apache.org/dist/phoenix/phoenix-${PHOENIX_VERSION}/phoenix-hbase-2.5-${PHOENIX_VERSION}-bin.tar.gz \
    | tar xz -C /tmp \
    && cp /tmp/phoenix-hbase-2.5-${PHOENIX_VERSION}-bin/phoenix-server-hbase-2.5-${PHOENIX_VERSION}.jar ${HBASE_HOME}/lib/ \
    && rm -rf /tmp/phoenix-*

# Copy HBase config
COPY hbase-site.xml ${HBASE_HOME}/conf/hbase-site.xml

ENV PATH=${HBASE_HOME}/bin:${PATH}

EXPOSE 2181 16000 16010 16020 16030

ENTRYPOINT ["/opt/hbase/bin/hbase"]
CMD ["master", "start"]
