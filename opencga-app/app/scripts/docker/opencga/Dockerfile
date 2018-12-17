
FROM opencga-build as build


FROM opencga-java-base

RUN  apt-get update && apt-get install sshpass -y

# Get opencga
WORKDIR /opt/opencga 

COPY --from=build /build/build /opt/opencga

# extract
#RUN   wget --quiet ${OPENCGA_RELEASE_URL} && mkdir /opt/opencga && tar xvf opencga-${OPENCGA_VERSION}.tar.gz --strip-components=1 -C /opt/opencga  && rm ./opencga-${OPENCGA_VERSION}.tar.gz

COPY ./config.sh /tmp/

