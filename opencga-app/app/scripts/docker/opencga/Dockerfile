
FROM opencga-build as build
FROM opencga-java-base

# Get opencga
WORKDIR /opt/opencga

COPY --from=build /build/build /opt/opencga

# from repo
#RUN   wget --quiet ${OPENCGA_RELEASE_URL} && mkdir /opt/opencga && tar xvf opencga-${OPENCGA_VERSION}.tar.gz --strip-components=1 -C /opt/opencga  && rm ./opencga-${OPENCGA_VERSION}.tar.gz

