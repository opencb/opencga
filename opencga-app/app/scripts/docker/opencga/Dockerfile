
FROM opencga-build as build
FROM opencga-java-base

# Get opencga
WORKDIR /opt/opencga

RUN useradd -u 1001 opencga

COPY --from=build /build/build /opt/opencga