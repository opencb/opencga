
FROM opencga-build as build
FROM opencga-java-base

# Get opencga
WORKDIR /opt/opencga

COPY --from=build /build/build /opt/opencga