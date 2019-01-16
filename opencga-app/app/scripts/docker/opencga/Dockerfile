FROM opencga-build as build
FROM openjdk:8

WORKDIR /opt/opencga

RUN useradd -u 1001 opencga

COPY --from=build /build/build /opt/opencga