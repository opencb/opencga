package org.opencb.opencga.catalog.config;

/**
 * Created by pfurio on 04/01/17.
 */
public class ServerConfiguration {

    private RestServerConfiguration rest;
    private GrpcServerConfiguration grpc;

    public ServerConfiguration() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ServerConfiguration{");
        sb.append("rest=").append(rest);
        sb.append(", grpc=").append(grpc);
        sb.append('}');
        return sb.toString();
    }

    public RestServerConfiguration getRest() {
        return rest;
    }

    public ServerConfiguration setRest(RestServerConfiguration rest) {
        this.rest = rest;
        return this;
    }

    public GrpcServerConfiguration getGrpc() {
        return grpc;
    }

    public ServerConfiguration setGrpc(GrpcServerConfiguration grpc) {
        this.grpc = grpc;
        return this;
    }
}
