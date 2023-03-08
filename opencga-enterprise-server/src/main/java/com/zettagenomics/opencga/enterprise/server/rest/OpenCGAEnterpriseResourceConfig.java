package com.zettagenomics.opencga.enterprise.server.rest;

import org.glassfish.jersey.server.ResourceConfig;
import org.opencb.opencga.server.rest.SampleWSServer;

public class OpenCGAEnterpriseResourceConfig extends ResourceConfig {

    public OpenCGAEnterpriseResourceConfig() {
        packages("org.opencb.opencga.server.rest");
        register(SampleWSServer.class);
    }
}
