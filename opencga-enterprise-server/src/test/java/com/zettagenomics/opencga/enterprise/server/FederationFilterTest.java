package com.zettagenomics.opencga.enterprise.server;

import org.junit.Test;

import java.nio.file.Paths;

public class FederationFilterTest  {

    @Test
    public void runServerTest() throws Exception {
        EnterpriseRestServer server = new EnterpriseRestServer(Paths.get("/opt/opencga"), 9090);
        server.start();
        server.blockUntilShutdown();
    }

}