package org.opencb.opencga.server.generator;

import junit.framework.TestCase;
import org.opencb.opencga.server.rest.SampleWSServer;

public class JSONManagerTest extends TestCase {

    public void testGetHelp() {
        new RestApiParser().parse(SampleWSServer.class, true);
    }
}