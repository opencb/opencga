package org.opencb.opencga.server.generator;

import junit.framework.TestCase;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.server.rest.SampleWSServer;

@Category(ShortTests.class)
public class JSONManagerTest extends TestCase {

    public void testGetHelp() {
        new RestApiParser().parse(SampleWSServer.class, true);
    }
}