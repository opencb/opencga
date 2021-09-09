package org.opencb.opencga.server.json;

import junit.framework.TestCase;
import org.opencb.opencga.server.rest.SampleWSServer;

import java.util.ArrayList;
import java.util.List;

public class JSONManagerTest extends TestCase {

    public void testGetHelp() {
        System.out.println("EMPEZANDO!!!!!! ");
        List<Class> classes = new ArrayList<>();
        classes.add(SampleWSServer.class);
        RestApiParser.getHelp(classes);
    }
}