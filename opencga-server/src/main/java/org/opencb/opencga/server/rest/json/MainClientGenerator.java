package org.opencb.opencga.server.rest.json;

import org.opencb.opencga.server.rest.SampleWSServer;

import java.util.ArrayList;
import java.util.List;

public class MainClientGenerator {

    public static void main(String[] args) {
        System.out.println("EMPEZANDO!!!!!! ");
        List<Class> classes = new ArrayList<>();
        classes.add(SampleWSServer.class);
        JSONManager.getHelp(classes);
    }
}
