package com.zettagenomics.opencga.enterprise.server;

import com.zettagenomics.opencga.enterprise.server.rest.CvaWSServer;
import com.zettagenomics.opencga.enterprise.server.rest.EnterpriseMetaWSServer;
import org.glassfish.jersey.server.ResourceConfig;
import org.opencb.opencga.server.rest.IndividualWSServer;
import org.opencb.opencga.server.rest.SampleWSServer;

import javax.ws.rs.ApplicationPath;
import java.util.LinkedHashMap;
import java.util.Map;

@ApplicationPath("resources")
public class EnterpriseResourceConfig extends ResourceConfig {

    public static final Map<String, Class> enterpriseClasses;

    static {
        enterpriseClasses = new LinkedHashMap<>(20);
        enterpriseClasses.put("SAMPLES", SampleWSServer.class);
        enterpriseClasses.put("INDIVIDUALS", IndividualWSServer.class);
        enterpriseClasses.put("CVA", CvaWSServer.class);
        enterpriseClasses.put("META", EnterpriseMetaWSServer.class);
    }

    public EnterpriseResourceConfig() {
//        packages("org.opencb.opencga.server.rest");
//        register(SampleWSServer.class);
//        register(IndividualWSServer.class);
//        register(CvaWSServer.class);
//        register(EnterpriseMetaWSServer.class);

        for (Class value : enterpriseClasses.values()) {
            register(value);
        }
    }

}
