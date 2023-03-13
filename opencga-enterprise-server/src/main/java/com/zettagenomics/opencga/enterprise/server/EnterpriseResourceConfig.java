package com.zettagenomics.opencga.enterprise.server;

import com.zettagenomics.opencga.enterprise.server.rest.CvaWSServer;
import com.zettagenomics.opencga.enterprise.server.rest.EnterpriseMetaWSServer;
import org.glassfish.jersey.server.ResourceConfig;
import org.opencb.opencga.server.rest.*;
import org.opencb.opencga.server.rest.admin.AdminWSServer;
import org.opencb.opencga.server.rest.analysis.AlignmentWebService;
import org.opencb.opencga.server.rest.analysis.ClinicalWebService;
import org.opencb.opencga.server.rest.analysis.VariantWebService;
import org.opencb.opencga.server.rest.operations.VariantOperationWebService;

import javax.ws.rs.ApplicationPath;
import java.util.LinkedHashMap;
import java.util.Map;

@ApplicationPath("resources")
public class EnterpriseResourceConfig extends ResourceConfig {

    public static final Map<String, Class<?>> enterpriseClasses;

    static {
        enterpriseClasses = new LinkedHashMap<>(20);
        enterpriseClasses.put("USER", UserWSServer.class);
        enterpriseClasses.put("PROJECT", ProjectWSServer.class);
        enterpriseClasses.put("STUDY", StudyWSServer.class);
        enterpriseClasses.put("FILE", FileWSServer.class);
        enterpriseClasses.put("JOB", JobWSServer.class);
        enterpriseClasses.put("SAMPLE", SampleWSServer.class);
        enterpriseClasses.put("INDIVIDUAL", IndividualWSServer.class);
        enterpriseClasses.put("FAMILY", FamilyWSServer.class);
        enterpriseClasses.put("COHORT", CohortWSServer.class);
        enterpriseClasses.put("PANEL", PanelWSServer.class);
        enterpriseClasses.put("ALIGNMENT", AlignmentWebService.class);
        enterpriseClasses.put("VARIANT", VariantWebService.class);
        enterpriseClasses.put("CLINICAL", ClinicalWebService.class);
        enterpriseClasses.put("VARIANT_OPERATION", VariantOperationWebService.class);
        enterpriseClasses.put("META", EnterpriseMetaWSServer.class);
        enterpriseClasses.put("CVA", CvaWSServer.class);
        enterpriseClasses.put("ADMIN", AdminWSServer.class);
    }

    public EnterpriseResourceConfig() {
        for (Class<?> value : enterpriseClasses.values()) {
            register(value);
        }
    }

}
