package com.zettagenomics.opencga.enterprise.server;

import com.zettagenomics.opencga.enterprise.server.rest.CvaWSServer;
import com.zettagenomics.opencga.enterprise.server.rest.EnterpriseMetaWSServer;
import org.glassfish.jersey.server.ResourceConfig;
import org.opencb.opencga.server.CORSFilter;
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
        enterpriseClasses = new LinkedHashMap<>(25);
        enterpriseClasses.put("users", UserWSServer.class);
        enterpriseClasses.put("projects", ProjectWSServer.class);
        enterpriseClasses.put("studies", StudyWSServer.class);
        enterpriseClasses.put("files", FileWSServer.class);
        enterpriseClasses.put("jobs", JobWSServer.class);
        enterpriseClasses.put("samples", SampleWSServer.class);
        enterpriseClasses.put("individuals", IndividualWSServer.class);
        enterpriseClasses.put("families", FamilyWSServer.class);
        enterpriseClasses.put("cohorts", CohortWSServer.class);
        enterpriseClasses.put("panels", PanelWSServer.class);
        enterpriseClasses.put("alignment", AlignmentWebService.class);
        enterpriseClasses.put("variant", VariantWebService.class);
        enterpriseClasses.put("clinical", ClinicalWebService.class);
        enterpriseClasses.put("variantOperation", VariantOperationWebService.class);
        enterpriseClasses.put("meta", EnterpriseMetaWSServer.class);
        enterpriseClasses.put("cva", CvaWSServer.class);
        enterpriseClasses.put("admin", AdminWSServer.class);

        // Utils and Filters
        enterpriseClasses.put("paramExceptionMapper", ParamExceptionMapper.class);
        enterpriseClasses.put("OpenCgaApplicationEventListener", OpenCgaApplicationEventListener.class);
    }

    public EnterpriseResourceConfig() {
        for (Class<?> value : enterpriseClasses.values()) {
            register(value);
        }
    }

}
