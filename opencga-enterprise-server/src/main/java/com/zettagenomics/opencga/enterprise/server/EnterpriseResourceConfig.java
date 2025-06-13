package com.zettagenomics.opencga.enterprise.server;

import com.zettagenomics.opencga.enterprise.server.rest.*;
import org.glassfish.jersey.server.ResourceConfig;
import org.opencb.opencga.server.rest.*;
import org.opencb.opencga.server.rest.admin.AdminWSServer;
import org.opencb.opencga.server.rest.analysis.AlignmentWebService;
import org.opencb.opencga.server.rest.analysis.VariantWebService;
import org.opencb.opencga.server.rest.fileupload.FileUploadServlet;
import org.opencb.opencga.server.rest.operations.VariantOperationWebService;
import org.opencb.opencga.server.rest.utils.FileRangesWSServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ApplicationPath;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

@ApplicationPath("resources")
public class EnterpriseResourceConfig extends ResourceConfig {

    private static final Logger logger;
    public static final Map<String, Class<?>> enterpriseApiClasses;
    private static final Set<Class<?>> auxiliarClasses;

    static {
        logger = LoggerFactory.getLogger(EnterpriseResourceConfig.class);

        enterpriseApiClasses = new LinkedHashMap<>(26);
        enterpriseApiClasses.put("federations", EnterpriseFederationWSServer.class);
        enterpriseApiClasses.put("organizations", OrganizationWSServer.class);
        enterpriseApiClasses.put("users", EnterpriseUserWSServer.class);
        enterpriseApiClasses.put("projects", ProjectWSServer.class);
        enterpriseApiClasses.put("studies", StudyWSServer.class);
        enterpriseApiClasses.put("files", FileWSServer.class);
        enterpriseApiClasses.put("jobs", JobWSServer.class);
        enterpriseApiClasses.put("workflows", WorkflowWSServer.class);
        enterpriseApiClasses.put("samples", SampleWSServer.class);
        enterpriseApiClasses.put("individuals", IndividualWSServer.class);
        enterpriseApiClasses.put("families", FamilyWSServer.class);
        enterpriseApiClasses.put("cohorts", CohortWSServer.class);
        enterpriseApiClasses.put("panels", PanelWSServer.class);
        enterpriseApiClasses.put("alignment", AlignmentWebService.class);
        enterpriseApiClasses.put("variant", VariantWebService.class);
        enterpriseApiClasses.put("variantOperation", VariantOperationWebService.class);
        enterpriseApiClasses.put("meta", EnterpriseMetaWSServer.class);
        enterpriseApiClasses.put("clinical", EnterpriseClinicalWebService.class);
        enterpriseApiClasses.put("cvdb", EnterpriseCvdbWebService.class);
        enterpriseApiClasses.put("admin", AdminWSServer.class);

        // Utils, Filters and hidden API classes
        auxiliarClasses = new HashSet<>(10);
        auxiliarClasses.add(FileUploadServlet.class);
        auxiliarClasses.add(FileRangesWSServer.class);
        auxiliarClasses.add(TestWSServer.class);

        auxiliarClasses.add(ParamExceptionMapper.class);
        auxiliarClasses.add(OpenCgaApplicationEventListener.class);
    }

    public EnterpriseResourceConfig() {
        for (Class<?> value : enterpriseApiClasses.values()) {
            logger.info("Loading '{}' API class", value.getName());
            register(value);
        }
        for (Class<?> value : auxiliarClasses) {
            logger.info("Loading '{}' auxiliar class", value.getName());
            register(value);
        }
    }

}
