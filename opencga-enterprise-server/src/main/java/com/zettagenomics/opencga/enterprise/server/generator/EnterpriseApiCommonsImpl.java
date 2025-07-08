package com.zettagenomics.opencga.enterprise.server.generator;

import com.zettagenomics.opencga.enterprise.server.rest.*;
import org.opencb.opencga.server.generator.commons.ApiCommons;
import org.opencb.opencga.server.rest.*;
import org.opencb.opencga.server.rest.admin.AdminWSServer;
import org.opencb.opencga.server.rest.analysis.AlignmentWebService;
import org.opencb.opencga.server.rest.analysis.ClinicalWebService;
import org.opencb.opencga.server.rest.analysis.VariantWebService;
import org.opencb.opencga.server.rest.ga4gh.Ga4ghWSServer;
import org.opencb.opencga.server.rest.operations.VariantOperationWebService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EnterpriseApiCommonsImpl implements ApiCommons {

    public EnterpriseApiCommonsImpl() {
    }

    public List<Class<?>> getApiClasses() {

        //The order of the classes is important for the order of the categories in the swagger.
        List<Class<?>> classes = new ArrayList();
        classes.add(EnterpriseFederationWSServer.class);
        classes.add(OrganizationWSServer.class);
        classes.add(UserWSServer.class);
        classes.add(EnterpriseUserWSServer.class);
        classes.add(ProjectWSServer.class);
        classes.add(StudyWSServer.class);
        classes.add(FileWSServer.class);
        classes.add(JobWSServer.class);
        classes.add(WorkflowWSServer.class);
        classes.add(SampleWSServer.class);
        classes.add(IndividualWSServer.class);
        classes.add(FamilyWSServer.class);
        classes.add(CohortWSServer.class);
        classes.add(PanelWSServer.class);
        classes.add(AlignmentWebService.class);
        classes.add(VariantWebService.class);
        classes.add(ClinicalWebService.class);
        classes.add(EnterpriseClinicalWebService.class);
        classes.add(EnterpriseCvdbWebService.class);
        classes.add(VariantOperationWebService.class);
        classes.add(MetaWSServer.class);
        classes.add(EnterpriseMetaWSServer.class);
        classes.add(AdminWSServer.class);
        classes.add(Ga4ghWSServer.class);

        return classes;
    }

/*
    @Override
    public List<String> getOrderCategories() {
        return Arrays.asList(
                "Federations",
                "Organizations",
                "Users",
                "Projects",
                "Studies",
                "Files",
                "Jobs",
                "Workflows",
                "Samples",
                "Individuals",
                "Families",
                "Cohorts",
                "Disease Panels",
                "Analysis - Alignment",
                "Analysis - Variant",
                "Analysis - Clinical",
                "CVDB",
                "Operations - Variant Storage",
                "Meta",
                "Admin",
                "Ga4gh"
        );
    }*/
}