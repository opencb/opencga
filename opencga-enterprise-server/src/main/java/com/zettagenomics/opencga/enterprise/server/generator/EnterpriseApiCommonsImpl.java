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
import java.util.List;

public class EnterpriseApiCommonsImpl implements ApiCommons {

    public EnterpriseApiCommonsImpl() {
    }

    public List<Class<?>> getApiClasses() {

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
        classes.add(VariantOperationWebService.class);
        classes.add(EnterpriseMetaWSServer.class);
        classes.add(MetaWSServer.class);
        classes.add(Ga4ghWSServer.class);
        classes.add(AdminWSServer.class);
        classes.add(EnterpriseCvdbWebService.class);

        return classes;
    }
}