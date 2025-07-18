package com.zettagenomics.opencga.enterprise.server.generator;

import com.zettagenomics.opencga.enterprise.core.GitUtils;
import com.zettagenomics.opencga.enterprise.server.rest.*;
import org.opencb.opencga.core.common.GitRepositoryState;
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

    /**
     * The order of the classes is important for the order of the categories in Swagger.
     * This is the correct order of categories in Swagger:
     *   1. Federations
     *   2. Organizations
     *   3. Users
     *   4. Projects
     *   5. Studies
     *   6. Files
     *   7. Jobs
     *   8. Workflows
     *   9. Samples
     *   10. Individuals
     *   11. Families
     *   12. Cohorts
     *   13. Disease Panels
     *   14. Analysis - Alignment
     *   15. Analysis - Variant
     *   16. Analysis - Clinical
     *   17. Analysis - CVDB
     *   18. Operations - Variant Storage
     *   19. Meta
     *   20. Admin
     *   21. Ga4gh
     */

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
        classes.add(EnterpriseCvdbWebService.class);
        classes.add(VariantOperationWebService.class);
        classes.add(MetaWSServer.class);
        classes.add(EnterpriseMetaWSServer.class);
        classes.add(AdminWSServer.class);
        classes.add(Ga4ghWSServer.class);

        return classes;
    }

    @Override
    public String getVersion() {
        return "OpenCGA-Enterprise-"+ GitUtils.getEnterprise().getBuildVersion();
    }
}