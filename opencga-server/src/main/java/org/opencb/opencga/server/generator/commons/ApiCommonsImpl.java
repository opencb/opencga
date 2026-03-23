package org.opencb.opencga.server.generator.commons;

import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.server.rest.*;
import org.opencb.opencga.server.rest.admin.AdminWSServer;
import org.opencb.opencga.server.rest.analysis.AlignmentWebService;
import org.opencb.opencga.server.rest.analysis.ClinicalWebService;
import org.opencb.opencga.server.rest.analysis.VariantWebService;
import org.opencb.opencga.server.rest.ga4gh.Ga4ghWSServer;
import org.opencb.opencga.server.rest.operations.VariantOperationWebService;
import org.opencb.opencga.server.rest.CvdbWSServer;
import org.opencb.opencga.server.rest.FederationWSServer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ApiCommonsImpl implements ApiCommons {

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
     *   9. User tools
     *   10. Samples
     *   11. Individuals
     *   12. Families
     *   13. Cohorts
     *   14. Disease Panels
     *   15. Analysis - Alignment
     *   16. Analysis - Variant
     *   17. Analysis - Clinical
     *   18. Analysis - CVDB
     *   19. Operations - Variant Storage
     *   20. Meta
     *   21. Admin
     *   22. Ga4gh
     */


    public List<Class<?>> getApiClasses(){

        //The order of the classes is important for the order of the categories in the swagger.
        List<Class<?>> classes = new ArrayList<>();
        classes.add(FederationWSServer.class);
        classes.add(OrganizationWSServer.class);
        classes.add(UserWSServer.class);
        classes.add(ProjectWSServer.class);
        classes.add(StudyWSServer.class);
        classes.add(FileWSServer.class);
        classes.add(JobWSServer.class);
        classes.add(WorkflowWSServer.class);
        classes.add(ExternalToolWSServer.class);
        classes.add(SampleWSServer.class);
        classes.add(IndividualWSServer.class);
        classes.add(FamilyWSServer.class);
        classes.add(CohortWSServer.class);
        classes.add(PanelWSServer.class);
        classes.add(AlignmentWebService.class);
        classes.add(VariantWebService.class);
        classes.add(ClinicalWebService.class);
        classes.add(CvdbWSServer.class);
        classes.add(VariantOperationWebService.class);
        classes.add(MetaWSServer.class);
        classes.add(AdminWSServer.class);
        classes.add(Ga4ghWSServer.class);

        return classes;
    }

    @Override
    public String getVersion() {
        return "OpenCGA-"+GitRepositoryState.getInstance().getBuildVersion();
    }

}
