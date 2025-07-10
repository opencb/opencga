package org.opencb.opencga.server.generator.commons;

import org.opencb.opencga.server.rest.*;
import org.opencb.opencga.server.rest.admin.AdminWSServer;
import org.opencb.opencga.server.rest.analysis.AlignmentWebService;
import org.opencb.opencga.server.rest.analysis.ClinicalWebService;
import org.opencb.opencga.server.rest.analysis.VariantWebService;
import org.opencb.opencga.server.rest.ga4gh.Ga4ghWSServer;
import org.opencb.opencga.server.rest.operations.VariantOperationWebService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ApiCommonsImpl implements ApiCommons {

    /**
     * The order of the classes is important for the order of the categories in Swagger.
     * This is the correct order of categories in Swagger:
     *   1. Organizations
     *   2. Users
     *   3. Projects
     *   4. Studies
     *   5. Files
     *   6. Jobs
     *   7. Workflows
     *   8. Samples
     *   9. Individuals
     *   10. Families
     *   11. Cohorts
     *   12. Disease Panels
     *   13. Analysis - Alignment
     *   14. Analysis - Variant
     *   15. Analysis - Clinical
     *   16. Operations - Variant Storage
     *   17. Meta
     *   18. Admin
     *   19. Ga4gh
     */


    public List<Class<?>> getApiClasses(){

        //The order of the classes is important for the order of the categories in the swagger.
        List<Class<?>> classes = new ArrayList<>();
        classes.add(OrganizationWSServer.class);
        classes.add(UserWSServer.class);
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
        classes.add(VariantOperationWebService.class);
        classes.add(MetaWSServer.class);
        classes.add(AdminWSServer.class);
        classes.add(Ga4ghWSServer.class);

        return classes;
    }

}
