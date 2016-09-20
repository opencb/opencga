package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.Study;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Created by hpccoll1 on 12/05/15.
 */
public abstract class AbstractManager {

    protected static Logger logger = LoggerFactory.getLogger(AbstractManager.class);
    protected final AuthorizationManager authorizationManager;
    protected final AuditManager auditManager;
    protected final CatalogIOManagerFactory catalogIOManagerFactory;
    protected final CatalogManager catalogManager;

    protected CatalogConfiguration catalogConfiguration;
    @Deprecated
    protected Properties catalogProperties;

    protected final DBAdaptorFactory catalogDBAdaptorFactory;
    protected final UserDBAdaptor userDBAdaptor;
    protected final ProjectDBAdaptor projectDBAdaptor;
    protected final StudyDBAdaptor studyDBAdaptor;
    protected final FileDBAdaptor fileDBAdaptor;
    protected final IndividualDBAdaptor individualDBAdaptor;
    protected final SampleDBAdaptor sampleDBAdaptor;
    protected final CohortDBAdaptor cohortDBAdaptor;
    protected final DatasetDBAdaptor datasetDBAdaptor;
    protected final JobDBAdaptor jobDBAdaptor;
    protected final PanelDBAdaptor panelDBAdaptor;

    @Deprecated
    public AbstractManager(AuthorizationManager authorizationManager, AuditManager auditManager,
                           DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                           CatalogConfiguration catalogConfiguration) {
        this.authorizationManager = authorizationManager;
        this.auditManager = auditManager;
        this.catalogConfiguration = catalogConfiguration;
        this.userDBAdaptor = catalogDBAdaptorFactory.getCatalogUserDBAdaptor();
        this.studyDBAdaptor = catalogDBAdaptorFactory.getCatalogStudyDBAdaptor();
        this.fileDBAdaptor = catalogDBAdaptorFactory.getCatalogFileDBAdaptor();
        this.individualDBAdaptor = catalogDBAdaptorFactory.getCatalogIndividualDBAdaptor();
        this.sampleDBAdaptor = catalogDBAdaptorFactory.getCatalogSampleDBAdaptor();
        this.jobDBAdaptor = catalogDBAdaptorFactory.getCatalogJobDBAdaptor();
        this.cohortDBAdaptor = catalogDBAdaptorFactory.getCatalogCohortDBAdaptor();
        this.datasetDBAdaptor = catalogDBAdaptorFactory.getCatalogDatasetDBAdaptor();
        this.panelDBAdaptor = catalogDBAdaptorFactory.getCatalogPanelDBAdaptor();
        this.catalogIOManagerFactory = ioManagerFactory;
        this.catalogDBAdaptorFactory = catalogDBAdaptorFactory;
        this.catalogManager = null;

        projectDBAdaptor = catalogDBAdaptorFactory.getCatalogProjectDbAdaptor();
    }

    public AbstractManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                           DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                           CatalogConfiguration catalogConfiguration) {
        this.authorizationManager = authorizationManager;
        this.auditManager = auditManager;
        this.catalogConfiguration = catalogConfiguration;
        this.userDBAdaptor = catalogDBAdaptorFactory.getCatalogUserDBAdaptor();
        this.studyDBAdaptor = catalogDBAdaptorFactory.getCatalogStudyDBAdaptor();
        this.fileDBAdaptor = catalogDBAdaptorFactory.getCatalogFileDBAdaptor();
        this.individualDBAdaptor = catalogDBAdaptorFactory.getCatalogIndividualDBAdaptor();
        this.sampleDBAdaptor = catalogDBAdaptorFactory.getCatalogSampleDBAdaptor();
        this.jobDBAdaptor = catalogDBAdaptorFactory.getCatalogJobDBAdaptor();
        this.cohortDBAdaptor = catalogDBAdaptorFactory.getCatalogCohortDBAdaptor();
        this.datasetDBAdaptor = catalogDBAdaptorFactory.getCatalogDatasetDBAdaptor();
        this.panelDBAdaptor = catalogDBAdaptorFactory.getCatalogPanelDBAdaptor();
        this.catalogIOManagerFactory = ioManagerFactory;
        this.catalogDBAdaptorFactory = catalogDBAdaptorFactory;
        this.catalogManager = catalogManager;

        projectDBAdaptor = catalogDBAdaptorFactory.getCatalogProjectDbAdaptor();
    }

    @Deprecated
    public AbstractManager(AuthorizationManager authorizationManager, AuditManager auditManager,
                           DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                           Properties catalogProperties) {
        this.authorizationManager = authorizationManager;
        this.auditManager = auditManager;
        this.catalogProperties = catalogProperties;
        this.userDBAdaptor = catalogDBAdaptorFactory.getCatalogUserDBAdaptor();
        this.studyDBAdaptor = catalogDBAdaptorFactory.getCatalogStudyDBAdaptor();
        this.fileDBAdaptor = catalogDBAdaptorFactory.getCatalogFileDBAdaptor();
        this.individualDBAdaptor = catalogDBAdaptorFactory.getCatalogIndividualDBAdaptor();
        this.sampleDBAdaptor = catalogDBAdaptorFactory.getCatalogSampleDBAdaptor();
        this.jobDBAdaptor = catalogDBAdaptorFactory.getCatalogJobDBAdaptor();
        this.cohortDBAdaptor = catalogDBAdaptorFactory.getCatalogCohortDBAdaptor();
        this.datasetDBAdaptor = catalogDBAdaptorFactory.getCatalogDatasetDBAdaptor();
        this.panelDBAdaptor = catalogDBAdaptorFactory.getCatalogPanelDBAdaptor();
        this.catalogIOManagerFactory = ioManagerFactory;
        this.catalogDBAdaptorFactory = catalogDBAdaptorFactory;
        this.catalogManager = null;
        projectDBAdaptor = catalogDBAdaptorFactory.getCatalogProjectDbAdaptor();
    }

    /**
     * Prior to the conversion to a numerical featureId, there is a need to know in which user/project/study look for the string.
     * This method calculates those parameters to know how to obtain the numerical id.
     *
     * @param userId User id of the user asking for the id. If no user is found in featureStr, we will assume that it is asking for its
     *               projects/studies...
     * @param featureStr Feature id in string format. Could be one of [user@aliasProject:aliasStudy:XXXXX
     *                | user@aliasStudy:XXXXX | aliasStudy:XXXXX | XXXXX].
     * @return an objectMap with the following possible keys: "user", "project", "study", "featureName"
     */
    protected ObjectMap parseFeatureId(String userId, String featureStr) {
        ObjectMap result = new ObjectMap("user", userId);

        String[] split = featureStr.split("@");
        if (split.length == 2) { // user@project:study
            result.put("user", split[0]);
            featureStr = split[1];
        }

        split = featureStr.split(":", 3);
        if (split.length == 2) {
            result.put("study", split[0]);
            result.put("featureName", split[1]);
        } else if (split.length == 3) {
            result.put("project", split[0]);
            result.put("study", split[1]);
            result.put("featureName", split[2]);
        } else {
            result.put("featureName", featureStr);
        }
        return result;
    }

    /**
     * Retrieve the list of study ids given some options parameters.
     *
     * @param parameters Object map containing the user/project/study where possible.
     * @return a list of study ids.
     * @throws CatalogException when no project or study id could be found.
     */
    protected List<Long> getStudyIds(ObjectMap parameters) throws CatalogException {
        String ownerId = (String) parameters.get("user");
        String aliasProject = (String) parameters.get("project");
        String aliasStudy = (String) parameters.get("study");

        if (aliasStudy != null && StringUtils.isNumeric(aliasStudy)) {
            return Arrays.asList(Long.parseLong(aliasStudy));
        }

        List<Long> projectIds = new ArrayList<>();
        if (aliasProject != null) {
            if (StringUtils.isNumeric(aliasProject)) {
                projectIds = Arrays.asList(Long.parseLong(aliasProject));
            } else {
                long projectId = projectDBAdaptor.getId(ownerId, aliasProject);
                if (projectId == -1) {
                    throw new CatalogException("Error: Could not retrieve any project for the user " + ownerId);
                }
                projectIds.add(projectId);
            }
        } else {
            QueryResult<Project> allProjects = projectDBAdaptor.get(ownerId,
                    new QueryOptions(QueryOptions.INCLUDE, "projects.id"));
            if (allProjects.getNumResults() > 0) {
                projectIds.addAll(allProjects.getResult().stream().map(Project::getId).collect(Collectors.toList()));
            } else {
                throw new CatalogException("Error: Could not retrieve any project for the user " + ownerId);
            }
        }

        Query query = new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectIds);
        if (aliasStudy != null) {
            query.append(StudyDBAdaptor.QueryParams.ALIAS.key(), aliasStudy);
        }
        QueryOptions qOptions = new QueryOptions(QueryOptions.INCLUDE, "projects.studies.id");
        QueryResult<Study> studyQueryResult = studyDBAdaptor.get(query, qOptions);
        List<Long> studyIds = new ArrayList<>();
        if (studyQueryResult.getNumResults() > 0) {
            studyIds.addAll(studyQueryResult.getResult().stream().map(Study::getId).collect(Collectors.toList()));
        } else {
            throw new CatalogException("Error: Could not retrieve any study id for the user " + ownerId);
        }

        return studyIds;
    }

}
