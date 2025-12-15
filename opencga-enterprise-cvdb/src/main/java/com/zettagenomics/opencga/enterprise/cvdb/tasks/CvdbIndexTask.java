package com.zettagenomics.opencga.enterprise.cvdb.tasks;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.models.CvdbIndexResult;
import com.zettagenomics.opencga.enterprise.cvdb.tasks.params.CvdbIndexTaskParams;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.util.Map;

@Tool(id = CvdbIndexTask.ID, resource = Enums.Resource.CLINICAL_ANALYSIS, description = CvdbIndexTask.DESCRIPTION)
public class CvdbIndexTask extends OpenCgaToolScopeStudy {
    public static final String ID = "cvdb-index-run";
    public static final String DESCRIPTION = "Index clinical analyses of a OpenCGA project, a study or a list of clinical analyses"
            + " into CVDB";

    public static final String NUM_INDEXED_ATTR = "Num. clinical analyses indexed";
    public static final String NUM_NOT_INDEXED_ATTR = "Num. clinical analyses not indexed";

    private Project project = null;
    private CvdbSolrEngine cvdbEngine;

    @ToolParams
    protected CvdbIndexTaskParams params = new CvdbIndexTaskParams();

    @Override
    protected void check() throws Exception {
        super.check();

        JwtPayload jwtPayload = getCatalogManager().getUserManager().validateToken(token);
        CatalogFqn catalogFqn = CatalogFqn.extractFqnFromStudy(getStudyFqn(), jwtPayload);
        String organizationId = catalogFqn.getOrganizationId();
        String userId = jwtPayload.getUserId(organizationId);
        getCatalogManager().getAuthorizationManager().checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);

        // Get study
        Study study = getCatalogManager().getStudyManager().get(getStudyFqn(), QueryOptions.empty(), token).first();

        // Check project
        Query query = new Query(ProjectDBAdaptor.QueryParams.STUDY.key(), study.getFqn());
        project = catalogManager.getProjectManager().search(organizationId, query, QueryOptions.empty(), token).first();
        // Sanity check
        if (project == null) {
            throw new CvdbException("Something wrong happened, could not get project from study '" + study.getFqn() + "'");
        }

        // Get enterprise configuration to set the CVDB engine
        EnterpriseConfiguration enterpriseConfiguration = EnterpriseConfiguration.load(getOpencgaHome());
        cvdbEngine = new CvdbSolrEngine(enterpriseConfiguration.getCvdb(), catalogManager);
        try {
            String collectionPrefix = cvdbEngine.getCollectionNameGenerator().getCollectionPrefix(organizationId, project.getId(), token);
            if (!cvdbEngine.existCollections(collectionPrefix)) {
                cvdbEngine.createCollections(project.getFqn(), collectionPrefix, token);
            }
        } catch (CvdbException e) {
            String msg = "Could not perform CVDB index for organization '" + organizationId + "' and project '" + project.getId() + "'";
            logger.error(msg);
            throw new CvdbException(msg);
        }
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            CvdbIndexResult result;
            if (params.isAllProject()) {
                // All clinical analyses for the given project
                result = cvdbEngine.indexProject(project.getId(), params.isOverwrite(), token);
            } else if (CollectionUtils.isNotEmpty(params.getClinicalAnalysisIds())) {
                // All clinical analyses for the input list
                result = cvdbEngine.indexClinicalAnalyses(params.getClinicalAnalysisIds(), getStudyFqn(), params.isOverwrite(), token);
            } else {
                // All clinical analyses for the given study
                result = cvdbEngine.indexStudy(getStudyFqn(), params.isOverwrite(), token);
            }

            // Check results and add events if needed
            if (MapUtils.isNotEmpty(result.getFailures())) {
                addEvent(Event.Type.ERROR, NUM_NOT_INDEXED_ATTR + ": " + result.getFailures().size());
                if (result.getFailures().size() < 50) {
                    for (Map.Entry<String, String> entry : result.getFailures().entrySet()) {
                        addEvent(Event.Type.WARNING, "Clinical analysis ID '" + entry.getKey() + "' could not be indexed: "
                                + entry.getValue());
                    }
                } else {
                    addEvent(Event.Type.ERROR, "More than 50 clinical analyses could not be indexed. Please check the logs for details.");
                    for (Map.Entry<String, String> entry : result.getFailures().entrySet()) {
                        logger.error("Clinical analysis ID '{}' could not be indexed: {}", entry.getKey(), entry.getValue());
                    }
                }
            }

            // Add results as attributes
            addAttribute(NUM_INDEXED_ATTR, result.getNumIndexed());
            addAttribute(NUM_NOT_INDEXED_ATTR, result.getFailures().size());
            addAttribute("Loading time (in sec.)", result.getTime());
        });
    }
}
