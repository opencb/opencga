package com.zettagenomics.opencga.enterprise.cvdb.tasks;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.tasks.params.CvdbUpdateUsersTaskParams;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrServerException;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.ClinicalAnalysisManager;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.core.models.Acl;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisPermissions;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opencb.commons.datastore.core.QueryOptions.INCLUDE;

@Tool(id = CvdbUpdateUsersTask.ID, resource = Enums.Resource.CLINICAL_ANALYSIS, description = CvdbUpdateUsersTask.DESCRIPTION)
public class CvdbUpdateUsersTask extends OpenCgaToolScopeStudy {
    public static final String ID = "cvdb-users-update";
    public static final String DESCRIPTION = "Update users in CVDB for a list clinical analyses, or for all clinical analyses in a"
            + " study or in a project";

    public static final String NUM_UPDATED_ATTR = "Num. clinical analyses updated";
    public static final String NUM_NOT_UPDATED_ATTR = "Num. clinical analyses not updated";

    private Project project = null;
    private String organizationId = null;

    private int numUpdated = 0;
    private int numTotal = 0;

    private CvdbSolrEngine cvdbEngine;

    @ToolParams
    protected CvdbUpdateUsersTaskParams taskParams = new CvdbUpdateUsersTaskParams();

    @Override
    protected void check() throws Exception {
        super.check();

        JwtPayload jwtPayload = getCatalogManager().getUserManager().validateToken(token);
        CatalogFqn catalogFqn = CatalogFqn.extractFqnFromStudy(getStudyFqn(), jwtPayload);
        organizationId = catalogFqn.getOrganizationId();
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
        cvdbEngine = new CvdbSolrEngine(enterpriseConfiguration.getCvdb(), catalogManager, null);
        try {
            if (!cvdbEngine.existCollections(organizationId, project.getId())) {
                cvdbEngine.createCollections(organizationId, project.getId());
            }
        } catch (CvdbException e) {
            String msg = "Could not perform update users for organization '" + organizationId + "' and project '" + project.getId() + "'";
            logger.error(msg);
            throw new CvdbException(msg);
        }
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            // Start time
            StopWatch stopWatch = StopWatch.createStarted();

            if (taskParams.isAllProject()) {
                // All clinical analyses for the given project
                Query query = new Query();
                QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "fqn");
                OpenCGAResult<Study> studyResults = catalogManager.getStudyManager().search(project.getId(), query, queryOptions, token);
                List<String> studyFqns = studyResults.getResults().stream().map(Study::getFqn).collect(Collectors.toList());
                updateUsersForStudies(studyFqns);
            } else if (CollectionUtils.isNotEmpty(taskParams.getClinicalAnalysisIds())) {
                // All clinical analyses for the input list
                updateUsersForClinicalAnalyses(taskParams.getClinicalAnalysisIds(), getStudyFqn());
            } else {
                // All clinical analyses for the given study
                List<String> studyFqns = Collections.singletonList(getStudyFqn());
                updateUsersForStudies(studyFqns);
            }

            // Stop time
            stopWatch.stop();

            // Add results as attributes
            addAttribute(NUM_UPDATED_ATTR, numUpdated);
            addAttribute(NUM_NOT_UPDATED_ATTR, numTotal - numUpdated);
            addAttribute("Updated time (in sec.)", (int) stopWatch.getTime(TimeUnit.SECONDS));
        });
    }


    private void updateUsersForStudies(List<String> studyFqns) throws CvdbException, CatalogException {
        // Get all clinical analyses for the given study
        Query query = new Query();
        QueryOptions queryOptions = new QueryOptions(INCLUDE, "id");
        ClinicalAnalysisManager clinicalAnalysisManager = catalogManager.getClinicalAnalysisManager();
        for (String studyFqn : studyFqns) {
            OpenCGAResult<ClinicalAnalysis> results = clinicalAnalysisManager.search(studyFqn, query, queryOptions, token);
            List<String> clinicalAnalysisIds = results.getResults().stream().map(ClinicalAnalysis::getId).collect(Collectors.toList());
            updateUsersForClinicalAnalyses(clinicalAnalysisIds, studyFqn);
        }
    }

    private void updateUsersForClinicalAnalyses(List<String> clinicalAnalysisIds, String studyFqn) throws CatalogException, CvdbException {
        numTotal += clinicalAnalysisIds.size();

        OpenCGAResult<Acl> aclResult = catalogManager.getAdminManager().getEffectivePermissions(studyFqn, clinicalAnalysisIds,
                Collections.singletonList(ClinicalAnalysisPermissions.VIEW.name()), Enums.Resource.CLINICAL_ANALYSIS.name(), token);

        // Sanity check
        if (aclResult.getNumResults() != clinicalAnalysisIds.size()) {
            throw new CvdbException("Something wrong happened, could not get all clinical analyses from the input list to update users");
        }

        for (Acl acl : aclResult.getResults()) {
            try {
                // Only one permission (VIEW) has been queried, so the first item has to be taken
                cvdbEngine.indexViewers(acl.getId(), acl.getPermissions().get(0).getUserIds(), organizationId, project.getId());
                numUpdated++;
            } catch (CvdbException | SolrServerException | IOException e) {
                logger.warn("Could not update users for clinical analysis '{}': {}", acl.getId(), e.getMessage());
            }
        }
    }
}
