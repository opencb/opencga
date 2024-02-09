package com.zettagenomics.opencga.enterprise.cvdb.tasks;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.models.CvdbIndexResult;
import com.zettagenomics.opencga.enterprise.cvdb.tasks.params.CvdbIndexTaskParams;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.managers.StudyManager;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Tool(id = CvdbIndexTask.ID, resource = Enums.Resource.CLINICAL_ANALYSIS, description = CvdbIndexTask.DESCRIPTION)
public class CvdbIndexTask extends OpenCgaToolScopeStudy {
    public final static String ID = "cvdb-index-run";
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
            if (!cvdbEngine.existCollections(project.getId())) {
                cvdbEngine.createCollections(project.getId());
            }
        } catch (CvdbException e) {
            logger.error("Could not perform CVDB index for project {}", project.getId(), e);
            throw new CvdbException("Could not CVDB index for project '" + project.getId() + "'.");
        }
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            CvdbIndexResult result;
            if (params.isAllProject()) {
                // All clinical analyses for the given project
                result = cvdbEngine.indexProject(project.getId(), getCatalogManager(), params.isOverwrite(), token);
            } else if (CollectionUtils.isNotEmpty(params.getClinicalAnalysisIds())) {
                // All clinical analyses for the input list
                result = cvdbEngine.indexClinicalAnalyses(params.getClinicalAnalysisIds(), getStudyFqn(), getCatalogManager(),
                        params.isOverwrite(), token);
            } else {
                // All clinical analyses for the given study
                result = cvdbEngine.indexStudy(getStudyFqn(), getCatalogManager(), params.isOverwrite(), token);
            }

            // Add results as attributes
            addAttribute(NUM_INDEXED_ATTR, result.getNumIndexed());
            addAttribute(NUM_NOT_INDEXED_ATTR, result.getFailures().size());
            addAttribute("Loading time (in sec.)", result.getTime());

            // Add warnings with the not indexed clinical analyses
//            if (result.getFailures().size() > 0) {
//                for (Map.Entry<String, String> entry : result.getFailures().entrySet()) {
//                    addWarning("Clinical analysis " + entry.getKey() + " could not be indexed: " + entry.getValue());
//                }
//            }
        });
    }
}
