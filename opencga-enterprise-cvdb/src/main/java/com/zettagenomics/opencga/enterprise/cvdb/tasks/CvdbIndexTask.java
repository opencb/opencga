package com.zettagenomics.opencga.enterprise.cvdb.tasks;

import com.zettagenomics.opencga.enterprise.core.configuration.CvdbConfiguration;
import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.models.CvdbIndexResult;
import com.zettagenomics.opencga.enterprise.cvdb.tasks.params.CvdbIndexTaskParams;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.solr.SolrManager;
import org.opencb.opencga.analysis.rga.exceptions.RgaException;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.models.ClinicalAnalysisLoadResult;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Tool(id = CvdbIndexTask.ID, resource = Enums.Resource.CLINICAL_ANALYSIS, description = CvdbIndexTask.DESCRIPTION)
public class CvdbIndexTask extends OpenCgaTool {
    public final static String ID = "cvdb-index-run";
    public static final String DESCRIPTION = "Index clinical analyses of a OpenCGA project, a study or a list of clinical analyses"
            + " into CVDB";

    public static final String NUM_INDEXED_ATTR = "Num. clinical analyses indexed";
    public static final String NUM_NOT_INDEXED_ATTR = "Num. clinical analyses not indexed";

    private Project project = null;
    private String studyFqn = null;
    private List<String> clinicalAnalysisIds = new ArrayList<>();

    private CvdbSolrEngine cvdbEngine;

    @ToolParams
    protected CvdbIndexTaskParams params = new CvdbIndexTaskParams();

    @Override
    protected void check() throws Exception {
        super.check();

        // Check project (mandatory parameter)
        String projectId = params.getProjectId();
        if (StringUtils.isEmpty(projectId)) {
            throw new ToolException("Missing project ID.");
        }
        project = catalogManager.getProjectManager().get(projectId, QueryOptions.empty(), token).first();

        // Check study (only mandatory parameter if clinical analysis IDs are provided)
        String studyId = params.getStudyId();
        if (StringUtils.isNotEmpty(studyId)) {
            Query query = new Query(StudyDBAdaptor.QueryParams.ID.key(), studyId);
            Study study = catalogManager.getStudyManager().search(projectId, query, QueryOptions.empty(), token).first();
            if (study == null) {
                throw new ToolException("Study '" + studyId + "' not found in project '" + projectId + "'");
            }
            studyFqn = study.getFqn();
        }

        // Check clinical analyses
        clinicalAnalysisIds = params.getClinicalAnalysisIds();
        if (studyFqn == null && CollectionUtils.isNotEmpty(clinicalAnalysisIds)) {
            throw new ToolException("Missing study: when providing a list of clinical analyses, it is mandatory to specify the study to"
                    + " which they belong");
        }

        // Get enterprise configuration to set the CVDB engine
        EnterpriseConfiguration enterpriseConfiguration = EnterpriseConfiguration.load(getOpencgaHome());
        cvdbEngine = new CvdbSolrEngine(enterpriseConfiguration.getCvdb(), null);
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
            CvdbIndexResult result = cvdbEngine.index(project.getId(), studyFqn, clinicalAnalysisIds, getCatalogManager(),
                    params.isOverwrite(), token);

            // Add results as attributes
            addAttribute(NUM_INDEXED_ATTR, result.getNumIndexed());
            addAttribute(NUM_NOT_INDEXED_ATTR, result.getFailures().size());
            addAttribute("Loading time (in sec.)", result.getTime());

            // Add warnings with the not indexed clinical analyses
            if (result.getFailures().size() > 0) {
                for (Map.Entry<String, String> entry : result.getFailures().entrySet()) {
                    addWarning("Clinical analysis " + entry.getKey() + " could not be indexed: " + entry.getValue());
                }
            }
        });
    }
}
