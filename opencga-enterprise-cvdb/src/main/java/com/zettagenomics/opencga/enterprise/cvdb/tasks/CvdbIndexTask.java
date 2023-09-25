package com.zettagenomics.opencga.enterprise.cvdb.tasks;

import com.zettagenomics.opencga.enterprise.core.configuration.CvdbConfiguration;
import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.tasks.params.CvdbIndexTaskParams;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.solr.SolrManager;
import org.opencb.opencga.analysis.rga.exceptions.RgaException;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.models.ClinicalAnalysisLoadResult;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

@Tool(id = CvdbIndexTask.ID, resource = Enums.Resource.CLINICAL, description = CvdbIndexTask.DESCRIPTION)
public class CvdbIndexTask extends OpenCgaToolScopeStudy {
    public final static String ID = "index";
    public static final String DESCRIPTION = "Index clinical analyses into CVDB";

    private Project project;
    private CvdbSolrEngine cvdbEngine;

    @ToolParams
    protected CvdbIndexTaskParams params = new CvdbIndexTaskParams();

    @Override
    protected void check() throws Exception {
        super.check();

        // Check project
        String projectStr = params.getProject();
        if (StringUtils.isEmpty(projectStr)) {
            throw new ToolException("Missing project when indexing clinical analyses.");
        }
        project = catalogManager.getProjectManager().get(projectStr, QueryOptions.empty(), token).first();

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
            cvdbEngine.index(project.getId(),getCatalogManager(), token);
        });
    }
}
