package org.opencb.opencga.analysis.clinical;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisLoadParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.panel.PanelImportParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.nio.file.Path;
import java.nio.file.Paths;

@Tool(id = ClinicalAnalysisLoadTask.ID, resource = Enums.Resource.DISEASE_PANEL, description = ClinicalAnalysisLoadTask.DESCRIPTION)
public class ClinicalAnalysisLoadTask extends OpenCgaToolScopeStudy {
    public final static String ID = "load";
    public static final String DESCRIPTION = "Load clinical analyses from a file";

    private Path filePath;

    @ToolParams
    protected ClinicalAnalysisLoadParams params = new ClinicalAnalysisLoadParams();

    @Override
    protected void check() throws Exception {
        super.check();

        String fileStr = params.getFile();
        if (StringUtils.isEmpty(fileStr)) {
            throw new ToolException("Missing input file when loading clinical analyses.");
        }

        File file = catalogManager.getFileManager().get(getStudy(), fileStr, FileManager.INCLUDE_FILE_URI_PATH, token).first();
        filePath = Paths.get(file.getUri());
        if (!filePath.toFile().exists()) {
            throw new ToolException("Input file '" + filePath + "' does not exist.");
        }
    }

    @Override
    protected void run() throws Exception {
        step(() -> catalogManager.getClinicalAnalysisManager().load(getStudy(), filePath, token));
    }
}
