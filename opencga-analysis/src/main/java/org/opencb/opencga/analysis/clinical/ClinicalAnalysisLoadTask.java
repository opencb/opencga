package org.opencb.opencga.analysis.clinical;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.models.ClinicalAnalysisLoadResult;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisLoadParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

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
            throw new ToolException("Input file '" + fileStr + "' does not exist: " + filePath);
        }
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            ClinicalAnalysisLoadResult loadResult = catalogManager.getClinicalAnalysisManager().load(getStudy(), filePath, token);

            // Add results as attributes
            addAttribute("Num. clinical analyses loaded", loadResult.getNumLoaded());
            addAttribute("Num. clinical analyses not loaded", loadResult.getFailures().size());
            addAttribute("Loading time (in sec.)", loadResult.getTime());
            addAttribute("Clinical analyses file name", loadResult.getFilename());

            // Add warnings with the not loaded clinical analysis
            if (loadResult.getFailures().size() > 0) {
                for (Map.Entry<String, String> entry : loadResult.getFailures().entrySet()) {
                    addWarning("Clinical analysis " + entry.getKey() + " could not be loaded due to error: " + entry.getValue());
                }
            }
        });
    }
}
