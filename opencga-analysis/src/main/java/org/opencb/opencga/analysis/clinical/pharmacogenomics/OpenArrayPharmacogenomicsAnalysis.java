package org.opencb.opencga.analysis.clinical.pharmacogenomics;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.OpenArrayPharmacogenomicsAnalysisParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

/**
 * OpenArray pharmacogenomics analysis tool.
 * Executes the Python pharmacogenomics CLI via Docker to infer star alleles
 * from ThermoFisher OpenArray genotyping data.
 */
@Tool(id = OpenArrayPharmacogenomicsAnalysis.ID, resource = Enums.Resource.CLINICAL_ANALYSIS,
        description = OpenArrayPharmacogenomicsAnalysis.DESCRIPTION)
public class OpenArrayPharmacogenomicsAnalysis extends OpenCgaTool {

    public static final String ID = "openarray-pharmacogenomics";
    public static final String DESCRIPTION = "OpenArray pharmacogenomics analysis: infer star alleles from "
            + "ThermoFisher OpenArray genotyping data and optionally annotate with CPIC";

    @ToolParams
    protected final OpenArrayPharmacogenomicsAnalysisParams analysisParams = new OpenArrayPharmacogenomicsAnalysisParams();

    // Resolved physical file paths
    private String snvFilePath;
    private String translationFilePath;
    private String cnvFilePath;
    private String renameFilePath;
    private String compareToPath;

    @Override
    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(study)) {
            throw new ToolException("Missing study");
        }

        FileManager fileManager = catalogManager.getFileManager();

        // Resolve required files from catalog
        if (StringUtils.isEmpty(analysisParams.getSnvFile())) {
            throw new ToolException("Missing required parameter: snvFile");
        }
        snvFilePath = AnalysisUtils.getCatalogFile(analysisParams.getSnvFile(), study, fileManager, token)
                .getUri().getPath();

        if (StringUtils.isEmpty(analysisParams.getTranslationFile())) {
            throw new ToolException("Missing required parameter: translationFile");
        }
        translationFilePath = AnalysisUtils.getCatalogFile(analysisParams.getTranslationFile(), study, fileManager, token)
                .getUri().getPath();

        // Resolve optional files
        if (StringUtils.isNotEmpty(analysisParams.getCnvFile())) {
            cnvFilePath = AnalysisUtils.getCatalogFile(analysisParams.getCnvFile(), study, fileManager, token)
                    .getUri().getPath();
        }

        if (StringUtils.isNotEmpty(analysisParams.getRenameFile())) {
            renameFilePath = AnalysisUtils.getCatalogFile(analysisParams.getRenameFile(), study, fileManager, token)
                    .getUri().getPath();
        }

        if (StringUtils.isNotEmpty(analysisParams.getCompareTo())) {
            compareToPath = AnalysisUtils.getCatalogFile(analysisParams.getCompareTo(), study, fileManager, token)
                    .getUri().getPath();
        }

        setUpStorageEngineExecutor(study);
    }

    @Override
    protected void run() throws ToolException {
        step(ID, () -> {
            OpenArrayPharmacogenomicsAnalysisExecutor executor =
                    getToolExecutor(OpenArrayPharmacogenomicsAnalysisExecutor.class);

            executor.setSnvFilePath(snvFilePath)
                    .setTranslationFilePath(translationFilePath)
                    .setCnvFilePath(cnvFilePath)
                    .setRenameFilePath(renameFilePath)
                    .setCompareToPath(compareToPath)
                    .setAnnotate(Boolean.TRUE.equals(analysisParams.getAnnotate()))
                    .execute();
        });
    }
}
