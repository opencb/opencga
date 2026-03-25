package org.opencb.opencga.analysis.clinical.pharmacogenomics;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.nio.file.Path;

/**
 * Executor for OpenArray pharmacogenomics analysis.
 * Runs the Python pharmacogenomics CLI locally (already inside opencga-base).
 */
@ToolExecutor(id = OpenArrayPharmacogenomicsAnalysisExecutor.ID,
        tool = OpenArrayPharmacogenomicsAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class OpenArrayPharmacogenomicsAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public static final String ID = OpenArrayPharmacogenomicsAnalysis.ID + "-local";

    private static final String VENV_BIN = ".venv/bin/pharmacogenomics";

    private Path opencgaHome;
    private String snvFilePath;
    private String translationFilePath;
    private String cnvFilePath;
    private String renameFilePath;
    private String compareToFilePath;
    private boolean annotate;

    @Override
    protected void run() throws Exception {
        // Build the CLI path from opencgaHome: <opencgaHome>/analysis/pharmacogenomics/.venv/bin/pharmacogenomics
        Path cliPath = opencgaHome.resolve("analysis").resolve("pharmacogenomics").resolve(VENV_BIN);

        StringBuilder cli = new StringBuilder(cliPath.toAbsolutePath().toString());
        cli.append(" openarray");
        cli.append(" --snv-file \"").append(snvFilePath).append("\"");
        cli.append(" --translation-file \"").append(translationFilePath).append("\"");

        if (StringUtils.isNotEmpty(cnvFilePath)) {
            cli.append(" --cnv-file \"").append(cnvFilePath).append("\"");
        }
        if (StringUtils.isNotEmpty(renameFilePath)) {
            cli.append(" --rename-file \"").append(renameFilePath).append("\"");
        }
        if (StringUtils.isNotEmpty(compareToFilePath)) {
            cli.append(" --compare-to \"").append(compareToFilePath).append("\"");
        }
        if (annotate) {
            cli.append(" --annotate");
        }
        cli.append(" --outdir \"").append(getOutDir().toAbsolutePath()).append("\"");

        logger.info("Pharmacogenomics CLI: {}", cli);
        runCommandLine(cli.toString());
    }

    // Fluent setters
    public Path getOpencgaHome() { return opencgaHome; }
    public OpenArrayPharmacogenomicsAnalysisExecutor setOpencgaHome(Path opencgaHome) {
        this.opencgaHome = opencgaHome;
        return this;
    }

    public String getSnvFilePath() { return snvFilePath; }
    public OpenArrayPharmacogenomicsAnalysisExecutor setSnvFilePath(String snvFilePath) {
        this.snvFilePath = snvFilePath;
        return this;
    }

    public String getTranslationFilePath() { return translationFilePath; }
    public OpenArrayPharmacogenomicsAnalysisExecutor setTranslationFilePath(String translationFilePath) {
        this.translationFilePath = translationFilePath;
        return this;
    }

    public String getCnvFilePath() { return cnvFilePath; }
    public OpenArrayPharmacogenomicsAnalysisExecutor setCnvFilePath(String cnvFilePath) {
        this.cnvFilePath = cnvFilePath;
        return this;
    }

    public String getRenameFilePath() { return renameFilePath; }
    public OpenArrayPharmacogenomicsAnalysisExecutor setRenameFilePath(String renameFilePath) {
        this.renameFilePath = renameFilePath;
        return this;
    }

    public String getCompareToFilePath() { return compareToFilePath; }
    public OpenArrayPharmacogenomicsAnalysisExecutor setCompareToFilePath(String compareToFilePath) {
        this.compareToFilePath = compareToFilePath;
        return this;
    }

    public boolean isAnnotate() { return annotate; }
    public OpenArrayPharmacogenomicsAnalysisExecutor setAnnotate(boolean annotate) {
        this.annotate = annotate;
        return this;
    }
}
