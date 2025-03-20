package org.opencb.opencga.analysis.wrappers.exomiser;

import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@ToolExecutor(id = ExomiserWrapperAnalysisExecutor.ID,
        tool = ExomiserWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class ExomiserWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor implements StorageToolExecutor {

    public static final String ID = ExomiserWrapperAnalysis.ID + "-local";

    public static final String EXOMISER_ANALYSIS_TEMPLATE_FILENAME = "exomiser-analysis.yml";
    public static final String EXOMISER_PROPERTIES_TEMPLATE_FILENAME = "application.properties";
    public static final String EXOMISER_OUTPUT_OPTIONS_FILENAME = "output.yml";

    private Path exomiserDataPath;
    private Path sampleFile;
    private Path pedigreeFile;
    private Path vcfFile;
    private String assembly;
    private String exomiserVersion;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void run() throws ToolException, IOException, CatalogException {
        // Build the docker command line to run Exomiser
        String[] userAndGroup = FileUtils.getUserAndGroup(getOutDir(), true);
        String dockerUser = userAndGroup[0] + ":" + userAndGroup[1];
        logger.info("Docker user: {}", dockerUser);
        StringBuilder sb = initCommandLine(dockerUser);

        // Append mounts
        sb.append(" --mount type=bind,source=" + exomiserDataPath + ",target=/data,readonly")
                .append(" --mount type=bind,source=" + getOutDir() + ",target=/jobdir ");

        // Append docker image, version and command
        appendCommand("", ExomiserWrapperAnalysis.ID, exomiserVersion, sb);

        // Append input file params
        sb.append(" --analysis /jobdir/").append(EXOMISER_ANALYSIS_TEMPLATE_FILENAME);
        sb.append(" --sample /jobdir/").append(sampleFile.getFileName());
        if (pedigreeFile != null && Files.exists(pedigreeFile)) {
            sb.append(" --ped /jobdir/").append(pedigreeFile.getFileName());
        }
        sb.append(" --vcf /jobdir/" + vcfFile.getFileName())
                .append(" --assembly ").append(assembly)
                .append(" --output /jobdir/").append(EXOMISER_OUTPUT_OPTIONS_FILENAME)
                .append(" --spring.config.location=/jobdir/").append(EXOMISER_PROPERTIES_TEMPLATE_FILENAME);

        // Execute command and redirect stdout and stderr to the files
        String msg = DOCKER_CLI_MSG + sb;
        logger.info(msg);
        addWarning(msg);
        runCommandLine(sb.toString());
    }

    public Path getExomiserDataPath() {
        return exomiserDataPath;
    }

    public ExomiserWrapperAnalysisExecutor setExomiserDataPath(Path exomiserDataPath) {
        this.exomiserDataPath = exomiserDataPath;
        return this;
    }

    public Path getSampleFile() {
        return sampleFile;
    }

    public ExomiserWrapperAnalysisExecutor setSampleFile(Path sampleFile) {
        this.sampleFile = sampleFile;
        return this;
    }

    public Path getPedigreeFile() {
        return pedigreeFile;
    }

    public ExomiserWrapperAnalysisExecutor setPedigreeFile(Path pedigreeFile) {
        this.pedigreeFile = pedigreeFile;
        return this;
    }

    public Path getVcfFile() {
        return vcfFile;
    }

    public ExomiserWrapperAnalysisExecutor setVcfFile(Path vcfFile) {
        this.vcfFile = vcfFile;
        return this;
    }

    public String getAssembly() {
        return assembly;
    }

    public ExomiserWrapperAnalysisExecutor setAssembly(String assembly) {
        this.assembly = assembly;
        return this;
    }

    public String getExomiserVersion() {
        return exomiserVersion;
    }

    public ExomiserWrapperAnalysisExecutor setExomiserVersion(String exomiserVersion) {
        this.exomiserVersion = exomiserVersion;
        return this;
    }
}
