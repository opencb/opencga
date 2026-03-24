package org.opencb.opencga.analysis.clinical.pharmacogenomics;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.io.File;
import java.nio.file.Paths;
import java.util.*;

/**
 * Executor for OpenArray pharmacogenomics analysis.
 * Runs the Python pharmacogenomics CLI inside the opencga-base Docker container.
 */
@ToolExecutor(id = OpenArrayPharmacogenomicsAnalysisExecutor.ID,
        tool = OpenArrayPharmacogenomicsAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class OpenArrayPharmacogenomicsAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public static final String ID = OpenArrayPharmacogenomicsAnalysis.ID + "-local";

    private static final String PYTHON_CLI = "/opt/opencga/analysis/pharmacogenomics/.venv/bin/pharmacogenomics";

    private String snvFilePath;
    private String translationFilePath;
    private String cnvFilePath;
    private String renameFilePath;
    private String compareToPath;
    private boolean annotate;

    @Override
    protected void run() throws Exception {
        // Get Docker image (opencga-ext-tools or opencga-base)
        String dockerImage = getDockerImageName() + ":" + getDockerImageVersion();

        // Collect all input file directories for mounting
        Set<String> inputDirs = new LinkedHashSet<>();
        inputDirs.add(new File(snvFilePath).getParent());
        inputDirs.add(new File(translationFilePath).getParent());
        if (StringUtils.isNotEmpty(cnvFilePath)) {
            inputDirs.add(new File(cnvFilePath).getParent());
        }
        if (StringUtils.isNotEmpty(renameFilePath)) {
            inputDirs.add(new File(renameFilePath).getParent());
        }
        if (StringUtils.isNotEmpty(compareToPath)) {
            inputDirs.add(new File(compareToPath).getParent());
        }

        // Build input bindings: mount each unique directory as read-only
        List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
        Set<String> readOnlyBindings = new HashSet<>();
        int idx = 0;
        Map<String, String> hostToContainer = new HashMap<>();
        for (String dir : inputDirs) {
            String containerPath = DOCKER_INPUT_PATH + idx;
            inputBindings.add(new AbstractMap.SimpleEntry<>(dir, containerPath));
            readOnlyBindings.add(containerPath);
            hostToContainer.put(dir, containerPath);
            idx++;
        }

        // Output binding
        String outDirStr = getOutDir().toAbsolutePath().toString();
        AbstractMap.SimpleEntry<String, String> outputBinding =
                new AbstractMap.SimpleEntry<>(outDirStr, DOCKER_OUTPUT_PATH);

        // Build Python CLI command with Docker-mapped paths
        StringBuilder cli = new StringBuilder(PYTHON_CLI);
        cli.append(" openarray");
        cli.append(" --snv-file ").append(toContainerPath(snvFilePath, hostToContainer));
        cli.append(" --translation-file ").append(toContainerPath(translationFilePath, hostToContainer));

        if (StringUtils.isNotEmpty(cnvFilePath)) {
            cli.append(" --cnv-file ").append(toContainerPath(cnvFilePath, hostToContainer));
        }
        if (StringUtils.isNotEmpty(renameFilePath)) {
            cli.append(" --rename-file ").append(toContainerPath(renameFilePath, hostToContainer));
        }
        if (StringUtils.isNotEmpty(compareToPath)) {
            cli.append(" --compare-to ").append(toContainerPath(compareToPath, hostToContainer));
        }
        if (annotate) {
            cli.append(" --annotate");
        }
        cli.append(" --outdir ").append(DOCKER_OUTPUT_PATH);

        // Build and run Docker command
        String dockerCli = buildCommandLine(dockerImage, inputBindings, readOnlyBindings, outputBinding,
                cli.toString(), null);

        logger.info(DOCKER_CLI_MSG + dockerCli);
        runCommandLine(dockerCli);
    }

    /**
     * Map a host file path to its Docker container path using the mount map.
     */
    private String toContainerPath(String hostPath, Map<String, String> hostToContainer) {
        String dir = new File(hostPath).getParent();
        String filename = new File(hostPath).getName();
        String containerDir = hostToContainer.get(dir);
        return containerDir + "/" + filename;
    }

    // Fluent setters
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

    public String getCompareToPath() { return compareToPath; }
    public OpenArrayPharmacogenomicsAnalysisExecutor setCompareToPath(String compareToPath) {
        this.compareToPath = compareToPath;
        return this;
    }

    public boolean isAnnotate() { return annotate; }
    public OpenArrayPharmacogenomicsAnalysisExecutor setAnnotate(boolean annotate) {
        this.annotate = annotate;
        return this;
    }
}
