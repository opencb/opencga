package org.opencb.opencga.analysis.wrappers.star;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.multiqc.MultiQcWrapperAnalysis;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.wrapper.StarWrapperParams;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.analysis.wrappers.WrapperUtils.*;
import static org.opencb.opencga.analysis.wrappers.WrapperUtils.buildVirtualPaths;
import static org.opencb.opencga.analysis.wrappers.WrapperUtils.buildWrapperCli;
import static org.opencb.opencga.analysis.wrappers.WrapperUtils.processParamsFile;
import static org.opencb.opencga.analysis.wrappers.WrapperUtils.updateParams;
import static org.opencb.opencga.analysis.wrappers.multiqc.MultiQcWrapperAnalysis.ID;

@ToolExecutor(id = StarWrapperAnalysisExecutor.ID,
        tool = ID,
        source = ToolExecutor.Source.FILE,
        framework = ToolExecutor.Framework.DOCKER)
public class StarWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public static final String ID = StarWrapperAnalysis.ID + "-docker";

    public static final String RUN_MODE_PARAM = "--runMode";
    public static final String READ_FILES_IN_PARAM = "--readFilesIn";
    public static final String OUT_FILE_NAME_PREFIX_PARAM = "--outFileNamePrefix";
    public static final String GENOME_DIR_PARAM = "--genomeDir";

    public static final String ALIGN_READS_VALUE = "alignReads";
    public static final String GENOME_GENERATE_VALUE = "genomeGenerate";

    private static final String WRAPPER_SCRIPT = "star_wrapper.py";
    private static final String TOOL = "STAR";

    private String study;
    private StarWrapperParams starWrapperParams;

    @Override
    protected void run() throws Exception {
        // Sanity check, outDir must match the parameter output
        String outDir = "";
        for (Map.Entry<String, Object> entry : starWrapperParams.getStarParams().entrySet()) {
            if (entry.getValue() instanceof String) {
                String value = (String) entry.getValue();
                if (StringUtils.isNotEmpty(value) && value.startsWith(OUTPUT_FILE_PREFIX)) {
                    outDir = value.substring(OUTPUT_FILE_PREFIX.length());
                    if (!getOutDir().toAbsolutePath().startsWith(outDir)) {
                        throw new ToolExecutorException("Output file parameter '" + entry.getKey() + "' (value '" + outDir + "') does not"
                                + " match the expected output directory '" + getOutDir() + "'");
                    }
                }
            }
        }

        // STAR parameters to be executed in the docker container, it must contain virtual paths
        ObjectMap updatedParams;

        // Input and output bindings
        List<AbstractMap.SimpleEntry<String, String>> bindings = new ArrayList<>();
        Set<String> readOnlyBindings = new HashSet<>();

        // Script path
        String virtualAnalysisPath = buildVirtualAnalysisPath(getExecutorParams().getString("opencgaHome"), bindings, readOnlyBindings);

        // STAR parameters
        updatedParams = updateParams(starWrapperParams.getStarParams(), "data", bindings, readOnlyBindings);

        // Params file path
        String virtualParamsPath = processParamsFile(updatedParams, getOutDir(), bindings, readOnlyBindings);

        // Build Python command line with params file and execute it in docker
//        String wrapperCli = buildWrapperCli("python3", virtualAnalysisPath, StarWrapperAnalysis.ID, WRAPPER_SCRIPT, virtualParamsPath);
        String wrapperCli = buildWrapperCli("python3", virtualAnalysisPath, TOOL, virtualParamsPath);
        String dockerImage = "opencb/opencga-transcriptomics:" + getDockerImageVersion();

        // User: array of two strings, the first string, the user; the second, the group
        String[] user = FileUtils.getUserAndGroup(getOutDir(), true);
        Map<String, String> dockerParams = new HashMap<>();
        dockerParams.put("user", user[0] + ":" + user[1]);

        String dockerCli = buildCommandLine(dockerImage, bindings, readOnlyBindings, wrapperCli, dockerParams);
        addEvent(Event.Type.INFO, "Docker command line: " + dockerCli);
        logger.info("Docker command line: {}", dockerCli);
        int exitValue = runCommandLine(dockerCli);

        if (exitValue != 0) {
            throw new ToolExecutorException("Error executing STAR: exit value " + exitValue + ". Check the logs for more details.");
        }
    }

    public String getStudy() {
        return study;
    }

    public StarWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public StarWrapperParams getStarWrapperParams() {
        return starWrapperParams;
    }

    public StarWrapperAnalysisExecutor setStarWrapperParams(StarWrapperParams starWrapperParams) {
        this.starWrapperParams = starWrapperParams;
        return this;
    }
}
