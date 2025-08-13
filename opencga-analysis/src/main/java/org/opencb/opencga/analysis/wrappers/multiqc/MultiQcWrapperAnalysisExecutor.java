package org.opencb.opencga.analysis.wrappers.multiqc;

import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.wrappers.WrapperUtils;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.config.Analysis;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.wrapper.MultiQcParams;
import org.opencb.opencga.core.models.wrapper.MultiQcWrapperParams;
import org.opencb.opencga.core.tools.ResourceManager;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.analysis.wrappers.WrapperUtils.*;
import static org.opencb.opencga.analysis.wrappers.WrapperUtils.buildVirtualAnalysisPath;
import static org.opencb.opencga.analysis.wrappers.WrapperUtils.buildVirtualPaths;
import static org.opencb.opencga.analysis.wrappers.WrapperUtils.buildWrapperCli;
import static org.opencb.opencga.analysis.wrappers.WrapperUtils.updateParams;
import static org.opencb.opencga.analysis.wrappers.multiqc.MultiQcWrapperAnalysis.ID;

@ToolExecutor(id = MultiQcWrapperAnalysisExecutor.ID,
        tool = ID,
        source = ToolExecutor.Source.FILE,
        framework = ToolExecutor.Framework.DOCKER)
public class MultiQcWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public static final String ID = MultiQcWrapperAnalysis.ID + "-docker";

    public static final String OUTDIR_PARAM = "--outdir";

    private static final String TOOL = "multiqc";
    private static final String WRAPPER_SCRIPT = "multiqc_wrapper.py";

    private String study;
    private MultiQcWrapperParams multiQcWrapperParams;

    @Override
    protected void run() throws Exception {
        // Sanity check, outdir must match the output directory
        String outDir = ((String) multiQcWrapperParams.getMultiQcParams().getParams().get(OUTDIR_PARAM))
                .substring(OUTPUT_FILE_PREFIX.length());
        if (!getOutDir().toAbsolutePath().toString().equals(outDir)) {
            throw new ToolExecutorException("Output directory '" + outDir + "' does not match the expected output directory '"
                    + getOutDir() + "'");
        }

        // MultiQC parameters to be executed in the docker container, it must contain virtual paths
        ObjectMap updatedParams = new ObjectMap();

        // Input and output bindings
        List<AbstractMap.SimpleEntry<String, String>> bindings = new ArrayList<>();
        Set<String> readOnlyBindings = new HashSet<>();

        // Script path
        String virtualAnalysisPath = buildVirtualAnalysisPath(getExecutorParams().getString("opencgaHome"), bindings, readOnlyBindings);

        // MultiQC input
        updatedParams.put(INPUT, buildVirtualPaths(multiQcWrapperParams.getMultiQcParams().getInput(), "input", bindings,
                readOnlyBindings));

        // MultiQC params
        updatedParams.put(PARAMS, updateParams(multiQcWrapperParams.getMultiQcParams().getParams(), "data", bindings, readOnlyBindings));

        // Params file path
        String virtualParamsPath = processParamsFile(updatedParams, getOutDir(), bindings, readOnlyBindings);

        // Build Python command line with params file and execute it in docker
//        String wrapperCli = buildWrapperCli("python3", virtualAnalysisPath, StarWrapperAnalysis.ID, WRAPPER_SCRIPT, virtualParamsPath);
        String wrapperCli = buildWrapperCli("python3", virtualAnalysisPath, TOOL, virtualParamsPath);
        String dockerImage = getDockerFullImageName(Analysis.TRASNSCRIPTOMICS_DOCKER_KEY);

        // User: array of two strings, the first string, the user; the second, the group
        String[] user = FileUtils.getUserAndGroup(getOutDir(), true);
        Map<String, String> dockerParams = new HashMap<>();
        dockerParams.put("user", user[0] + ":" + user[1]);

        String dockerCli = buildCommandLine(dockerImage, bindings, readOnlyBindings, wrapperCli, dockerParams);
        addEvent(Event.Type.INFO, "Docker command line: " + dockerCli);
        logger.info("Docker command line: {}", dockerCli);
        int exitValue = runCommandLine(dockerCli);

        if (exitValue != 0) {
            throw new ToolExecutorException("Error executing MultiQC: exit value " + exitValue + ". Check the logs for more details.");
        }
    }

    public String getStudy() {
        return study;
    }

    public MultiQcWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public MultiQcWrapperParams getMultiQcWrapperParams() {
        return multiQcWrapperParams;
    }

    public MultiQcWrapperAnalysisExecutor setMultiQcWrapperParams(MultiQcWrapperParams multiQcWrapperParams) {
        this.multiQcWrapperParams = multiQcWrapperParams;
        return this;
    }
}
