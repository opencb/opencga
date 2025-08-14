package org.opencb.opencga.analysis.wrappers.multiqc;

import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.wrappers.WrapperUtils;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.config.Analysis;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.wrapper.multiqc.MultiQcParams;
import org.opencb.opencga.core.models.wrapper.multiqc.MultiQcWrapperParams;
import org.opencb.opencga.core.models.wrapper.WrapperParams;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.util.*;

@ToolExecutor(id = MultiQcWrapperAnalysisExecutor.ID,
        tool = MultiQcWrapperAnalysis.ID,
        source = ToolExecutor.Source.FILE,
        framework = ToolExecutor.Framework.DOCKER)
public class MultiQcWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public static final String ID = MultiQcWrapperAnalysis.ID + "-docker";

    private static final String TOOL = "multiqc";
    private static final String WRAPPER_SCRIPT = "multiqc_wrapper.py";

    private String study;
    private MultiQcWrapperParams multiQcWrapperParams;

    @Override
    protected void run() throws Exception {
        // Sanity check, the parameter --outdir must match the job directory (getOutDir())
        String outDir = ((String) multiQcWrapperParams.getMultiQcParams().getOptions().get(MultiQcParams.OUTDIR_PARAM))
                .substring(WrapperUtils.OUTPUT_FILE_PREFIX.length());
        if (!getOutDir().toAbsolutePath().toString().equals(outDir)) {
            throw new ToolExecutorException("The output directory '" + outDir + "' (from the input parameter) does not match the"
                    + " expected job directory '" + getOutDir() + "'");
        }

        // MultiQC parameters to be executed in the docker container, it must contain virtual paths
        WrapperParams params = new WrapperParams();

        // Input and output bindings
        List<AbstractMap.SimpleEntry<String, String>> bindings = new ArrayList<>();
        Set<String> readOnlyBindings = new HashSet<>();

        // MultiQC input
        params.setInput(WrapperUtils.buildVirtualPaths(multiQcWrapperParams.getMultiQcParams().getInput(), "input", bindings,
                readOnlyBindings));

        // MultiQC options
        params.setOptions(WrapperUtils.updateParams(multiQcWrapperParams.getMultiQcParams().getOptions(), "data", bindings,
                readOnlyBindings));

        // Create the params file (in JSON format), it will be the input parameter of the Python wrapper
        String virtualParamsPath = WrapperUtils.createParamsFile(params, getOutDir(), bindings, readOnlyBindings);

        // Build Python command line with params file to be executed in docker
        String virtualAnalysisPath = WrapperUtils.buildVirtualAnalysisPath(getExecutorParams().getString("opencgaHome"), bindings,
                readOnlyBindings);
        String wrapperCli = WrapperUtils.buildWrapperCommandLine("python3", virtualAnalysisPath, TOOL, virtualParamsPath);

        // Build the docker command line, set user and group, and run it
        String dockerImage = getDockerFullImageName(Analysis.TRASNSCRIPTOMICS_DOCKER_KEY);
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
