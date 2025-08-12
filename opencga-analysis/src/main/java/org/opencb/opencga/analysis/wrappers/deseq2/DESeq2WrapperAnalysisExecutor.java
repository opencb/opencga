package org.opencb.opencga.analysis.wrappers.deseq2;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.multiqc.MultiQcWrapperAnalysis;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.wrapper.deseq2.DESeq2Input;
import org.opencb.opencga.core.models.wrapper.deseq2.DESeq2Params;
import org.opencb.opencga.core.models.wrapper.deseq2.DESeq2WrapperParams;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.util.*;

import static org.opencb.opencga.analysis.wrappers.WrapperUtils.*;

@ToolExecutor(id = DESeq2WrapperAnalysisExecutor.ID,
        tool = MultiQcWrapperAnalysis.ID,
        source = ToolExecutor.Source.FILE,
        framework = ToolExecutor.Framework.DOCKER)
public class DESeq2WrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public static final String ID = DESeq2WrapperAnalysis.ID + "-docker";

    private static final String WRAPPER_SCRIPT = "run_deseq2.py";

    private String study;
    private DESeq2WrapperParams deSeq2WrapperParams;

    @Override
    protected void run() throws Exception {

        // DESeq2 parameters to be executed in the docker container, it must contain virtual paths
        DESeq2Params updatedParams = new DESeq2Params();

        // Input and output bindings
        List<AbstractMap.SimpleEntry<String, String>> bindings = new ArrayList<>();
        Set<String> readOnlyBindings = new HashSet<>();

        // Script path
        String virtualAnalysisPath = buildVirtualAnalysisPath(getExecutorParams().getString("opencgaHome"), bindings, readOnlyBindings);

        // DESeq2 input
        DESeq2Input input = deSeq2WrapperParams.getDESeq2Params2Params().getInput();
        updatedParams.getInput().setCountsFile(buildVirtualPath(input.getCountsFile(), "counts", bindings, readOnlyBindings));
        updatedParams.getInput().setMetadataFile(buildVirtualPath(input.getMetadataFile(), "metadata", bindings, readOnlyBindings));

        // DESeq2 analysis
        updatedParams.setAnalysis(deSeq2WrapperParams.getDESeq2Params2Params().getAnalysis());

        // DESeq2 output
        updatedParams.setOutput(deSeq2WrapperParams.getDESeq2Params2Params().getOutput());
        String output = buildVirtualPath(OUTPUT_FILE_PREFIX + getOutDir().toAbsolutePath(), "output", bindings, readOnlyBindings);
        updatedParams.getOutput().setBasename(output + deSeq2WrapperParams.getDESeq2Params2Params().getOutput().getBasename());

        // Params file path
        Map<String, Object> params = new ObjectMapper().convertValue(updatedParams, Map.class);
        String virtualParamsPath = processParamsFile(new ObjectMap(params), false, getOutDir(), bindings, readOnlyBindings);

        // Build Python command line with params file and execute it in docker
        String wrapperCli = "python3 " + virtualAnalysisPath + "/" + DESeq2WrapperAnalysis.ID + "/" + WRAPPER_SCRIPT + " "
                + virtualParamsPath;
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
            throw new ToolExecutorException("Error executing DESeq2: exit value " + exitValue + ". Check the logs for more details.");
        }
    }

    public String getStudy() {
        return study;
    }

    public DESeq2WrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public DESeq2WrapperParams getDESeq2WrapperParams() {
        return deSeq2WrapperParams;
    }

    public DESeq2WrapperAnalysisExecutor setDESeq2WrapperParams(DESeq2WrapperParams deSeq2WrapperParams) {
        this.deSeq2WrapperParams = deSeq2WrapperParams;
        return this;
    }
}

