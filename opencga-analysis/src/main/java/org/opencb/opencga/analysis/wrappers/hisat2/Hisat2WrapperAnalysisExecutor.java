package org.opencb.opencga.analysis.wrappers.hisat2;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.multiqc.MultiQcWrapperAnalysis;
import org.opencb.opencga.core.config.Analysis;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.wrapper.Hisat2WrapperParams;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.analysis.wrappers.WrapperUtils.*;
import static org.opencb.opencga.analysis.wrappers.multiqc.MultiQcWrapperAnalysis.ID;

@ToolExecutor(id = Hisat2WrapperAnalysisExecutor.ID,
        tool = ID,
        source = ToolExecutor.Source.FILE,
        framework = ToolExecutor.Framework.DOCKER)
public class Hisat2WrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public static final String ID = MultiQcWrapperAnalysis.ID + "-docker";

    public static final String X_PARAM = "-x";
    public static final String S_PARAM = "-S";

    public static final String HISAT2_TOOL = "hisat2";
    public static final String HISAT2_BUILD_TOOL = "hisat2-build";
    public static final String HISAT2_BUILD_L_TOOL = "hisat2-build-l";
    public static final String HISAT2_BUILD_S_TOOL = "hisat2-build-s";

    private static final String WRAPPER_SCRIPT = "multiqc_wrapper.py";

    private String study;
    private Hisat2WrapperParams hisat2WrapperParams;

    @Override
    protected void run() throws Exception {
        // Sanity check, outdir must match the output directory
        String outDir;
        if (hisat2WrapperParams.getHisat2Params().getParams().containsKey(S_PARAM)) {
            outDir = hisat2WrapperParams.getHisat2Params().getParams().getString(S_PARAM).substring(OUTPUT_FILE_PREFIX.length());
        } else {
            outDir = ((String) hisat2WrapperParams.getHisat2Params().getInput().get(1)).substring(OUTPUT_FILE_PREFIX.length());
        }
//        if (!getOutDir().toAbsolutePath().toString().startsWith(outDir)) {
//            throw new ToolExecutorException("Output directory '" + outDir + "' does not match the expected output directory '"
//                    + getOutDir() + "'");
//        }

        // HISAT2 parameters to be executed in the docker container, it must contain virtual paths
        ObjectMap updatedParams = new ObjectMap();

        // Input and output bindings
        List<AbstractMap.SimpleEntry<String, String>> bindings = new ArrayList<>();
        Set<String> readOnlyBindings = new HashSet<>();

        // Script path
        String virtualAnalysisPath = buildVirtualAnalysisPath(getExecutorParams().getString("opencgaHome"), bindings, readOnlyBindings);

        // HISAT2 input
        String indexBasename ;
        if (CollectionUtils.isNotEmpty(hisat2WrapperParams.getHisat2Params().getInput())) {
            List<String> input = new ArrayList<>();
            List<String> input0 = buildVirtualPaths((List<String>) hisat2WrapperParams.getHisat2Params().getInput().get(0), "input",
                    bindings, readOnlyBindings);
            input.add(StringUtils.join(input0, ","));

            // If the command is hisat2-build, we need to get index basename
            Path path = Paths.get(((String) hisat2WrapperParams.getHisat2Params().getInput().get(1))
                    .substring(OUTPUT_FILE_PREFIX.length()));
            indexBasename = path.getFileName().toString();
            String input1 = buildVirtualPath(OUTPUT_FILE_PREFIX + path.getParent().toAbsolutePath(), "output", bindings, readOnlyBindings);
            input.add(input1 + indexBasename);

            updatedParams.put(INPUT, input);
        }

        // HISAT params
        // Save SAM file path if it exists, so we can update it later
        String samFile = hisat2WrapperParams.getHisat2Params().getParams().getString(S_PARAM);
        if (StringUtils.isNotEmpty(samFile)) {
            hisat2WrapperParams.getHisat2Params().getParams().remove(S_PARAM);
        }
        // Save index file path if it exists, so we can update it later
        String indexFile = hisat2WrapperParams.getHisat2Params().getParams().getString(X_PARAM);
        if (StringUtils.isNotEmpty(indexFile)) {
            hisat2WrapperParams.getHisat2Params().getParams().remove(X_PARAM);
        }
        updatedParams.put(PARAMS, updateParams(hisat2WrapperParams.getHisat2Params().getParams(), "data", bindings, readOnlyBindings));
        // Restore SAM file path if it exists
        if (StringUtils.isNotEmpty(samFile)) {
            String output = buildVirtualPath(OUTPUT_FILE_PREFIX + Paths.get(samFile.substring(OUTPUT_FILE_PREFIX.length())).getParent()
                            .toAbsolutePath(), "output", bindings, readOnlyBindings);
            String samFilename = Paths.get(samFile.substring(OUTPUT_FILE_PREFIX.length())).getFileName().toString();
            updatedParams.getMap(PARAMS).put(S_PARAM, output + samFilename);
        }
        // Restore index file path if it exists
        if (StringUtils.isNotEmpty(indexFile)) {
            String index = buildVirtualPath(INPUT_FILE_PREFIX + Paths.get(indexFile.substring(INPUT_FILE_PREFIX.length())).getParent()
                    .toAbsolutePath(), "index", bindings, readOnlyBindings);
            String indexFilename = Paths.get(indexFile.substring(INPUT_FILE_PREFIX.length())).getFileName().toString();
            String basename = indexFilename.substring(0, indexFilename.indexOf('.'));
            updatedParams.getMap(PARAMS).put(X_PARAM, index + basename);
        }

        // Params file path
        String virtualParamsPath = processParamsFile(updatedParams, getOutDir(), bindings, readOnlyBindings);

        // Build Python command line with params file and execute it in docker
//        String wrapperCli = buildWrapperCli("python3", virtualAnalysisPath, StarWrapperAnalysis.ID, WRAPPER_SCRIPT, virtualParamsPath);
        String wrapperCli = buildWrapperCommandLine("python3", virtualAnalysisPath, hisat2WrapperParams.getHisat2Params().getCommand(),
                virtualParamsPath);
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
            throw new ToolExecutorException("Error executing HISAT2: exit value " + exitValue + ". Check the logs for more details.");
        }
    }

    public String getStudy() {
        return study;
    }

    public Hisat2WrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public Hisat2WrapperParams getHisat2WrapperParams() {
        return hisat2WrapperParams;
    }

    public Hisat2WrapperAnalysisExecutor setHisat2WrapperParams(Hisat2WrapperParams hisat2WrapperParams) {
        this.hisat2WrapperParams = hisat2WrapperParams;
        return this;
    }
}
