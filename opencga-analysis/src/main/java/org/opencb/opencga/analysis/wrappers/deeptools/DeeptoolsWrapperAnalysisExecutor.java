package org.opencb.opencga.analysis.wrappers.deeptools;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@ToolExecutor(id = DeeptoolsWrapperAnalysisExecutor.ID,
        tool = DeeptoolsWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class DeeptoolsWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public final static String ID = DeeptoolsWrapperAnalysis.ID + "-local";

    private String study;
    private String command;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public String getDockerImageName() {
        return "dhspence/docker-deeptools";
    }

    @Override
    public String getDockerImageVersion() {
        return null;
    }

    @Override
    public void run() throws ToolException {
        switch (command) {
            case "bamCoverage":
            case "bamCompare":
                runBamCommonCommand();
                break;
            default:
                throw new ToolException("Deeptools command '" + command + "' is not supported yet.");
        }
    }

    private void runBamCommonCommand() throws ToolException {
        StringBuilder sb = initCommandLine();

        // Append mounts
        List<Pair<String, String>> inputFilenames = DockerWrapperAnalysisExecutor.getInputFilenames(null,
                DeeptoolsWrapperAnalysis.getFileParamNames(command), getExecutorParams());
        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand(command, sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Append output file params
        if (getExecutorParams().containsKey("o") || getExecutorParams().containsKey("outFileName")) {
            String value = String.valueOf(getExecutorParams().get("o"));
            if (StringUtils.isEmpty(value)) {
                value = String.valueOf(getExecutorParams().get("outFileName"));
            }
            if (StringUtils.isNotEmpty(value)) {
                List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("o", value)));
                appendOutputFiles(outputFilenames, sb);
            }
        }

        // Append other params
        Set<String> skipParams = new HashSet<>(Arrays.asList("o", "outFileName"));
        skipParams.addAll(DeeptoolsWrapperAnalysis.getFileParamNames(command));
        appendOtherParams(skipParams, sb);

        // Execute command and redirect stdout and stderr to the files
        logger.info("Docker command line: " + sb.toString());
        runCommandLine(sb.toString());
    }

    public String getStudy() {
        return study;
    }

    public DeeptoolsWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getCommand() {
        return command;
    }

    public DeeptoolsWrapperAnalysisExecutor setCommand(String command) {
        this.command = command;
        return this;
    }
}
