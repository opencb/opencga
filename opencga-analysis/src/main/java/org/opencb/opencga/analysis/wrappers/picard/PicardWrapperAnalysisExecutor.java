package org.opencb.opencga.analysis.wrappers.picard;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@ToolExecutor(id = PicardWrapperAnalysisExecutor.ID,
        tool = PicardWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class PicardWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public final static String ID = PicardWrapperAnalysis.ID + "-local";

    private String study;
    private String command;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public String getDockerImageName() {
        return "broadinstitute/picard";
    }

    @Override
    public String getDockerImageVersion() {
        return "";
    }

    @Override
    public String getShortPrefix() {
        return "";
    }

    @Override
    public String getLongPrefix() {
        return "";
    }

    @Override
    public String getKeyValueSeparator() {
        return "=";
    }

    @Override
    public void run() throws ToolException {
        switch (command) {
            case "CollectHsMetrics":
            case "CollectWgsMetrics":
            case "BedToIntervalList":
                runCommonCommand();
                break;
            default:
                throw new ToolException("Picard tool name '" + command + "' is not supported yet.");
        }
    }

    private void runCommonCommand() throws ToolException {
        StringBuilder sb = initCommandLine();

        // Append mounts
        Set<String> inputFileParamNames = PicardWrapperAnalysis.getFileParamNames(command);
        List<Pair<String, String>> inputFilenames = DockerWrapperAnalysisExecutor.getInputFilenames(null, inputFileParamNames,
                getExecutorParams());
        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand("java -jar /usr/picard/picard.jar " + command, sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Append output file params
        if (getExecutorParams().containsKey("O") || getExecutorParams().containsKey("OUTPUT")) {
            String value = String.valueOf(getExecutorParams().get("O"));
            if (StringUtils.isEmpty(value)) {
                value = String.valueOf(getExecutorParams().get("OUTPUT"));
            }
            if (StringUtils.isNotEmpty(value)) {
                List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("O", value)));
                appendOutputFiles(outputFilenames, sb);
            }
        }

        // Append other params
        Set<String> skipParams =  new HashSet<>(Arrays.asList("arguments_file", "O", "OUTPUT"));
        skipParams.addAll(inputFileParamNames);
        appendOtherParams(skipParams, sb);

        // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
        logger.info("Docker command line: " + sb.toString());
        runCommandLine(sb.toString());
    }

    public String getStudy() {
        return study;
    }

    public PicardWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getCommand() {
        return command;
    }

    public PicardWrapperAnalysisExecutor setCommand(String command) {
        this.command = command;
        return this;
    }
}
