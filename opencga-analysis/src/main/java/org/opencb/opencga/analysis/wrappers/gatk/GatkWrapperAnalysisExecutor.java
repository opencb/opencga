package org.opencb.opencga.analysis.wrappers.gatk;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@ToolExecutor(id = GatkWrapperAnalysisExecutor.ID,
        tool = GatkWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class GatkWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public final static String ID = GatkWrapperAnalysis.ID + "-local";

    private String study;
    private String command;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public String getDockerImageName() {
        return "broadinstitute/gatk";
    }

    @Override
    public String getDockerImageVersion() {
        return "";
    }

    @Override
    public void run() throws ToolException {
        switch (command) {
            case "HaplotypeCaller":
                runHaplotypeCaller();
                break;
            default:
                throw new ToolException("Gatk command '" + command + "' is not supported yet.");
        }
    }

    private void runHaplotypeCaller() throws ToolException {
        StringBuilder sb = initCommandLine();

        // Append mounts
        Set<String> inputFileParamNames = GatkWrapperAnalysis.getFileParamNames(command);
        List<Pair<String, String>> inputFilenames = DockerWrapperAnalysisExecutor.getInputFilenames(null, inputFileParamNames,
                getExecutorParams());
        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand("gatk " + command, sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Append output file params
        String value = null;
        if (getExecutorParams().containsKey("O")) {
            value = String.valueOf(getExecutorParams().get("O"));
        }
        if (StringUtils.isEmpty(value)) {
            value = "out.vcf";
        }
        List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("O", value)));
        appendOutputFiles(outputFilenames, sb);

        // Append other params
        Set<String> skipParams = new HashSet<>(Arrays.asList("O"));
        skipParams.addAll(inputFileParamNames);
        appendOtherParams(skipParams, sb);

        // Execute command and redirect stdout and stderr to the files
        logger.info("Docker command line: " + sb.toString());
        runCommandLine(sb.toString());
    }

    public String getStudy() {
        return study;
    }

    public GatkWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getCommand() {
        return command;
    }

    public GatkWrapperAnalysisExecutor setCommand(String command) {
        this.command = command;
        return this;
    }
}
