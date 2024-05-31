package org.opencb.opencga.analysis.wrappers.samtools;

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

@ToolExecutor(id = SamtoolsWrapperAnalysisExecutor.ID,
        tool = SamtoolsWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class SamtoolsWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public final static String ID = SamtoolsWrapperAnalysis.ID + "-local";

    private String study;
    private String command;
    private String inputFile;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void run() throws ToolException {
        switch (command) {
            case "depth":
                runDepth();
                break;
            case "dict":
                runDict();
                break;
            case "view":
                runView();
                break;
            case "index":
                runIndex();
                break;
            case "sort":
                runSort();
                break;
            case "faidx":
                runFaidx();
                break;
            case "stats":
                runStats();
                break;
            case "flagstat":
                runFlagstat();
                break;
            case "plot-bamstats":
                runPlotBamStats();
                break;
            default:
                throw new ToolException("Samtools command '" + command + "' is not supported yet.");
        }
    }

    private void runDepth() throws ToolException {
        StringBuilder sb = initCommandLine();

        // Append mounts
        Set<String> inputFileParamNames = SamtoolsWrapperAnalysis.getFileParamNames(command);
        List<Pair<String, String>> inputFilenames = DockerWrapperAnalysisExecutor.getInputFilenames(getInputFile(),
                inputFileParamNames, getExecutorParams());
        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand("samtools " + command, sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Append other params
        Set<String> skipParams = new HashSet<>(Arrays.asList("f"));
        skipParams.addAll(inputFileParamNames);
        appendOtherParams(skipParams, sb);

        // Execute command and redirect stdout and stderr to the files
        logger.info("Docker command line: " + sb.toString());
        runCommandLine(sb.toString());
    }

    private void runDict() throws ToolException {
        StringBuilder sb = initCommandLine();

        // Append mounts
        Set<String> inputFileParamNames = SamtoolsWrapperAnalysis.getFileParamNames(command);
        List<Pair<String, String>> inputFilenames = DockerWrapperAnalysisExecutor.getInputFilenames(getInputFile(),
                inputFileParamNames, getExecutorParams());
        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand("samtools " + command, sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Append output file params
        if (getExecutorParams().containsKey("o") || getExecutorParams().containsKey("output")) {
            String value = String.valueOf(getExecutorParams().get("o"));
            if (StringUtils.isEmpty(value)) {
                value = String.valueOf(getExecutorParams().get("output"));
            }
            if (StringUtils.isNotEmpty(value)) {
                List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("o",
                        value)));
                appendOutputFiles(outputFilenames, sb);
            }
        }

        // Append other params
        Set<String> skipParams = new HashSet<>(Arrays.asList("u", "uri", "o", "output"));
        skipParams.addAll(inputFileParamNames);
        appendOtherParams(skipParams, sb);

        // Execute command and redirect stdout and stderr to the files
        logger.info("Docker command line: " + sb.toString());
        runCommandLine(sb.toString());
    }

    private void runView() throws ToolException {
        StringBuilder sb = initCommandLine();

        // Append mounts
        Set<String> inputFileParamNames = SamtoolsWrapperAnalysis.getFileParamNames(command);
        List<Pair<String, String>> inputFilenames = DockerWrapperAnalysisExecutor.getInputFilenames(getInputFile(),
                inputFileParamNames, getExecutorParams());
        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand("samtools " + command, sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Append output file params
        if (getExecutorParams().containsKey("o")) {
            String value = String.valueOf(getExecutorParams().get("o"));
            if (StringUtils.isNotEmpty(value)) {
                List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("o",
                        value)));
                appendOutputFiles(outputFilenames, sb);
            }
        }

        // Append other params
        Set<String> skipParams = new HashSet<>(Arrays.asList("o"));
        skipParams.addAll(inputFileParamNames);
        appendOtherParams(skipParams, sb);

        // Execute command and redirect stdout and stderr to the files
        logger.info("Docker command line: " + sb.toString());
        runCommandLine(sb.toString());
    }

    private void runIndex() throws ToolException {
        StringBuilder sb = initCommandLine();

        // Append mounts
        Set<String> inputFileParamNames = SamtoolsWrapperAnalysis.getFileParamNames(command);
        List<Pair<String, String>> inputFilenames = DockerWrapperAnalysisExecutor.getInputFilenames(getInputFile(),
                inputFileParamNames,
                getExecutorParams());
        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand("samtools " + command, sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Append other params
        appendOtherParams(null, sb);

        // Execute command and redirect stdout and stderr to the files
        logger.info("Docker command line: " + sb.toString());
        runCommandLine(sb.toString());
    }

    private void runSort() throws ToolException {
        StringBuilder sb = initCommandLine();

        // Append mounts
        Set<String> inputFileParamNames = SamtoolsWrapperAnalysis.getFileParamNames(command);
        List<Pair<String, String>> inputFilenames = DockerWrapperAnalysisExecutor.getInputFilenames(getInputFile(),
                inputFileParamNames,
                getExecutorParams());
        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand("samtools " + command, sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Append output file params
        if (getExecutorParams().containsKey("o")) {
            String value = String.valueOf(getExecutorParams().get("o"));
            if (StringUtils.isNotEmpty(value)) {
                List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("o",
                        value)));
                appendOutputFiles(outputFilenames, sb);
            }
        }

        // Append other params
        Set<String> skipParams = new HashSet<>(Arrays.asList("T", "o"));
        skipParams.addAll(inputFileParamNames);
        appendOtherParams(skipParams, sb);

        // Execute command and redirect stdout and stderr to the files
        logger.info("Docker command line: " + sb.toString());
        runCommandLine(sb.toString());
    }

    private void runFaidx() throws ToolException {
        StringBuilder sb = initCommandLine();

        // Append mounts
        Set<String> inputFileParamNames = SamtoolsWrapperAnalysis.getFileParamNames(command);
        List<Pair<String, String>> inputFilenames = DockerWrapperAnalysisExecutor.getInputFilenames(getInputFile(),
                inputFileParamNames, getExecutorParams());
        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand("samtools " + command, sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Append other params
        appendOtherParams(null, sb);

        // Execute command and redirect stdout and stderr to the files
        logger.info("Docker command line: " + sb.toString());
        runCommandLine(sb.toString());
    }

    private void runStats() throws ToolException {
        StringBuilder sb = initCommandLine();

        // Append mounts
        Set<String> inputFileParamNames = SamtoolsWrapperAnalysis.getFileParamNames(command);
        List<Pair<String, String>> inputFilenames = DockerWrapperAnalysisExecutor.getInputFilenames(getInputFile(),
                inputFileParamNames, getExecutorParams());
        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand("samtools " + command, sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Append other params
        appendOtherParams(null, sb);

        // Execute command and redirect stdout and stderr to the files
        logger.info("Docker command line: " + sb.toString());
        runCommandLine(sb.toString());
    }

    private void runPlotBamStats() throws ToolException {
        StringBuilder sb = initCommandLine();

        // Append mounts
        Set<String> inputFileParamNames = SamtoolsWrapperAnalysis.getFileParamNames(command);
        List<Pair<String, String>> inputFilenames = DockerWrapperAnalysisExecutor.getInputFilenames(getInputFile(),
                inputFileParamNames, getExecutorParams());
        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand(command, sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Append output file params
        List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("p", " ")));
        appendOutputFiles(outputFilenames, sb);

        // Append other params
        Set<String> skipParams =  new HashSet<>(Arrays.asList("p", "prefix"));
        skipParams.addAll(inputFileParamNames);
        appendOtherParams(skipParams, sb);

        // Execute command and redirect stdout and stderr to the files
        logger.info("Docker command line: " + sb.toString());
        runCommandLine(sb.toString());
    }

    private void runFlagstat() throws ToolException {
        StringBuilder sb = initCommandLine();

        // Append mounts
        Set<String> inputFileParamNames = SamtoolsWrapperAnalysis.getFileParamNames(command);
        List<Pair<String, String>> inputFilenames = DockerWrapperAnalysisExecutor.getInputFilenames(getInputFile(),
                inputFileParamNames, getExecutorParams());
        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand("samtools " + command, sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Append other params
        Set<String> skipParams =  new HashSet<>(Arrays.asList("input-fmt-option"));
        skipParams.addAll(inputFileParamNames);
        appendOtherParams(skipParams, sb);

        // Execute command and redirect stdout and stderr to the files
        logger.info("Docker command line: " + sb);
        runCommandLine(sb.toString());
    }

    public String getStudy() {
        return study;
    }

    public SamtoolsWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getCommand() {
        return command;
    }

    public SamtoolsWrapperAnalysisExecutor setCommand(String command) {
        this.command = command;
        return this;
    }

    public String getInputFile() {
        return inputFile;
    }

    public SamtoolsWrapperAnalysisExecutor setInputFile(String inputFile) {
        this.inputFile = inputFile;
        return this;
    }
}
