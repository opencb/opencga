package org.opencb.opencga.analysis.wrappers.bwa;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@ToolExecutor(id = BwaWrapperAnalysisExecutor.ID,
        tool = BwaWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class BwaWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public final static String ID = BwaWrapperAnalysis.ID + "-local";

    private String study;
    private String command;
    private String fastaFile;
    private String fastq1File;
    private String fastq2File;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public String getDockerImageName() {
        return "alexcoppe/bwa";
    }

    @Override
    public String getDockerImageVersion() {
        return null;
    }

    @Override
    public void run() throws ToolException {
        switch (command) {
            case "index":
                runIndex();
                break;
            case "mem":
                runMem();
                break;
            default:
                throw new ToolException("BWA command '" + command + "' is not supported yet.");
        }
    }

    private void runIndex() throws ToolException {
        StringBuilder sb = initCommandLine();

        // Append mounts
        List<Pair<String, String>> inputFilenames = DockerWrapperAnalysisExecutor.getInputFilenames(getFastaFile(), null,
                getExecutorParams());
        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand(command, sb);

        // Append other params
        appendOtherParams(null, sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Execute command and redirect stdout and stderr to the files
        logger.info("Docker command line: " + sb.toString());
        runCommandLine(sb.toString());
    }

    private void runMem() throws ToolException {
        StringBuilder sb = initCommandLine();

        // Append mounts
        List<Pair<String, String>> inputFilenames = new ArrayList<>();
        inputFilenames.add(new ImmutablePair<>("", fastaFile));
        inputFilenames.add(new ImmutablePair<>("", fastq1File));
        if (StringUtils.isNotEmpty(fastq2File)) {
            inputFilenames.add(new ImmutablePair<>("", fastq2File));
        }
        boolean hIsFile = false;
        String hParam = "H";
        if (getExecutorParams().containsKey(hParam) && StringUtils.isNotEmpty(getExecutorParams().getString(hParam))) {
            if (!getExecutorParams().getString(hParam).startsWith("@")) {
                Pair<String, String> pair = new ImmutablePair<>("H", getExecutorParams().getString("H"));
                inputFilenames.add(pair);
                hIsFile = true;
            }
        }
        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand(command, sb);

        // Append other params
        Set<String> skipParams = new HashSet<>(Arrays.asList("o"));
        if (hIsFile) {
            skipParams.add(hParam);
        }
        appendOtherParams(skipParams, sb);

        // Append output file params
        if (getExecutorParams().containsKey("o")) {
            String value = getExecutorParams().getString("o");
            if (StringUtils.isNotEmpty(value)) {
                List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("o", value)));
                appendOutputFiles(outputFilenames, sb);
            }
        }

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Execute command and redirect stdout and stderr to the files
        logger.info("Docker command line: " + sb.toString());
        runCommandLine(sb.toString());
    }

    public String getStudy() {
        return study;
    }

    public BwaWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getCommand() {
        return command;
    }

    public BwaWrapperAnalysisExecutor setCommand(String command) {
        this.command = command;
        return this;
    }

    public String getFastaFile() {
        return fastaFile;
    }

    public BwaWrapperAnalysisExecutor setFastaFile(String fastaFile) {
        this.fastaFile = fastaFile;
        return this;
    }

    public String getFastq1File() {
        return fastq1File;
    }

    public BwaWrapperAnalysisExecutor setFastq1File(String fastq1File) {
        this.fastq1File = fastq1File;
        return this;
    }

    public String getFastq2File() {
        return fastq2File;
    }

    public BwaWrapperAnalysisExecutor setFastq2File(String fastq2File) {
        this.fastq2File = fastq2File;
        return this;
    }
}
