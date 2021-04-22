package org.opencb.opencga.analysis.wrappers.fastqc;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@ToolExecutor(id = FastqcWrapperAnalysisExecutor.ID,
        tool = FastqcWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class FastqcWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public final static String ID = FastqcWrapperAnalysis.ID + "-local";

    private String study;
    private String inputFile;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public String getDockerImageName() {
        return "dceoy/fastqc";
    }

    @Override
    public String getDockerImageVersion() {
        return "";
    }

    @Override
    protected void run() throws Exception {
        StringBuilder sb = initCommandLine();

        // Append mounts
        List<Pair<String, String>> inputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("", getInputFile())));

        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand("", sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Append output file params
        List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("o", "")));
        appendOutputFiles(outputFilenames, sb);

        // Append other params
        Set<String> skipParams =  new HashSet<>(Arrays.asList("o", "outdir", "d", "dir"));
        appendOtherParams(skipParams, sb);

        // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
        logger.info("Docker command line: " + sb.toString());
        runCommandLine(sb.toString());
    }

    public String getStudy() {
        return study;
    }

    public FastqcWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getInputFile() {
        return inputFile;
    }

    public FastqcWrapperAnalysisExecutor setInputFile(String inputFile) {
        this.inputFile = inputFile;
        return this;
    }
}
