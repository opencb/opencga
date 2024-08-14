package org.opencb.opencga.analysis.wrappers.fastqc;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.opencb.opencga.analysis.wrappers.fastqc.FastqcWrapperAnalysis.FASTQC_DOCKER_CLI_KEY;

@ToolExecutor(id = FastqcWrapperAnalysisExecutor.ID,
        tool = FastqcWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class FastqcWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public  static final String ID = FastqcWrapperAnalysis.ID + "-local";

    private String inputFile;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    protected void run() throws Exception {
        addStepParams();

        StringBuilder sb = initCommandLine();

        // Append mounts
        List<Pair<String, String>> inputFilenames = DockerWrapperAnalysisExecutor.getInputFilenames(getInputFile(),
                FastqcWrapperAnalysis.FILE_PARAM_NAMES, getExecutorParams());
        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand("fastqc", sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Append output file params
        List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("o", "")));
        appendOutputFiles(outputFilenames, sb);

        // Append other params
        Set<String> skipParams =  new HashSet<>(Arrays.asList("o", "outdir", "d", "dir", "j", "java"));
        appendOtherParams(skipParams, sb);

        // Execute command and redirect stdout and stderr to the files
        logger.info("Docker command line: {}", sb);
        addAttribute(FASTQC_DOCKER_CLI_KEY, sb);
        runCommandLine(sb.toString());
    }

    private void addStepParams() throws ToolException {
        addAttribute("INPUT_FILE", inputFile);
    }

    public String getInputFile() {
        return inputFile;
    }

    public FastqcWrapperAnalysisExecutor setInputFile(String inputFile) {
        this.inputFile = inputFile;
        return this;
    }
}
