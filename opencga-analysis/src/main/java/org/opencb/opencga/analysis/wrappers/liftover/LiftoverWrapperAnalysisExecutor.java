package org.opencb.opencga.analysis.wrappers.liftover;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.plink.PlinkWrapperAnalysis;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@ToolExecutor(id = LiftoverWrapperAnalysisExecutor.ID,
        tool = LiftoverWrapperAnalysis.ID,
        source = ToolExecutor.Source.FILE,
        framework = ToolExecutor.Framework.LOCAL)
public class LiftoverWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public final static String ID = LiftoverWrapperAnalysis.ID + "-local";

    private String study;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    protected void run() throws Exception {
        StringBuilder sb = initCommandLine();

        // Append mounts
        List<Pair<String, String>> inputFilenames = DockerWrapperAnalysisExecutor.getInputFilenames(null,
                PlinkWrapperAnalysis.FILE_PARAM_NAMES, getExecutorParams());
        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand("bcftools +liftover", sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Append output file params
        List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("out", "")));
        appendOutputFiles(outputFilenames, sb);

        // Append other params
        Set<String> skipParams =  new HashSet<>(Arrays.asList("out", "noweb"));
        skipParams.addAll(PlinkWrapperAnalysis.FILE_PARAM_NAMES);
        appendOtherParams(skipParams, sb);

        // Execute command and redirect stdout and stderr to the files
        logger.info("Docker command line: " + sb.toString());
        runCommandLine(sb.toString());
    }

    public String getStudy() {
        return study;
    }

    public LiftoverWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }
}
