package org.opencb.opencga.analysis.wrappers.picard;

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
    private String bamFile;
    private String bedFile;
    private String baitIntervalsFile;
    private String targetIntervalsFile;
    private String dictFile;
    private String refSeqFile;
    private String outFile;

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
                runCollectHsMetrics();
                break;
            case "CollectWgsMetrics":
                runCollectWgsMetrics();
                break;
            case "BedToIntervalList":
                runBedToIntervalList();
                break;
            default:
                throw new ToolException("Picard tool name '" + command + "' is not supported yet.");
        }
    }

    private void runBedToIntervalList() throws ToolException {
        StringBuilder sb = initCommandLine();

        // Append mounts
        List<Pair<String, String>> inputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("I", getBedFile()),
                new ImmutablePair<>("SD", getDictFile())));

        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand("java -jar /usr/picard/picard.jar " + command, sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Append output file params
        List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("O", getOutFile())));
        appendOutputFiles(outputFilenames, sb);

        // Append other params
        Set<String> skipParams =  new HashSet<>(Arrays.asList("I", "INPUT", "O", "OUTPUT", "SEQUENCE_DICTIONARY","SD"));
        appendOtherParams(skipParams, sb);

        // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
        logger.info("Docker command line: " + sb.toString());
        runCommandLine(sb.toString());
    }

    private void runCollectWgsMetrics() throws ToolException {
        StringBuilder sb = initCommandLine();

        // Append mounts
        List<Pair<String, String>> inputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("I", getBamFile()),
                new ImmutablePair<>("R", getRefSeqFile())));

        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand("java -jar /usr/picard/picard.jar " + command, sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Append output file params
        List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("O", getOutFile())));
        appendOutputFiles(outputFilenames, sb);

        // Append other params
        Set<String> skipParams =  new HashSet<>(Arrays.asList("I", "INPUT", "O", "OUTPUT", "REFERENCE_SEQUENCE","R"));
        appendOtherParams(skipParams, sb);

        // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
        logger.info("Docker command line: " + sb.toString());
        runCommandLine(sb.toString());
    }

    private void runCollectHsMetrics() throws ToolException {
        // Prepare input files
        List<Pair<String, String>> inputFilenames = new ArrayList<>(Arrays.asList(
                new ImmutablePair<>("I", getBamFile()),
                new ImmutablePair<>("R", getRefSeqFile()),
                new ImmutablePair<>("BI", getBaitIntervalsFile()),
                new ImmutablePair<>("TI", getTargetIntervalsFile())
        ));

        // Prepare output files
        List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(
                new ImmutablePair<>("O", getOutFile())
        ));

        StringBuilder sb = initCommandLine();

        // Append mounts
        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand("java -jar /usr/picard/picard.jar " + command, sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Append output file params
        appendOutputFiles(outputFilenames, sb);

        // Append other params
        Set<String> skipParams =  new HashSet<>(Arrays.asList("I", "INPUT", "O", "OUTPUT", "BAIT_INTERVALS", "BI", "TARGET_INTERVALS", "TI",
                "REFERENCE_SEQUENCE","R"));
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

    public String getBamFile() {
        return bamFile;
    }

    public PicardWrapperAnalysisExecutor setBamFile(String bamFile) {
        this.bamFile = bamFile;
        return this;
    }

    public String getBedFile() {
        return bedFile;
    }

    public PicardWrapperAnalysisExecutor setBedFile(String bedFile) {
        this.bedFile = bedFile;
        return this;
    }

    public String getBaitIntervalsFile() {
        return baitIntervalsFile;
    }

    public PicardWrapperAnalysisExecutor setBaitIntervalsFile(String baitIntervalsFile) {
        this.baitIntervalsFile = baitIntervalsFile;
        return this;
    }

    public String getTargetIntervalsFile() {
        return targetIntervalsFile;
    }

    public PicardWrapperAnalysisExecutor setTargetIntervalsFile(String targetIntervalsFile) {
        this.targetIntervalsFile = targetIntervalsFile;
        return this;
    }

    public String getDictFile() {
        return dictFile;
    }

    public PicardWrapperAnalysisExecutor setDictFile(String dictFile) {
        this.dictFile = dictFile;
        return this;
    }

    public String getRefSeqFile() {
        return refSeqFile;
    }

    public PicardWrapperAnalysisExecutor setRefSeqFile(String refSeqFile) {
        this.refSeqFile = refSeqFile;
        return this;
    }

    public String getOutFile() {
        return outFile;
    }

    public PicardWrapperAnalysisExecutor setOutFile(String outFile) {
        this.outFile = outFile;
        return this;
    }
}
