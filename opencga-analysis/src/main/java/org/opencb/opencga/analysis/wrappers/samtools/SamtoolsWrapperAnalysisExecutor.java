package org.opencb.opencga.analysis.wrappers.samtools;

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
    private String referenceFile;
    private String readGroupFile;
    private String bedFile;
    private String refSeqFile;
    private String referenceNamesFile;
    private String targetRegionFile;
    private String readsNotSelectedFilename;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public String getDockerImageName() {
        return "opencb/opencga-samtools";
    }

    @Override
    public String getDockerImageVersion() {
        return GitRepositoryState.get().getBuildVersion();
    }

    @Override
    public void run() throws ToolException {
        switch (command) {
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

    private void runStats() throws ToolException {
        StringBuilder sb = initCommandLine();

        // Append mounts
        List<Pair<String, String>> inputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("", getInputFile()),
                new ImmutablePair<>("reference", getReferenceFile()), new ImmutablePair<>("t", getTargetRegionFile()),
                new ImmutablePair<>("ref-seq", getRefSeqFile())));

        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand("samtools " + command, sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Append other params
        Set<String> skipParams =  new HashSet<>(Arrays.asList("reference", "t", "target-regions", "ref-seq", "r"));
        appendOtherParams(skipParams, sb);

        // Execute command and redirect stdout and stderr to the files
        logger.info("Docker command line: " + sb.toString());
        runCommandLine(sb.toString());
    }

    private void runPlotBamStats() throws ToolException {
        StringBuilder sb = initCommandLine();

        // Append mounts
        List<Pair<String, String>> inputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("", getInputFile())));

        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand(command, sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Append output file params
        List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("p", " ")));
        appendOutputFiles(outputFilenames, sb);

        // Append other params
        Set<String> skipParams =  new HashSet<>(Arrays.asList("p", "prefix", "r", "ref-stats", "s", "do-ref-stats", "t", "targets"));
        appendOtherParams(skipParams, sb);

        // Execute command and redirect stdout and stderr to the files
        logger.info("Docker command line: " + sb.toString());
        runCommandLine(sb.toString());
    }

    private void runFlagstat() throws ToolException {
        StringBuilder sb = initCommandLine();

        // Append mounts
        List<Pair<String, String>> inputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("", getInputFile())));

        Map<String, String> mountMap = appendMounts(inputFilenames, sb);

        // Append docker image, version and command
        appendCommand("samtools " + command, sb);

        // Append input file params
        appendInputFiles(inputFilenames, mountMap, sb);

        // Append other params
        Set<String> skipParams =  new HashSet<>(Arrays.asList("input-fmt-option"));
        appendOtherParams(skipParams, sb);

        // Execute command and redirect stdout and stderr to the files
        logger.info("Docker command line: " + sb.toString());
        runCommandLine(sb.toString());
    }


    /*
    @Override
    protected void run() throws Exception {
        outputFilename = getOutputFilename();
        if (StringUtils.isNotEmpty(outputFilename)) {
            // Set output file to use further
            outputFile = getOutDir().resolve(outputFilename).toFile();
        }

        String commandLine = getCommandLine();

        logger.info("Samtools command line: " + commandLine);
        try {
            // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
            Command cmd = new Command(commandLine)
                    .setOutputOutputStream(
                            new DataOutputStream(new FileOutputStream(getOutDir().resolve(STDOUT_FILENAME).toFile())))
                    .setErrorOutputStream(
                            new DataOutputStream(new FileOutputStream(getOutDir().resolve(STDERR_FILENAME).toFile())));

            cmd.run();

            // Check samtools errors
            boolean success = false;
            switch (getCommand()) {
                case "dict":
                case "index":
                case "sort":
                case "view": {
                    if (outputFile.exists()) {
                        success = true;

//                        if (!"view".equals(command)) {
//                            String catalogPath = getCatalogPath(inputFile);
//                            File file = new File(fileUriMap.get(inputFile).getPath());
//
//                            Path src = outputFile.toPath();
//                            Path dest = new File(file.getParent()).toPath();
//
//                            moveFile(getStudy(), src, dest, catalogPath, token);
//                        }
                    }
                    break;
                }
//                case "faidx": {
//                    File file = new File(fileUriMap.get(inputFile).getPath());
//                    File faidxFile = file.getParentFile().toPath().resolve(file.getName() + ".fai").toFile();
//                    success = isValidFile(faidxFile);
//                    if (success) {
//                        String catalogPath = getCatalogPath(inputFile);
//                        catalogManager.getFileManager().link(getStudy(), faidxFile.toURI(), catalogPath, new ObjectMap("parents", true),
//                                token);
//                    }
//                    break;
//                }
                case "flagstat": {
                    File file = getOutDir().resolve(STDOUT_FILENAME).toFile();
                    List<String> lines = readLines(file, Charset.defaultCharset());
                    if (lines.size() > 0 && lines.get(0).contains("QC-passed")) {
                        FileUtils.copyFile(file, outputFile);
                        success = true;
                    }
                    break;
                }
                case "stats": {
                    File file = getOutDir().resolve(STDOUT_FILENAME).toFile();
                    List<String> lines = readLines(file, Charset.defaultCharset());
                    if (lines.size() > 0 && lines.get(0).startsWith("# This file was produced by samtools stats")) {
                        FileUtils.copyFile(file, outputFile);
                        success = true;
                    }
                    break;
                }
                case "depth": {
                    File file = new File(getOutDir() + "/" + STDOUT_FILENAME);
                    if (file.exists() && file.length() > 0) {
                        FileUtils.copyFile(file, outputFile);
                        success = true;
                    }
                    break;
                }
                case "plot-bamstats": {
                    int pngCounter = 0;
                    for (File file : getOutDir().toFile().listFiles()) {
                        if (file.getName().endsWith("png")) {
                            pngCounter++;
                        }
                    }
                    success = (pngCounter == 11);
                    break;
                }
            }

            if (!success) {
                File file = getOutDir().resolve(STDERR_FILENAME).toFile();
                String msg = "Something wrong happened when executing Samtools";
                if (file.exists()) {
                    msg = StringUtils.join(readLines(file, Charset.defaultCharset()), ". ");
                }
                throw new ToolException(msg);
            }
        } catch (Exception e) {
            throw new ToolException(e);
        }
    }

    private String getOutputFilename() throws ToolException {
        String outputFilename = null;
        String prefix = Paths.get(getInputFile()).toFile().getName();
        switch (getCommand()) {
            case "index": {
                if (prefix.endsWith("cram")) {
                    outputFilename = prefix + ".crai";
                } else {
                    outputFilename = prefix + ".bai";
                }
                break;
            }
            case "faidx": {
                outputFilename = prefix + ".fai";
                break;
            }
            case "dict": {
                outputFilename = prefix + ".dict";
                break;
            }
            case "stats": {
                outputFilename = prefix + ".stats.txt";
                break;
            }
            case "flagstat": {
                outputFilename = prefix + ".flagstats.txt";
                break;
            }
            case "depth": {
                outputFilename = prefix + ".depth.txt";
                break;
            }
            case "plot-bamstats":
                break;
            default: {
                throw new ToolException("Missing output file name when executing 'samtools " + getCommand() + "'.");
            }
        }

        return outputFilename;
    }

    public String getCommandLine() throws ToolException {

        List<String> inputFiles = Arrays.asList(getInputFile(), getReferenceFile(), getReadGroupFile(), getBedFile(), getRefSeqFile(),
                getReferenceNamesFile(), getTargetRegionFile(), getReadsNotSelectedFilename());

        Map<String, String> srcTargetMap = getDockerMountMap(inputFiles);

        StringBuilder sb = initDockerCommandLine(srcTargetMap, getDockerImageName(), getDockerImageVersion());

        // Samtools command
        if ("plot-bamstats".equals(getCommand())) {
            sb.append(" ").append(getCommand());
        } else {
            sb.append(" samtools ").append(getCommand());
        }

        // Samtools options
        for (String param : getExecutorParams().keySet()) {
            if (skipParameter(param)) {
                String sep = param.length() == 1 ? " -" : " --";
                String value = getExecutorParams().getString(param);
                if (StringUtils.isEmpty(value)) {
                    sb.append(sep).append(param);
                } else {
                    switch (value.toLowerCase()) {
                        case "false":
                            // Nothing to do
                            break;
                        case "null":
                        case "true":
                            // Only param must be appended
                            sb.append(sep).append(param);
                            break;
                        default:
                            // Otherwise, param + value must be appended
                            sb.append(sep).append(param).append(" ").append(value);
                            break;
                    }
                }
            }
        }

        // File parameters
        File file;
        switch (getCommand()) {
            case "depth": {
                if (StringUtils.isNotEmpty(getReferenceFile())) {
                    file = new File(getReferenceFile());
                    sb.append(" --reference ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/")
                            .append(file.getName());
                }
                if (StringUtils.isNotEmpty(getBedFile())) {
                    file = new File(getBedFile());
                    sb.append(" -b ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                }
                file = new File(getInputFile());
                sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                break;
            }
            case "faidx": {
                file = new File(getInputFile());
                sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                break;
            }
            case "stats": {
                if (StringUtils.isNotEmpty(getReferenceFile())) {
                    file = new File(getReferenceFile());
                    sb.append(" --reference ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/")
                            .append(file.getName());
                }
                if (StringUtils.isNotEmpty(getTargetRegionFile())) {
                    file = new File(getTargetRegionFile());
                    sb.append(" -t ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                }
                if (StringUtils.isNotEmpty(getRefSeqFile())) {
                    file = new File(getRefSeqFile());
                    sb.append(" --ref-seq ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/")
                            .append(file.getName());
                }
                file = new File(getInputFile());
                sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                break;
            }
            case "flagstat": {
                file = new File(getInputFile());
                sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                break;
            }
            case "index": {
                file = new File(getInputFile());
                sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                sb.append(" ").append(DOCKER_OUTPUT_PATH).append("/").append(outputFilename);
                break;
            }
            case "dict": {
                sb.append(" -o ").append(DOCKER_OUTPUT_PATH).append("/").append(outputFilename);

                file = new File(getInputFile());
                sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                break;
            }
            case "sort": {
                if (StringUtils.isNotEmpty(getReferenceFile())) {
                    file = new File(getReferenceFile());
                    sb.append(" --reference ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/")
                            .append(file.getName());
                }
                sb.append(" -o ").append(DOCKER_OUTPUT_PATH).append("/").append(outputFilename);

                file = new File(getInputFile());
                sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                break;
            }
            case "view": {
                if (StringUtils.isNotEmpty(getReferenceFile())) {
                    file = new File(getReferenceFile());
                    sb.append(" --reference ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/")
                            .append(file.getName());
                }
                if (StringUtils.isNotEmpty(getBedFile())) {
                    file = new File(getBedFile());
                    sb.append(" -L ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                }
                if (StringUtils.isNotEmpty(getReadGroupFile())) {
                    file = new File(getReadGroupFile());
                    sb.append(" -R ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                }
                if (StringUtils.isNotEmpty(getReadsNotSelectedFilename())) {
                    sb.append(" -U ").append(DOCKER_OUTPUT_PATH).append("/").append(getReadsNotSelectedFilename());
                }
                if (StringUtils.isNotEmpty(getReferenceNamesFile())) {
                    file = new File(getReferenceNamesFile());
                    sb.append(" -t ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                }
                sb.append(" -o ").append(DOCKER_OUTPUT_PATH).append("/").append(outputFilename);

                file = new File(getInputFile());
                sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                break;
            }
            case "plot-bamstats": {
                file = new File(getInputFile());
                sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                sb.append(" -p ").append(DOCKER_OUTPUT_PATH).append("/");
                break;
            }
        }

        return sb.toString();
    }

    @Override
    protected boolean skipParameter(String param) {
        if (!super.skipParameter(param)) {
            return false;
        }

        switch (getCommand()) {
            case "dict": {
                if ("o".equals(param)) {
                    return false;
                }
                break;
            }
            case "view": {
                switch (param) {
                    case "t":
                    case "L":
                    case "U":
                    case "R":
                    case "T":
                    case "reference":
                    case "o": {
                        return false;
                    }
                }
                break;
            }
            case "stats": {
                switch (param) {
                    case "reference":
                    case "r":
                    case "ref-seq":
                    case "t": {
                        return false;
                    }
                }
                break;
            }
            case "flagstat": {
                switch (param) {
                    case "input-fmt-option": {
                        return false;
                    }
                }
                break;
            }
            case "sort": {
                switch (param) {
                    case "reference":
                    case "o": {
                        return false;
                    }
                }
                break;
            }
            case "depth": {
                switch (param) {
                    case "b":
                    case "reference": {
                        return false;
                    }
                }
                break;
            }
            case "plot-bamstats": {
                switch (param) {
                    case "p": {
                        return false;
                    }
                }
                break;
            }
        }
        return true;
    }
*/
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

    public String getReferenceFile() {
        return referenceFile;
    }

    public SamtoolsWrapperAnalysisExecutor setReferenceFile(String referenceFile) {
        this.referenceFile = referenceFile;
        return this;
    }

    public String getReadGroupFile() {
        return readGroupFile;
    }

    public SamtoolsWrapperAnalysisExecutor setReadGroupFile(String readGroupFile) {
        this.readGroupFile = readGroupFile;
        return this;
    }

    public String getBedFile() {
        return bedFile;
    }

    public SamtoolsWrapperAnalysisExecutor setBedFile(String bedFile) {
        this.bedFile = bedFile;
        return this;
    }

    public String getRefSeqFile() {
        return refSeqFile;
    }

    public SamtoolsWrapperAnalysisExecutor setRefSeqFile(String refSeqFile) {
        this.refSeqFile = refSeqFile;
        return this;
    }

    public String getReferenceNamesFile() {
        return referenceNamesFile;
    }

    public SamtoolsWrapperAnalysisExecutor setReferenceNamesFile(String referenceNamesFile) {
        this.referenceNamesFile = referenceNamesFile;
        return this;
    }

    public String getTargetRegionFile() {
        return targetRegionFile;
    }

    public SamtoolsWrapperAnalysisExecutor setTargetRegionFile(String targetRegionFile) {
        this.targetRegionFile = targetRegionFile;
        return this;
    }

    public String getReadsNotSelectedFilename() {
        return readsNotSelectedFilename;
    }

    public SamtoolsWrapperAnalysisExecutor setReadsNotSelectedFilename(String readsNotSelectedFilename) {
        this.readsNotSelectedFilename = readsNotSelectedFilename;
        return this;
    }
}
