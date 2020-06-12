/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.analysis.wrappers;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.*;

@Tool(id = PicardWrapperAnalysis.ID, resource = Enums.Resource.ALIGNMENT,
        description = "")
public class PicardWrapperAnalysis extends OpenCgaWrapperAnalysis {

    public final static String ID = "picard";
    public final static String DESCRIPTION = "Picard is a set of command line tools (in Java) for manipulating high-throughput sequencing"
            + " (HTS) data and formats such as SAM/BAM/CRAM and VCF.";

    public final static String PICARD_DOCKER_IMAGE = "broadinstitute/picard";

    private String command;
//    private String fastaFile;
//    private String indexBaseFile;
//    private String fastq1File;
//    private String fastq2File;
//    private String samFilename;

    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(command)) {
            throw new ToolException("Missig Picard command.");
        }

        switch (command) {
            case "CollectHsMetrics":
            case "CollectWgsMetrics":
            case "BedToIntervalList":
                break;
            default:
                throw new ToolException("Picard tool name '" + command + "' is not available. Supported tools: CollectHsMetrics,"
                        + " CollectWgsMetrics, BedToIntervalList");
        }
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
//            String commandLine = getCommandLine();
//            logger.info("Picard command line:" + commandLine);
            try {
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
                        throw new ToolException("Picard tool name '" + command + "' is not available. Supported tools: CollectHsMetrics,"
                                + " CollectWgsMetrics, BedToIntervalList");
                }

//                // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
//                Command cmd = new Command(commandLine)
//                        .setOutputOutputStream(
//                                new DataOutputStream(new FileOutputStream(getScratchDir().resolve(STDOUT_FILENAME).toFile())))
//                        .setErrorOutputStream(
//                                new DataOutputStream(new FileOutputStream(getScratchDir().resolve(STDERR_FILENAME).toFile())));
//
//                cmd.run();

                // Check BWA errors by reading the stdout and stderr files
//                boolean success = false;
//                switch (command) {
//                    case "index": {
//                        File file = params.containsKey("p")
//                                ? new File(params.getString("p"))
//                                : new File(fileUriMap.get(fastaFile).getPath());
//                        String prefix = getOutDir().toAbsolutePath() + "/" + file.getName();
//                        String[] suffixes = new String[]{".sa", ".bwt", ".pac", ".amb", ".ann"};
//                        success = true;
//                        for (String suffix : suffixes) {
//                            if (!new File(prefix + suffix).exists()) {
//                                success = false;
//                                break;
//                            }
//                        }
//                        if (success) {
//                            // Get catalog path
//                            OpenCGAResult<org.opencb.opencga.core.models.file.File> fileResult;
//                            try {
//                                fileResult = catalogManager.getFileManager().get(getStudy(), fastaFile, QueryOptions.empty(), token);
//                            } catch (CatalogException e) {
//                                throw new ToolException("Error accessing file '" + fastaFile + "' of the study " + getStudy() + "'", e);
//                            }
//                            if (fileResult.getNumResults() <= 0) {
//                                throw new ToolException("File '" + fastaFile + "' not found in study '" + getStudy() + "'");
//                            }
//
//                            String catalogPath = fileResult.getResults().get(0).getPath();
//                            Path dest = new File(file.getParent()).toPath();
//
//                            for (String suffix : suffixes) {
//                                Path src = new File(prefix + suffix).toPath();
//
//                                System.out.println("src = " + src + ", dest = " + dest + ", catalog path = " + catalogPath.concat(suffix));
//                                moveFile(getStudy(), src, dest, catalogPath.concat(suffix), token);
//                            }
//                        }
//                        break;
//                    }
//                    case "mem": {
//                        if (new File(getOutDir() + "/" + samFilename).exists()) {
//                            success = true;
//                        }
//                        break;
//                    }
//                }
//                if (!success) {
//                    File file = new File(getScratchDir() + "/" + STDERR_FILENAME);
//                    String msg = "Something wrong executing BWA ";
//                    if (file.exists()) {
//                        msg = StringUtils.join(FileUtils.readLines(file, Charset.defaultCharset()), ". ");
//                    }
//                    throw new ToolException(msg);
//                }
            } catch (Exception e) {
                throw new ToolException(e);
            }
        });
    }

    private void runBedToIntervalList() throws ToolException, FileNotFoundException {
        String bedFilename = "";
        if (StringUtils.isNotEmpty(params.getString("INPUT"))) {
            bedFilename = params.getString("INPUT");
        } else if (StringUtils.isNotEmpty(params.getString("I"))) {
            bedFilename = params.getString("I");
        }

        String outFilename = "";
        if (StringUtils.isNotEmpty(params.getString("OUTPUT"))) {
            outFilename = params.getString("OUTPUT");
        } else if (StringUtils.isNotEmpty(params.getString("O"))) {
            outFilename = params.getString("O");
        }

        String dictFilename = "";
        if (StringUtils.isNotEmpty(params.getString("SEQUENCE_DICTIONARY"))) {
            dictFilename = params.getString("SEQUENCE_DICTIONARY");
        } else if (StringUtils.isNotEmpty(params.getString("SD"))) {
            dictFilename = params.getString("SD");
        }

        StringBuilder sb = initCommandLine();

        // Append mounts
        List<Pair<String, String>> inputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("I", bedFilename),
                new ImmutablePair<>("SD", dictFilename)));
        Map<String, String> srcTargetMap = new HashMap<>();
        appendMounts(inputFilenames, srcTargetMap, sb);

        // Append docker image, version and command
        appendCommand(sb);

        // Append input file params
        appendInputFiles(inputFilenames, srcTargetMap, sb);

        // Append output file params
        List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("O", outFilename)));
        appendOutputFiles(outputFilenames, sb);

        // Append other params
        Set<String> skipParams =  new HashSet<>(Arrays.asList("I", "INPUT", "O", "OUTPUT", "SEQUENCE_DICTIONARY","SD"));
        appendOtherParams(skipParams, sb);

        // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
        runCommandLine(sb.toString());

    }

    private void runCollectWgsMetrics() throws ToolException, FileNotFoundException {
        String bamFilename = "";
        if (StringUtils.isNotEmpty(params.getString("INPUT"))) {
            bamFilename = params.getString("INPUT");
        } else if (StringUtils.isNotEmpty(params.getString("I"))) {
            bamFilename = params.getString("I");
        }

        String outFilename = "";
        if (StringUtils.isNotEmpty(params.getString("OUTPUT"))) {
            outFilename = params.getString("OUTPUT");
        } else if (StringUtils.isNotEmpty(params.getString("O"))) {
            outFilename = params.getString("O");
        }

        String refFilename = "";
        if (StringUtils.isNotEmpty(params.getString("REFERENCE_SEQUENCE"))) {
            refFilename = params.getString("REFERENCE_SEQUENCE");
        } else if (StringUtils.isNotEmpty(params.getString("R"))) {
            refFilename = params.getString("R");
        }

        StringBuilder sb = initCommandLine();

        // Append mounts
        List<Pair<String, String>> inputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("I", bamFilename),
                new ImmutablePair<>("R", refFilename)));
        Map<String, String> srcTargetMap = new HashMap<>();
        appendMounts(inputFilenames, srcTargetMap, sb);

        // Append docker image, version and command
        appendCommand(sb);

        // Append input file params
        appendInputFiles(inputFilenames, srcTargetMap, sb);

        // Append output file params
        List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("O", outFilename)));
        appendOutputFiles(outputFilenames, sb);

        // Append other params
        Set<String> skipParams =  new HashSet<>(Arrays.asList("I", "INPUT", "O", "OUTPUT", "REFERENCE_SEQUENCE","R"));
        appendOtherParams(skipParams, sb);

        // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
        runCommandLine(sb.toString());
    }

    private void runCollectHsMetrics() throws ToolException, FileNotFoundException {
        String bamFilename = "";
        if (StringUtils.isNotEmpty(params.getString("INPUT"))) {
            bamFilename = params.getString("INPUT");
        } else if (StringUtils.isNotEmpty(params.getString("I"))) {
            bamFilename = params.getString("I");
        }

        String outFilename = "";
        if (StringUtils.isNotEmpty(params.getString("OUTPUT"))) {
            outFilename = params.getString("OUTPUT");
        } else if (StringUtils.isNotEmpty(params.getString("O"))) {
            outFilename = params.getString("O");
        }

        String refFilename = "";
        if (StringUtils.isNotEmpty(params.getString("REFERENCE_SEQUENCE"))) {
            refFilename = params.getString("REFERENCE_SEQUENCE");
        } else if (StringUtils.isNotEmpty(params.getString("R"))) {
            refFilename = params.getString("R");
        }

        String baitFilename = "";
        if (StringUtils.isNotEmpty(params.getString("BAIT_INTERVALS"))) {
            baitFilename = params.getString("BAIT_INTERVALS");
        } else if (StringUtils.isNotEmpty(params.getString("BI"))) {
            baitFilename = params.getString("BI");
        }

        String targetFilename = "";
        if (StringUtils.isNotEmpty(params.getString("TARGET_INTERVALS"))) {
            targetFilename = params.getString("TARGET_INTERVALS");
        } else if (StringUtils.isNotEmpty(params.getString("TI"))) {
            targetFilename = params.getString("TI");
        }

        StringBuilder sb = initCommandLine();

        // Append mounts
        List<Pair<String, String>> inputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("I", bamFilename),
                new ImmutablePair<>("R", refFilename), new ImmutablePair<>("BI", baitFilename), new ImmutablePair<>("TI", targetFilename)));
        Map<String, String> srcTargetMap = new HashMap<>();
        appendMounts(inputFilenames, srcTargetMap, sb);

        // Append docker image, version and command
        appendCommand(sb);

        // Append input file params
        appendInputFiles(inputFilenames, srcTargetMap, sb);

        // Append output file params
        List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("O", outFilename)));
        appendOutputFiles(outputFilenames, sb);

        // Append other params
        Set<String> skipParams =  new HashSet<>(Arrays.asList("I", "INPUT", "O", "OUTPUT", "BAIT_INTERVALS", "BI", "TARGET_INTERVALS", "TI",
                "REFERENCE_SEQUENCE","R"));
        appendOtherParams(skipParams, sb);

        // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
        runCommandLine(sb.toString());
    }

    @Override
    public String getDockerImageName() {
        return PICARD_DOCKER_IMAGE;
    }

    private StringBuilder initCommandLine() {
        return new StringBuilder("docker run ");
    }

    private void appendMounts(List<Pair<String, String>> inputFilenames, Map<String, String> srcTargetMap, StringBuilder sb) throws ToolException {
        // Mount input dirs
        for (Pair<String, String> pair : inputFilenames) {
            updateFileMaps(pair.getValue(), sb, fileUriMap, srcTargetMap);
        }

        // Mount output dir
        sb.append("--mount type=bind,source=\"").append(getOutDir().toAbsolutePath()).append("\",target=\"").append(DOCKER_OUTPUT_PATH)
                .append("\" ");
    }

    private void appendCommand(StringBuilder sb) {
        // Docker image and version
        sb.append(getDockerImageName());
        if (params.containsKey(DOCKER_IMAGE_VERSION_PARAM)) {
            sb.append(":").append(params.getString(DOCKER_IMAGE_VERSION_PARAM));
        }

        // Picard command
        sb.append(" java -jar /usr/picard/picard.jar ").append(command);
    }

    private void appendInputFiles(List<Pair<String, String>> inputFilenames, Map<String, String> srcTargetMap, StringBuilder sb) {
        File file;
        for (Pair<String, String> pair : inputFilenames) {
            file = new File(fileUriMap.get(pair.getValue()).getPath());
            sb.append(" ").append(pair.getKey()).append("=").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/")
                    .append(file.getName());
        }
    }

    private void appendOutputFiles(List<Pair<String, String>> outputFilenames, StringBuilder sb) {
        for (Pair<String, String> pair : outputFilenames) {
            sb.append(" ").append(pair.getKey()).append("=").append(DOCKER_OUTPUT_PATH).append("/").append(pair.getValue());
        }
    }

    private void appendOtherParams(Set<String> skipParams, StringBuilder sb) {
        for (String paramName : params.keySet()) {
            if (skipParams.contains(paramName)) {
                continue;
            }
            sb.append(" ").append(paramName);
            if (StringUtils.isNotEmpty(params.getString(paramName))) {
                sb.append("=").append(params.getString(paramName));
            }
        }
    }

    private void runCommandLine(String cmdline) throws FileNotFoundException {
        System.out.println("Docker command line:\n" + cmdline);
        new Command(cmdline)
                .setOutputOutputStream(
                        new DataOutputStream(new FileOutputStream(getScratchDir().resolve(STDOUT_FILENAME).toFile())))
                .setErrorOutputStream(
                        new DataOutputStream(new FileOutputStream(getScratchDir().resolve(STDERR_FILENAME).toFile())))
                .run();
    }

    private boolean checkParam(String param) {
        if (param.equals(DOCKER_IMAGE_VERSION_PARAM)) {
            return false;
        } else if ("index".equals(command) && "p".equals(param)) {
            return false;
        } else if ("mem".equals(command) && "o".equals(param)) {
            return false;
        }
        return true;
    }


    public String getCommand() {
        return command;
    }

    public PicardWrapperAnalysis setCommand(String command) {
        this.command = command;
        return this;
    }

//    public String getFastaFile() {
//        return fastaFile;
//    }
//
//    public PicardWrapperAnalysis setFastaFile(String fastaFile) {
//        this.fastaFile = fastaFile;
//        return this;
//    }
//
//    public String getIndexBaseFile() {
//        return indexBaseFile;
//    }
//
//    public PicardWrapperAnalysis setIndexBaseFile(String indexBaseFile) {
//        this.indexBaseFile = indexBaseFile;
//        return this;
//    }
//
//    public String getFastq1File() {
//        return fastq1File;
//    }
//
//    public PicardWrapperAnalysis setFastq1File(String fastq1File) {
//        this.fastq1File = fastq1File;
//        return this;
//    }
//
//    public String getFastq2File() {
//        return fastq2File;
//    }
//
//    public PicardWrapperAnalysis setFastq2File(String fastq2File) {
//        this.fastq2File = fastq2File;
//        return this;
//    }
//
//    public String getSamFilename() {
//        return samFilename;
//    }
//
//    public PicardWrapperAnalysis setSamFilename(String samFilename) {
//        this.samFilename = samFilename;
//        return this;
//    }
}
