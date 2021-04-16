package org.opencb.opencga.analysis.wrappers.executors;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.wrappers.PicardWrapperAnalysis;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.exceptions.ToolException;

import java.io.FileNotFoundException;
import java.nio.file.Path;

public class PicardWrapperAnalysisExecutor extends OpenCgaWrapperAnalysisExecutor {

    private String command;

    public PicardWrapperAnalysisExecutor(String studyId, ObjectMap params, Path outDir, Path scratchDir, CatalogManager catalogManager,
                                         String token) {
//        super(studyId, params, outDir, scratchDir, catalogManager, token);
        this.sep = "=";
        this.shortPrefix = "";
        this.longPrefix = "";
    }

    @Override
    public void run() throws ToolException {
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
        } catch (FileNotFoundException e) {
            throw new ToolException(e);
        }
    }

//    @Override
//    protected String getId() {
//        return PicardWrapperAnalysis.ID;
//    }

    @Override
    protected String getDockerImageName() {
        return PicardWrapperAnalysis.PICARD_DOCKER_IMAGE;
    }

    private void runBedToIntervalList() throws ToolException {
//        String bedFilename = "";
//        if (StringUtils.isNotEmpty(params.getString("INPUT"))) {
//            bedFilename = params.getString("INPUT");
//        } else if (StringUtils.isNotEmpty(params.getString("I"))) {
//            bedFilename = params.getString("I");
//        }
//
//        String outFilename = "";
//        if (StringUtils.isNotEmpty(params.getString("OUTPUT"))) {
//            outFilename = params.getString("OUTPUT");
//        } else if (StringUtils.isNotEmpty(params.getString("O"))) {
//            outFilename = params.getString("O");
//        }
//
//        String dictFilename = "";
//        if (StringUtils.isNotEmpty(params.getString("SEQUENCE_DICTIONARY"))) {
//            dictFilename = params.getString("SEQUENCE_DICTIONARY");
//        } else if (StringUtils.isNotEmpty(params.getString("SD"))) {
//            dictFilename = params.getString("SD");
//        }
//
//        StringBuilder sb = initCommandLine();
//
//        // Append mounts
//        List<Pair<String, String>> inputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("I", bedFilename),
//                new ImmutablePair<>("SD", dictFilename)));
//        Map<String, String> srcTargetMap = new HashMap<>();
//        appendMounts(inputFilenames, srcTargetMap, sb);
//
//        // Append docker image, version and command
//        appendCommand("java -jar /usr/picard/picard.jar " + command, sb);
//
//        // Append input file params
//        appendInputFiles(inputFilenames, srcTargetMap, sb);
//
//        // Append output file params
//        List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("O", outFilename)));
//        appendOutputFiles(outputFilenames, sb);
//
//        // Append other params
//        Set<String> skipParams =  new HashSet<>(Arrays.asList("I", "INPUT", "O", "OUTPUT", "SEQUENCE_DICTIONARY","SD"));
//        appendOtherParams(skipParams, sb);
//
//        // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
//        runCommandLine(sb.toString());
    }

    private void runCollectWgsMetrics() throws ToolException, FileNotFoundException {
//        String bamFilename = "";
//        if (StringUtils.isNotEmpty(params.getString("INPUT"))) {
//            bamFilename = params.getString("INPUT");
//        } else if (StringUtils.isNotEmpty(params.getString("I"))) {
//            bamFilename = params.getString("I");
//        }
//
//        String outFilename = "";
//        if (StringUtils.isNotEmpty(params.getString("OUTPUT"))) {
//            outFilename = params.getString("OUTPUT");
//        } else if (StringUtils.isNotEmpty(params.getString("O"))) {
//            outFilename = params.getString("O");
//        }
//
//        String refFilename = "";
//        if (StringUtils.isNotEmpty(params.getString("REFERENCE_SEQUENCE"))) {
//            refFilename = params.getString("REFERENCE_SEQUENCE");
//        } else if (StringUtils.isNotEmpty(params.getString("R"))) {
//            refFilename = params.getString("R");
//        }
//
//        StringBuilder sb = initCommandLine();
//
//        // Append mounts
//        List<Pair<String, String>> inputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("I", bamFilename),
//                new ImmutablePair<>("R", refFilename)));
//        Map<String, String> srcTargetMap = new HashMap<>();
//        appendMounts(inputFilenames, srcTargetMap, sb);
//
//        // Append docker image, version and command
//        appendCommand("java -jar /usr/picard/picard.jar " + command, sb);
//
//        // Append input file params
//        appendInputFiles(inputFilenames, srcTargetMap, sb);
//
//        // Append output file params
//        List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("O", outFilename)));
//        appendOutputFiles(outputFilenames, sb);
//
//        // Append other params
//        Set<String> skipParams =  new HashSet<>(Arrays.asList("I", "INPUT", "O", "OUTPUT", "REFERENCE_SEQUENCE","R"));
//        appendOtherParams(skipParams, sb);
//
//        // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
//        runCommandLine(sb.toString());
    }

    private void runCollectHsMetrics() throws ToolException, FileNotFoundException {
//        List<Pair<String, String>> inputFilenames = new ArrayList<>();
//
//        String bamFilename = "";
//        if (StringUtils.isNotEmpty(params.getString("INPUT"))) {
//            bamFilename = params.getString("INPUT");
//        } else if (StringUtils.isNotEmpty(params.getString("I"))) {
//            bamFilename = params.getString("I");
//        }
//        if (StringUtils.isNotEmpty(bamFilename)) {
//            inputFilenames.add(new ImmutablePair<>("I", bamFilename));
//        }
//
//        String outFilename = "";
//        if (StringUtils.isNotEmpty(params.getString("OUTPUT"))) {
//            outFilename = params.getString("OUTPUT");
//        } else if (StringUtils.isNotEmpty(params.getString("O"))) {
//            outFilename = params.getString("O");
//        }
//
//        String refFilename = "";
//        if (StringUtils.isNotEmpty(params.getString("REFERENCE_SEQUENCE"))) {
//            refFilename = params.getString("REFERENCE_SEQUENCE");
//        } else if (StringUtils.isNotEmpty(params.getString("R"))) {
//            refFilename = params.getString("R");
//        }
//        if (StringUtils.isNotEmpty(refFilename)) {
//            inputFilenames.add(new ImmutablePair<>("R", refFilename));
//        }
//
//        String baitFilename = "";
//        if (StringUtils.isNotEmpty(params.getString("BAIT_INTERVALS"))) {
//            baitFilename = params.getString("BAIT_INTERVALS");
//        } else if (StringUtils.isNotEmpty(params.getString("BI"))) {
//            baitFilename = params.getString("BI");
//        }
//        if (StringUtils.isNotEmpty(baitFilename)) {
//            inputFilenames.add(new ImmutablePair<>("BI", baitFilename));
//        }
//
//        String targetFilename = "";
//        if (StringUtils.isNotEmpty(params.getString("TARGET_INTERVALS"))) {
//            targetFilename = params.getString("TARGET_INTERVALS");
//        } else if (StringUtils.isNotEmpty(params.getString("TI"))) {
//            targetFilename = params.getString("TI");
//        }
//        if (StringUtils.isNotEmpty(targetFilename)) {
//            inputFilenames.add(new ImmutablePair<>("TI", targetFilename));
//        }
//
//        StringBuilder sb = initCommandLine();
//
//        // Append mounts
//        Map<String, String> srcTargetMap = new HashMap<>();
//        appendMounts(inputFilenames, srcTargetMap, sb);
//
//        // Append docker image, version and command
//        appendCommand("java -jar /usr/picard/picard.jar " + command, sb);
//
//        // Append input file params
//        appendInputFiles(inputFilenames, srcTargetMap, sb);
//
//        // Append output file params
//        List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("O", outFilename)));
//        appendOutputFiles(outputFilenames, sb);
//
//        // Append other params
//        Set<String> skipParams =  new HashSet<>(Arrays.asList("I", "INPUT", "O", "OUTPUT", "BAIT_INTERVALS", "BI", "TARGET_INTERVALS", "TI",
//                "REFERENCE_SEQUENCE","R"));
//        appendOtherParams(skipParams, sb);
//
//        // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
//        runCommandLine(sb.toString());
    }

    public String getCommand() {
        return command;
    }

    public PicardWrapperAnalysisExecutor setCommand(String command) {
        this.command = command;
        return this;
    }
}
