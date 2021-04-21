package org.opencb.opencga.analysis.wrappers.executors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.formats.sequence.fastqc.FastQcMetrics;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.wrappers.FastqcWrapperAnalysis;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.exceptions.ToolException;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.*;

public class FastqcWrapperAnalysisExecutor extends OpenCgaWrapperAnalysisExecutor {

    private String file;

    public FastqcWrapperAnalysisExecutor(String studyId, ObjectMap params, Path outDir, Path scratchDir, CatalogManager catalogManager,
                                         String token) {
//        super(studyId, params, outDir, scratchDir, catalogManager, token);

        sep = " ";
        shortPrefix = "-";
        longPrefix = "--";
    }

    @Override
    public void run() throws ToolException {
        StringBuilder sb = initCommandLine();

        // Append mounts
        List<Pair<String, String>> inputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("", file)));
        Map<String, String> srcTargetMap = new HashMap<>();
        try {
            appendMounts(inputFilenames, srcTargetMap, sb);
        } catch (URISyntaxException e) {
            throw new ToolException(e);
        }

        // Append docker image, version and command
        appendCommand("", sb);

        // Append input file params
        appendInputFiles(inputFilenames, srcTargetMap, sb);

        // Append other params
        Set<String> skipParams =  new HashSet<>(Arrays.asList("o", "output"));
        appendOtherParams(skipParams, sb);

        // Append output file params
        List<Pair<String, String>> outputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("o", "")));
        appendOutputFiles(outputFilenames, sb);

        // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
        runCommandLine(sb.toString());
    }

    public FastQcMetrics getResult() throws ToolException {
        return null;
//        File[] files = outDir.toFile().listFiles();
//        if (files != null) {
//            for (File file : files) {
//                if (file.getName().endsWith("_fastqc") && file.isDirectory()) {
//                    try {
//                        Path dataPath = file.toPath().resolve("fastqc_data.txt");
//                        return FastQcParser.parse(dataPath.toFile());
//                    } catch (IOException e) {
//                        throw new ToolException(e);
//                    }
//                }
//            }
//        }
//        String msg = "Something wrong when reading FastQC result.\n";
//        try {
//            msg += StringUtils.join(FileUtils.readLines(scratchDir.resolve(getId() + ".stderr.txt").toFile()), "\n");
//        } catch (IOException e) {
//            throw new ToolException(e);
//        }
//
//        throw new ToolException(msg);
    }

//    @Override
//    protected String getId() {
//        return FastqcWrapperAnalysis.ID;
//    }

    @Override
    protected String getDockerImageName() {
        return FastqcWrapperAnalysis.FASTQC_DOCKER_IMAGE;
    }

    private boolean checkParam(String param) {
        if ("o".equals(param) || param.equals(DOCKER_IMAGE_VERSION_PARAM)) {
            return false;
        }
        return true;
    }

    public String getFile() {
        return file;
    }

    public FastqcWrapperAnalysisExecutor setFile(String file) {
        this.file = file;
        return this;
    }
}
