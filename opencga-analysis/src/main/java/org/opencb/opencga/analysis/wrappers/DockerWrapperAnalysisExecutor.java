package org.opencb.opencga.analysis.wrappers;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.opencb.opencga.analysis.wrappers.samtools.SamtoolsWrapperAnalysis.INDEX_STATS_PARAM;

public abstract class DockerWrapperAnalysisExecutor   extends OpenCgaToolExecutor {

    public final static String DOCKER_INPUT_PATH = "/data/input";
    public final static String DOCKER_OUTPUT_PATH = "/data/output";

    public static final String STDOUT_FILENAME = "stdout.txt";
    public static final String STDERR_FILENAME = "stderr.txt";

    protected boolean isValidParameter(String param) {
        switch (param) {
            case "opencgaHome":
            case "token":
            case "storageEngineId":
            case "dbName":
            case "executorId":
                return false;
        }
        return true;
    }

    protected Map<String, String> getDockerMountMap(List<String> inputFilenames) {
        Map<String, String> dockerMountMap = new HashMap<>();

        // Mount input dirs
        if (CollectionUtils.isNotEmpty(inputFilenames)) {
            // Pair: key = name of the parameter; value = full path to the file
            for (String inputFilename : inputFilenames) {
                if (StringUtils.isNotEmpty(inputFilename)) {
                    File file = new File(inputFilename);
                    String src = file.getParentFile().getAbsolutePath();
                    if (!dockerMountMap.containsKey(src)) {
                        dockerMountMap.put(src, DOCKER_INPUT_PATH + dockerMountMap.size());
                    }
                }
            }
        }

        return dockerMountMap;
    }

    protected StringBuilder initDockerCommandLine(Map<String, String> dockerMountingMap, String dockerImageName,
                                                  String dockerImageVersion) {
        StringBuilder sb = new StringBuilder("docker run --log-driver=none -a stdin -a stdout -a stderr ");

        // Mount input dir
        for (Map.Entry<String, String> entry : dockerMountingMap.entrySet()) {
            sb.append("--mount type=bind,source=\"").append(entry.getKey()).append("\",target=\"").append(entry.getValue()).append("\" ");
        }

        // Mount output dir
        sb.append("--mount type=bind,source=\"").append(getOutDir().toAbsolutePath()).append("\",target=\"").append(DOCKER_OUTPUT_PATH)
                .append("\" ");


        sb.append(dockerImageName);
        if (StringUtils.isNotEmpty(dockerImageVersion)) {
            sb.append(":").append(dockerImageVersion);
        }

        return sb;
    }
}