package org.opencb.opencga.analysis.wrappers;

import org.apache.commons.lang.StringUtils;
import org.opencb.opencga.analysis.OpenCgaAnalysis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class OpenCgaWrapperAnalysis extends OpenCgaAnalysis {

    public final String DOCKER_IMAGE_VERSION_PARAM = "DOCKER_IMAGE_VERSION";
    public final static String DOCKER_INPUT_PATH = "/data/input";
    public final static String DOCKER_OUTPUT_PATH = "/data/output";

    public final String STDOUT_FILENAME = "stdout.txt";
    public final String STDERR_FILENAME = "stderr.txt";

    public abstract String getDockerImageName();

    protected String getCommandLine() {
        return getCommandLine("--");
    }

    protected String getCommandLine(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append("run docker ").append(getDockerImageName());
        for (String key : params.keySet()) {
            String value = params.getString(key);
            sb.append(" ").append(prefix).append(key);
            if (!StringUtils.isEmpty(value)) {
                sb.append(" ").append(value);
            }
        }
        return sb.toString();
    }

    protected List<String> getFilenames(Path dir) throws IOException {
        Stream<Path> walk = Files.walk(dir);
        return walk.map(x -> x.getFileName().toString()).collect(Collectors.toList());
    }
}