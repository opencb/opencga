package org.opencb.opencga.analysis.wrappers.executors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class DockerWrapperAnalysisExecutor  extends OpenCgaToolExecutor {

    public final static String DOCKER_INPUT_PATH = "/data/input";
    public final static String DOCKER_OUTPUT_PATH = "/data/output";

    public static final String STDOUT_FILENAME = "executor.stdout.txt";
    public static final String STDERR_FILENAME = "executor.stderr.txt";

    public abstract String getDockerImageName();
    public abstract String getDockerImageVersion();

    public String getShortPrefix() {
        return "-";
    }

    public String getLongPrefix() {
        return "--";
    }

    public String getKeyValueSeparator() {
        return " ";
    }

    protected StringBuilder initCommandLine() {
        return new StringBuilder("docker run --log-driver=none -a stdin -a stdout -a stderr ");
    }

    protected Map<String, String> appendMounts(List<Pair<String, String>> inputFilenames, StringBuilder sb) {
        Map<String, String> mountMap = new HashMap<>();

        // Mount input dirs
        for (Pair<String, String> pair : inputFilenames) {
            if (StringUtils.isNotEmpty(pair.getValue())) {
                File file = new File(pair.getValue());
                if (!mountMap.containsKey(file.getParent())) {
                    // Update source target map
                    mountMap.put(file.getParent(), DOCKER_INPUT_PATH + mountMap.size());

                    // Update command line
                    sb.append("--mount type=bind,source=\"").append(file.getParent()).append("\",target=\"")
                            .append(mountMap.get(file.getParent())).append("\" ");
                }
            }
        }

        // Mount output dir
        sb.append("--mount type=bind,source=\"").append(getOutDir().toAbsolutePath()).append("\",target=\"").append(DOCKER_OUTPUT_PATH)
                .append("\" ");

        return mountMap;
    }

    protected void appendCommand(String command, StringBuilder sb) {
        // Docker image and version
        sb.append(getDockerImageName());
        if (StringUtils.isNotEmpty(getDockerImageVersion())) {
            sb.append(":").append(getDockerImageVersion());
        }

        // Append command
        sb.append(" ").append(command);
    }

    protected void appendInputFiles(List<Pair<String, String>> inputFilenames, Map<String, String> srcTargetMap, StringBuilder sb) {
        for (Pair<String, String> pair : inputFilenames) {
            if (StringUtils.isNotEmpty(pair.getValue())) {
                sb.append(" ");
                if (StringUtils.isNotEmpty(pair.getKey())) {
                    if (pair.getKey().length() <= 1) {
                        sb.append(getShortPrefix());
                    } else {
                        sb.append(getLongPrefix());
                    }
                    sb.append(pair.getKey()).append(getKeyValueSeparator());
                }
                File file = new File(pair.getValue());
                sb.append(srcTargetMap.get(file.getParent())).append("/").append(file.getName());
            }
        }
    }

    protected void appendOutputFiles(List<Pair<String, String>> outputFilenames, StringBuilder sb) {
        for (Pair<String, String> pair : outputFilenames) {
            sb.append(" ");
            if (StringUtils.isNotEmpty(pair.getKey())) {
                if (pair.getKey().length() <= 1) {
                    sb.append(getShortPrefix());
                } else {
                    sb.append(getLongPrefix());
                }
                sb.append(pair.getKey()).append(getKeyValueSeparator());
            }
            sb.append(DOCKER_OUTPUT_PATH);
            // Sometimes, no output filename is provided
            if (StringUtils.isNotEmpty(pair.getValue())) {
                sb.append("/").append(pair.getValue());
            }
        }
    }

    protected void appendOtherParams(Set<String> skipParams, StringBuilder sb) {
        for (String paramName : getExecutorParams().keySet()) {
            if (skipParameter(paramName)) {
                continue;
            }

            if (skipParams.contains(paramName)) {
                continue;
            }
            sb.append(" ");
            if (StringUtils.isNotEmpty(paramName)) {
                if (paramName.length() <= 1) {
                    sb.append(getShortPrefix());
                } else {
                    sb.append(getLongPrefix());
                }
                sb.append(paramName).append(getKeyValueSeparator());
            }
            String value = getExecutorParams().getString(paramName);
            if (StringUtils.isNotEmpty(value) && !"true".equals(value)) {
                sb.append(getExecutorParams().getString(paramName));
            }
        }
    }

    protected void runCommandLine(String cmdline) throws ToolException {
        try {
            new Command(cmdline)
                    .setOutputOutputStream(
                            new DataOutputStream(new FileOutputStream(getOutDir().resolve(STDOUT_FILENAME).toFile())))
                    .setErrorOutputStream(
                            new DataOutputStream(new FileOutputStream(getOutDir().resolve(STDERR_FILENAME).toFile())))
                    .run();
        } catch (FileNotFoundException e) {
            throw new ToolException(e);
        }
    }

    protected boolean skipParameter(String param) {
        switch (param) {
            case "opencgaHome":
            case "token":
            case "storageEngineId":
            case "dbName":
            case "executorId":
                return true;
        }
        return false;
    }
}