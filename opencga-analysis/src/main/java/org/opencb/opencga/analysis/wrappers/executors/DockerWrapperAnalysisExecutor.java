package org.opencb.opencga.analysis.wrappers.executors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.*;

public abstract class DockerWrapperAnalysisExecutor  extends OpenCgaToolExecutor {

    public final static String DOCKER_INPUT_PATH = "/data/input";
    public final static String DOCKER_OUTPUT_PATH = "/data/output";

    public static final String STDOUT_FILENAME = "stdout.txt";
    public static final String STDERR_FILENAME = "stderr.txt";

    public String getDockerImageName() {
        return "opencb/opencga-ext-tools";
    }

    public String getDockerImageVersion() {
        return GitRepositoryState.getInstance().getBuildVersion();
    }

    private Logger privateLogger = LoggerFactory.getLogger(DockerWrapperAnalysisExecutor.class);

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

            if (CollectionUtils.isNotEmpty(skipParams) && skipParams.contains(paramName)) {
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
        checkDockerDaemonAlive();
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

    protected final void checkDockerDaemonAlive() throws ToolException {
        int maxAttempts = 6;
        for (int i = 0; i < maxAttempts; i++) {
            Command command = new Command("docker stats --no-stream");
            command.run();
            if (command.getExitValue() == 0) {
                // Docker is alive
                if (i != 0) {
                    privateLogger.info("Docker daemon up and running!");
                }
                return;
            }
            privateLogger.info("Waiting for docker to start... (sleep 5s) [" + i + "/" + maxAttempts + "]");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new ToolException(e);
            }
        }
        throw new ToolException("Docker daemon is not available on this node!");
    }

    public static List<Pair<String, String>> getInputFilenames(String inputFile, Set<String> fileParamNames, ObjectMap executorParams) {
        List<Pair<String, String>> inputFilenames = new ArrayList<>();
        if (StringUtils.isNotEmpty(inputFile)) {
            inputFilenames.add(new ImmutablePair<>("", inputFile));
        }

        if (MapUtils.isNotEmpty(executorParams)) {
            for (String paramName : executorParams.keySet()) {
                if (skipParameter(paramName)) {
                    continue;
                }

                if (fileParamNames.contains(paramName)) {
                    Pair<String, String> pair = new ImmutablePair<>(paramName, executorParams.get(paramName).toString());
                    inputFilenames.add(pair);
                }
            }
        }

        return inputFilenames;
    }

    protected static boolean skipParameter(String param) {
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