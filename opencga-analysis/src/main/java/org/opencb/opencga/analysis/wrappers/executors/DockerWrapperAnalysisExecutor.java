package org.opencb.opencga.analysis.wrappers.executors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.exec.Command;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.ConfigurationUtils;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.config.AnalysisTool;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.tools.ResourceManager.RESOURCES_DIRNAME;

public abstract class DockerWrapperAnalysisExecutor extends OpenCgaToolExecutor {

    protected Logger logger;

    public static final String DOCKER_INPUT_PATH = "/data/input";
    public static final String DOCKER_OUTPUT_PATH = "/data/output";

    public static final String SCRIPT_VIRTUAL_PATH = "/script";
    public static final String INPUT_VIRTUAL_PATH = "/input";
    public static final String OUTPUT_VIRTUAL_PATH = "/output";
    protected static final String RESOURCES_VIRTUAL_PATH = "/" + RESOURCES_DIRNAME;

    protected static final String RESOURCES_ATTR_KEY = "resources";

    public static final String STDOUT_FILENAME = "stdout.txt";
    public static final String STDERR_FILENAME = "stderr.txt";

    public static final String DOCKER_CLI_MSG = "Docker CLI: ";

    public DockerWrapperAnalysisExecutor() {
        logger = LoggerFactory.getLogger(this.getClass());
    }

    public String getDockerImageName() throws ToolException {
        return getConfiguration().getAnalysis().getOpencgaExtTools().split(":")[0];
    }

    public String getDockerImageVersion() {
        if (getConfiguration().getAnalysis().getOpencgaExtTools().contains(":")) {
            return getConfiguration().getAnalysis().getOpencgaExtTools().split(":")[1];
        } else {
            return GitRepositoryState.getInstance().getBuildVersion();
        }
    }

    public String getDockerImageName(String toolId, String version) throws ToolException {
        AnalysisTool tool = ConfigurationUtils.getAnalysisTool(toolId, version, getConfiguration());
        return tool.getDockerId().split(":")[0];
    }

    public String getDockerImageVersion(String toolId, String version) throws ToolException {
        AnalysisTool tool = ConfigurationUtils.getAnalysisTool(toolId, version, getConfiguration());
        if (tool.getDockerId().contains(":")) {
            return tool.getDockerId().split(":")[1];
        } else {
            return null;
        }
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

    protected StringBuilder initCommandLine(String user) {
        StringBuilder sb = initCommandLine();
        if (StringUtils.isNotEmpty(user)) {
            sb.append("--user ").append(user);
        }
        return sb;
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

    protected void appendCommand(String command, StringBuilder sb) throws ToolException {
        appendDockerAndCommand(command, getDockerImageName(), getDockerImageVersion(), sb);
    }

    protected void appendCommand(String command, String toolId, String version, StringBuilder sb) throws ToolException {
        appendDockerAndCommand(command, getDockerImageName(toolId, version), getDockerImageVersion(toolId, version), sb);
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

    protected String buildCommandLine(String image, List<AbstractMap.SimpleEntry<String, String>> inputBindings,
                                      AbstractMap.SimpleEntry<String, String> outputBinding, String cmdParams,
                                      Map<String, String> dockerParams) throws IOException {
        return buildCommandLine(image, inputBindings, null, outputBinding, cmdParams, dockerParams);
    }

    protected String buildCommandLine(String image, List<AbstractMap.SimpleEntry<String, String>> inputBindings,
                                      Set<String> readOnlyInputBindings, AbstractMap.SimpleEntry<String, String> outputBinding,
                                      String cmdParams, Map<String, String> dockerParams) throws IOException {
        // Sanity check
        if (outputBinding == null) {
            throw new IllegalArgumentException("Missing output binding");
        }

        // Docker run
        StringBuilder commandLine = new StringBuilder("docker run --rm ");

        // Docker params
        boolean setUser = true;
        if (dockerParams != null) {
            if (dockerParams.containsKey("user")) {
                setUser = false;
            }
            for (String key : dockerParams.keySet()) {
                commandLine.append(key).append(" ").append(dockerParams.get(key)).append(" ");
            }
        }

        if (setUser) {
            // User: array of two strings, the first string, the user; the second, the group
            String[] user = FileUtils.getUserAndGroup(Paths.get(outputBinding.getKey()), true);
            commandLine.append("--user ").append(user[0]).append(":").append(user[1]).append(" ");
        }

        if (inputBindings != null) {
            // Mount management (bindings)
            for (AbstractMap.SimpleEntry<String, String> binding : inputBindings) {
                commandLine.append("--mount type=bind,source=\"").append(binding.getKey()).append("\",target=\"")
                        .append(binding.getValue()).append("\"");
                if (CollectionUtils.isNotEmpty(readOnlyInputBindings) && readOnlyInputBindings.contains(binding.getValue())) {
                    commandLine.append(",readonly");
                }
                commandLine.append(" ");
            }
        }
        commandLine.append("--mount type=bind,source=\"").append(outputBinding.getKey()).append("\",target=\"")
                .append(outputBinding.getValue()).append("\" ");

        // Docker image and version
        commandLine.append(image).append(" ");

        // Image command params
        commandLine.append(cmdParams);
        return commandLine.toString();
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
        int maxAttempts = 20;
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

    protected void addParameters(ObjectMap params) throws ToolException {
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            addParam(entry.getKey(), entry.getValue());
        }
    }

    protected void addResources(Path resourcePath) throws ToolException {
        List<String> resourceNames = Arrays.stream(resourcePath.toFile().listFiles()).map(File::getName).collect(Collectors.toList());
        addAttribute(RESOURCES_ATTR_KEY, resourceNames);
    }

    private void appendDockerAndCommand(String command, String dockerImage, String dockerImageVersion, StringBuilder sb) {
        // Docker image and version
        sb.append(dockerImage);
        if (StringUtils.isNotEmpty(dockerImageVersion)) {
            sb.append(":").append(dockerImageVersion);
        }

        // Append command
        sb.append(" ").append(command);
    }

    protected Map<String, String> getDefaultDockerParams() throws IOException {
        Map<String, String> dockerParams = new HashMap<>();
        dockerParams.put("--volume", "/var/run/docker.sock:/var/run/docker.sock");
        dockerParams.put("--network", "host");

        // Get default docker params, and get docker group id to avoid permission issues when writing to output directory
        String dockerGid = getDockerGid();
        if (!StringUtils.isEmpty(dockerGid)) {
            dockerParams.put("--group-add", dockerGid);
        } else {
            logger.warn("Could not get docker group id to avoid permission issues when writing to output directory");
        }

        return dockerParams;
    }

    protected String getDockerGid() throws IOException {
        Path dockerSocket = Paths.get("/var/run/docker.sock");
        if (Files.exists(dockerSocket)) {
            PosixFileAttributes attrs = Files.readAttributes(dockerSocket, PosixFileAttributes.class);
            return String.valueOf(attrs.group().hashCode());
        } else {
            // Extract GID from group name
            UserPrincipalLookupService lookupService =
                    FileSystems.getDefault().getUserPrincipalLookupService();
            GroupPrincipal dockerGroup = lookupService.lookupPrincipalByGroupName("docker");

            return dockerGroup.getName().split(":")[1];
        }
    }
}