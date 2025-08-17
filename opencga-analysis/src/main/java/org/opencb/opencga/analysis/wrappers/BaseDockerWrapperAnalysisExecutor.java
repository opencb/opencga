package org.opencb.opencga.analysis.wrappers;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.Analysis;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.wrapper.WrapperParams;
import org.opencb.opencga.core.tools.ResourceManager;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public abstract class BaseDockerWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public static final String INPUT_FILE_PREFIX = "input://";
    public static final String OUTPUT_FILE_PREFIX = "output://";

    protected static final String ANALYSIS_VIRTUAL_PATH = "/analysis";

    protected String study;

    protected abstract String getTool();
    protected abstract WrapperParams getWrapperParams();
    protected abstract void validateOutputDirectory() throws ToolExecutorException;

    private static final String EXECUTE_TOOL_SCRIPT = "execute_tool.py";

    @Override
    protected void run() throws Exception {
        // Validate output directory
        validateOutputDirectory();

        // Wrapper parameters to be executed in the docker container
        WrapperParams params = getWrapperParams();

        // Input and output bindings
        List<AbstractMap.SimpleEntry<String, String>> bindings = new ArrayList<>();
        Set<String> readOnlyBindings = new HashSet<>();

        // Build virtual paths and update params
        buildVirtualParams(params, bindings, readOnlyBindings);

        // Execute docker command
        executeDockerCommand(bindings, readOnlyBindings, params);
    }

    protected void buildVirtualParams(WrapperParams params,
                                      List<AbstractMap.SimpleEntry<String, String>> bindings,
                                      Set<String> readOnlyBindings) throws ToolException {
        // Handle input if present
        if (params.getInput() != null) {
            params.setInput(buildVirtualPaths(params.getInput(), "input", bindings, readOnlyBindings));
        }

        // Handle options
        params.setOptions(updateParams(params.getOptions(), "data", bindings, readOnlyBindings));
    }

    private void executeDockerCommand(List<AbstractMap.SimpleEntry<String, String>> bindings,
                                      Set<String> readOnlyBindings,
                                      WrapperParams params) throws Exception {
        // Create params file
        String virtualParamsPath = createParamsFile(params, getOutDir(), bindings, readOnlyBindings);

        // Build command line
        String virtualAnalysisPath = buildVirtualAnalysisPath(getExecutorParams().getString("opencgaHome"), bindings, readOnlyBindings);
        String wrapperCli = buildWrapperCommandLine("python3", virtualAnalysisPath, getTool(), virtualParamsPath);

        // Build docker command
        String dockerImage = getDockerFullImageName(Analysis.TRASNSCRIPTOMICS_DOCKER_KEY);
        String[] user = FileUtils.getUserAndGroup(getOutDir(), true);
        Map<String, String> dockerParams = new HashMap<>();
        dockerParams.put("user", user[0] + ":" + user[1]);

        String dockerCli = buildCommandLine(dockerImage, bindings, readOnlyBindings, wrapperCli, dockerParams);
        addEvent(Event.Type.INFO, "Docker command line: " + dockerCli);
        logger.info("Docker command line: {}", dockerCli);

        int exitValue = runCommandLine(dockerCli);
        if (exitValue != 0) {
            throw new ToolExecutorException("Error executing " + getTool() + ": exit value " + exitValue + ". Check the logs for more details.");
        }
    }

    protected String buildVirtualPath(String inputPath, String prefix, List<AbstractMap.SimpleEntry<String, String>> bindings,
                                      Set<String> readOnlyBindings) throws ToolException {
        // Sanity check
        if (!inputPath.startsWith(INPUT_FILE_PREFIX) && !inputPath.startsWith(OUTPUT_FILE_PREFIX)) {
            throw new ToolException("Input path '" + inputPath + "' must start with '" + INPUT_FILE_PREFIX + "' or '"
                    + OUTPUT_FILE_PREFIX + "'.");
        }

        // Process paths
        String filePrefix = INPUT_FILE_PREFIX;
        if (inputPath.startsWith(OUTPUT_FILE_PREFIX)) {
            filePrefix = OUTPUT_FILE_PREFIX;
        }
        Path path = Paths.get(inputPath.substring(filePrefix.length()));

        // Check if the path exists already in the bindings
        for (AbstractMap.SimpleEntry<String, String> binding : bindings) {
            if (binding.getKey().equals(path.toAbsolutePath().toString())) {
                return binding.getValue();
            }
        }

        // Otherwise, we need to create a new binding
        String virtualPath = "/" + prefix + "/";
        if (path.toFile().isFile()) {
            virtualPath += path.getFileName().toString();
        }
        bindings.add(new AbstractMap.SimpleEntry<>(path.toAbsolutePath().toString(), virtualPath));
        if (filePrefix.equals(INPUT_FILE_PREFIX)) {
            readOnlyBindings.add(virtualPath);
        }
        return virtualPath;
    }

    protected List<String> buildVirtualPaths(List<String> inputPaths, String prefix,
                                             List<AbstractMap.SimpleEntry<String, String>> bindings,
                                             Set<String> readOnlyBindings) throws ToolException {
        int counter = 0;
        List<String> virtualPaths = new ArrayList<>(inputPaths.size());
        for (String inputPath : inputPaths) {
            String virtualPath = buildVirtualPath(inputPath, prefix + "_" + counter, bindings, readOnlyBindings);
            counter++;
            virtualPaths.add(virtualPath);
        }
        return virtualPaths;
    }

    protected ObjectMap updateParams(ObjectMap params, String prefix, List<AbstractMap.SimpleEntry<String, String>> bindings,
                                     Set<String> readOnlyBindings) throws ToolException {
        int counter = 0;
        ObjectMap updatedParams = new ObjectMap();
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            if (entry.getValue() instanceof List) {
                List<String> values = (List<String>) entry.getValue();
                if (values.get(0).startsWith(INPUT_FILE_PREFIX) || values.get(0).startsWith(OUTPUT_FILE_PREFIX)) {
                    updatedParams.put(entry.getKey(), buildVirtualPaths((List<String>) entry.getValue(),
                            prefix + "_" + counter, bindings, readOnlyBindings));
                    counter++;
                } else {
                    // Otherwise, we assume it's a regular parameter
                    updatedParams.put(entry.getKey(), entry.getValue());
                }
            } else if (entry.getValue() instanceof String) {
                String value = (String) entry.getValue();
                if (value.startsWith(INPUT_FILE_PREFIX) || value.startsWith(OUTPUT_FILE_PREFIX)) {
                    // If the entry value starts with INPUT_FILE_PREFIX or OUTPUT_FILE_PREFIX, it has to be converted to a virtual path
                    String virtualPath = buildVirtualPath(value, prefix + "_" + counter, bindings, readOnlyBindings);
                    counter++;
                    updatedParams.put(entry.getKey(), virtualPath);
                } else {
                    // Otherwise, we assume it's a regular parameter
                    updatedParams.put(entry.getKey(), entry.getValue());
                }
            } else {
                // Otherwise, we assume it's a regular parameter
                updatedParams.put(entry.getKey(), entry.getValue());
            }
        }
        return updatedParams;
    }

    private String buildVirtualAnalysisPath(String opencgaHome, List<AbstractMap.SimpleEntry<String, String>> bindings,
                                            Set<String> readOnlyBindings) {
        Path analysisPath = Paths.get(opencgaHome).resolve(ResourceManager.ANALYSIS_DIRNAME);
        String virtualAnalysisPath = ANALYSIS_VIRTUAL_PATH;
        bindings.add(new AbstractMap.SimpleEntry<>(analysisPath.toAbsolutePath().toString(), virtualAnalysisPath));
        readOnlyBindings.add(virtualAnalysisPath);
        return virtualAnalysisPath;
    }

//    @Deprecated
//    private String processParamsFile(ObjectMap params, Path outDir, List<AbstractMap.SimpleEntry<String, String>> bindings,
//                                     Set<String> readOnlyBindings) {
//        return processParamsFile(params, !params.containsKey("params"), outDir, bindings, readOnlyBindings);
//    }
//
//    @Deprecated
//    private String processParamsFile(ObjectMap params, boolean addInParams, Path outDir,
//                                     List<AbstractMap.SimpleEntry<String, String>> bindings, Set<String> readOnlyBindings) {
//        String paramsFilename = "params.json";
//        String virtualParamsPath = "/outdir/" + paramsFilename;
//        ObjectMap newParams = new ObjectMap();
//        if (addInParams) {
//            newParams.put("params", params);
//        } else {
//            newParams.putAll(params);
//        }
//        bindings.add(new AbstractMap.SimpleEntry<>(outDir.resolve(paramsFilename).toAbsolutePath().toString(), virtualParamsPath));
//        readOnlyBindings.add(virtualParamsPath);
//
//        // Write the parameters to a file
//        writeParamsFile(newParams, outDir.resolve(paramsFilename));
//
//        return virtualParamsPath;
//    }
//
//    @Deprecated
//    private void writeParamsFile(ObjectMap params, Path path) {
//        try (OutputStream outputStream = Files.newOutputStream(path)) {
//
//            // Get the default ObjectMapper instance and configure the ObjectMapper to ignore all fields with null values
//            ObjectMapper objectMapper = JacksonUtils.getDefaultObjectMapper()
//                    .setSerializationInclusion(JsonInclude.Include.NON_NULL);
//
//            // Write the params to the output stream
//            objectMapper.writeValue(outputStream, params);
//        } catch (IOException e) {
//            logger.error("Error writing params file to '{}'", path, e);
//        }
//    }

    private String createParamsFile(WrapperParams params, Path outDir, List<AbstractMap.SimpleEntry<String, String>> bindings,
                                    Set<String> readOnlyBindings) throws IOException {
        String paramsFilename = "params.json";
        String virtualParamsPath = "/outdir/" + paramsFilename;
        bindings.add(new AbstractMap.SimpleEntry<>(outDir.resolve(paramsFilename).toAbsolutePath().toString(), virtualParamsPath));
        readOnlyBindings.add(virtualParamsPath);

        // Write the parameters to a file
        try (OutputStream outputStream = Files.newOutputStream(outDir.resolve(paramsFilename))) {
            // Get the default ObjectMapper instance and configure the ObjectMapper to ignore all fields with null values
            ObjectMapper objectMapper = JacksonUtils.getDefaultObjectMapper()
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL);

            // Write the params to the output stream
            objectMapper.writeValue(outputStream, params);
        }

        return virtualParamsPath;
    }

//    private String buildWrapperCommandLine(String interpreter, String virtualAnalysisPath, String analysisId, String wrapperScript,
//                                           String virtualParamsPath) {
//        StringBuilder cli = new StringBuilder();
//        if (StringUtils.isNotEmpty(interpreter)) {
//            cli.append(interpreter).append(" ");
//        }
//        cli.append(virtualAnalysisPath).append("/").append(analysisId).append("/").append(wrapperScript)
//                .append(" -p ").append(virtualParamsPath);
//
//        return cli.toString();
//    }

    private String buildWrapperCommandLine(String interpreter, String virtualAnalysisPath, String tool, String virtualParamsPath) {
        StringBuilder cli = new StringBuilder();
        if (StringUtils.isNotEmpty(interpreter)) {
            cli.append(interpreter).append(" ");
        }
        cli.append(virtualAnalysisPath).append("/").append(EXECUTE_TOOL_SCRIPT)
                .append(" -t \"").append(tool).append("\"")
                .append(" -p ").append(virtualParamsPath);

        return cli.toString();
    }

    public String getStudy() {
        return study;
    }

    public BaseDockerWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }
}