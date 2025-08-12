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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.wrappers.multiqc.MultiQcWrapperAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.tools.ResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.analysis.wrappers.multiqc.MultiQcWrapperAnalysisExecutor.OUTDIR_PARAM;

public class WrapperUtils {

    public static final String FILE_PREFIX = "file://";
    public static final String INPUT_FILE_PREFIX = "input://";
    public static final String OUTPUT_FILE_PREFIX = "output://";

    public static final String EXECUTE_TOOL_SCRIPT = "execute_tool.py";

    public static final String COMMAND = "command";
    public static final String INPUT = "input";
    public static final String PARAMS = "params";

    private static Logger logger = LoggerFactory.getLogger(WrapperUtils.class);

    private WrapperUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static String checkPath(String fileId, String study, CatalogManager catalogManager, String token) throws ToolException {
        try {
            File file = catalogManager.getFileManager().get(study, fileId.substring(FILE_PREFIX.length()), QueryOptions.empty(), token)
                    .first();
            Path path = Paths.get(file.getUri().getPath());
            if (Files.exists(path)) {
                return INPUT_FILE_PREFIX + path.toAbsolutePath();
            } else {
                throw new ToolException("File '" + fileId + "' does not exist in the local filesystem: " + path.toAbsolutePath());
            }
        } catch (CatalogException e) {
            throw new ToolException("Error checking file '" + fileId + "'", e);
        }
    }

    public static List<String> checkPaths(List<String> paths, String study, CatalogManager catalogManager, String token)
            throws ToolException {
        List<String> updatedPaths = new ArrayList<>(paths.size());
        for (String path : paths) {
            updatedPaths.add(WrapperUtils.checkPath(path, study, catalogManager, token));
        }
        return updatedPaths;
    }

    public static List<Object> checkPathsEx(List<Object> input, String study, CatalogManager catalogManager, String token) {
        return null;
    }

    public static ObjectMap checkParams(ObjectMap params, String study, CatalogManager catalogManager, String token)
            throws ToolException {
        ObjectMap updatedParams = new ObjectMap();
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            if (entry.getValue() instanceof List) {
                updatedParams.put(entry.getKey(), WrapperUtils.checkPaths((List<String>) entry.getValue(), study, catalogManager, token));
            } else if (entry.getValue() instanceof String && ((String) entry.getValue()).startsWith(FILE_PREFIX)) {
                // If the entry value starts with "file://", it is a file path, so we need to check it
                updatedParams.put(entry.getKey(), WrapperUtils.checkPath((String) entry.getValue(), study, catalogManager, token));
            } else {
                // Otherwise, we assume it's a regular parameter
                updatedParams.put(entry.getKey(), entry.getValue());
            }
        }
        return updatedParams;
    }

    public static String buildVirtualPath(String inputPath, String prefix, List<AbstractMap.SimpleEntry<String, String>> bindings,
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

    public static List<String> buildVirtualPaths(List<String> inputPaths, String prefix,
                                                 List<AbstractMap.SimpleEntry<String, String>> bindings,
                                                 Set<String> readOnlyBindings) throws ToolException {
        int counter = 0;
        List<String> virtualPaths = new ArrayList<>(inputPaths.size());
        for (String inputPath : inputPaths) {
            String virtualPath = WrapperUtils.buildVirtualPath(inputPath, prefix + "_" + counter, bindings, readOnlyBindings);
            counter++;
            virtualPaths.add(virtualPath);
        }
        return virtualPaths;
    }

    public static ObjectMap updateParams(ObjectMap params, String prefix, List<AbstractMap.SimpleEntry<String, String>> bindings,
                                         Set<String> readOnlyBindings) throws ToolException {
        int counter = 0;
        ObjectMap updatedParams = new ObjectMap();
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            if (entry.getValue() instanceof List) {
                List<String> values = (List<String>) entry.getValue();
                if (values.get(0).startsWith(INPUT_FILE_PREFIX) || values.get(0).startsWith(OUTPUT_FILE_PREFIX)) {
                    updatedParams.put(entry.getKey(), WrapperUtils.buildVirtualPaths((List<String>) entry.getValue(),
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
                    String virtualPath = WrapperUtils.buildVirtualPath(value, prefix + "_" + counter, bindings, readOnlyBindings);
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

    public static String buildVirtualAnalysisPath(String opencgaHome, List<AbstractMap.SimpleEntry<String, String>> bindings,
                                                  Set<String> readOnlyBindings) {
        Path analysisPath = Paths.get(opencgaHome).resolve(ResourceManager.ANALYSIS_DIRNAME);
        String virtualAnalysisPath = "/analysis";
        bindings.add(new AbstractMap.SimpleEntry<>(analysisPath.toAbsolutePath().toString(), virtualAnalysisPath));
        readOnlyBindings.add(virtualAnalysisPath);
        return virtualAnalysisPath;
    }

    public static String processParamsFile(ObjectMap params, Path outDir, List<AbstractMap.SimpleEntry<String, String>> bindings,
                                           Set<String> readOnlyBindings) {
        return processParamsFile(params, !params.containsKey("params"), outDir, bindings, readOnlyBindings);
    }

    public static String processParamsFile(ObjectMap params, boolean addInParams, Path outDir,
                                           List<AbstractMap.SimpleEntry<String, String>> bindings, Set<String> readOnlyBindings) {
        String paramsFilename = "params.json";
        String virtualParamsPath = "/outdir/" + paramsFilename;
        ObjectMap newParams = new ObjectMap();
        if (addInParams) {
            newParams.put("params", params);
        } else {
            newParams.putAll(params);
        }
        bindings.add(new AbstractMap.SimpleEntry<>(outDir.resolve(paramsFilename).toAbsolutePath().toString(), virtualParamsPath));
        readOnlyBindings.add(virtualParamsPath);

        // Write the parameters to a file
        WrapperUtils.writeParamsFile(newParams, outDir.resolve(paramsFilename));

        return virtualParamsPath;
    }

    public static void writeParamsFile(ObjectMap params, Path path) {
        try (OutputStream outputStream = Files.newOutputStream(path)) {
            JacksonUtils.getDefaultObjectMapper().writeValue(outputStream, params);
        } catch (IOException e) {
            logger.error("Error writing params file to '{}'", path, e);
        }
    }

    public static String buildWrapperCli(String interpreter, String virtualAnalysisPath, String analysisId, String wrapperScript,
                                         String virtualParamsPath) {
        StringBuilder cli = new StringBuilder();
        if (StringUtils.isNotEmpty(interpreter)) {
            cli.append(interpreter).append(" ");
        }
        cli.append(virtualAnalysisPath).append("/").append(analysisId).append("/").append(wrapperScript)
                .append(" -p ").append(virtualParamsPath);

        return cli.toString();
    }
    public static String buildWrapperCli(String interpreter, String virtualAnalysisPath, String tool, String virtualParamsPath) {
        StringBuilder cli = new StringBuilder();
        if (StringUtils.isNotEmpty(interpreter)) {
            cli.append(interpreter).append(" ");
        }
        cli.append(virtualAnalysisPath).append("/").append(EXECUTE_TOOL_SCRIPT)
                .append(" -t ").append(tool)
                .append(" -p ").append(virtualParamsPath);

        return cli.toString();
    }
}
