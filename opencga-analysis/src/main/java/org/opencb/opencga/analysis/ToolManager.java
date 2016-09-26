/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.analysis;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.opencga.analysis.execution.plugins.OpenCGAAnalysis;
import org.opencb.opencga.analysis.execution.plugins.PluginFactory;
import org.opencb.opencga.catalog.models.tool.Execution;
import org.opencb.opencga.catalog.models.tool.Manifest;
import org.opencb.opencga.catalog.models.tool.Option;
import org.opencb.opencga.core.common.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class ToolManager {

    protected static Logger logger = LoggerFactory.getLogger(ToolManager.class);
    protected final Properties analysisProperties;
    protected final String home;
    protected String analysisName;
    protected String executionName;
    protected Path analysisRootPath;
    protected Path analysisPath;
    protected Path manifestFile;
    protected Path resultsFile;
    protected String sessionId;
    protected Manifest manifest;
    protected Execution execution;
    protected boolean plugin;

    protected static ObjectMapper jsonObjectMapper = new ObjectMapper();

    private ToolManager() throws IOException, AnalysisExecutionException {
        home = Config.getOpenCGAHome();
        analysisProperties = Config.getAnalysisProperties();
        executionName = null;
    }

    public ToolManager(String analysisStr, String execution) throws IOException, AnalysisExecutionException {
        this(analysisStr, execution, "system");
    }

    @Deprecated
    public ToolManager(String analysisStr, String execution, String analysisOwner) throws IOException, AnalysisExecutionException {
        this();
        if (analysisOwner.equals("system")) {
            this.analysisRootPath = Paths.get(analysisProperties.getProperty("OPENCGA.ANALYSIS.BINARIES.PATH"));
        } else {
            this.analysisRootPath = Paths.get(home, "accounts", analysisOwner);
        }
        this.analysisName = analysisStr;
        if (analysisName.contains(".")) {
            executionName = analysisName.split("\\.")[1];
            analysisName = analysisName.split("\\.")[0];
        } else {
            executionName = execution;
        }

        load();
    }

    public ToolManager(Path analysisRootPath, String analysisName, String executionName) throws IOException, AnalysisExecutionException {
        this();
        this.analysisRootPath = analysisRootPath;
        this.analysisName = analysisName;
        this.executionName = executionName;

        load();
    }

    private void load() throws IOException, AnalysisExecutionException {

        analysisPath = Paths.get(home).resolve(analysisRootPath).resolve(analysisName);

        if (!analysisPath.toFile().exists()) {
            //Search for a plugin
            OpenCGAAnalysis plugin = PluginFactory.get().getPlugin(analysisName);
            if (plugin == null) {
                throw new IllegalArgumentException("Plugin  '" + analysisName + "' does not exist");
            }
            manifest = plugin.getManifest();
            execution = getExecution();
            this.plugin = true;
        } else {
            manifestFile = analysisPath.resolve(Paths.get("manifest.json"));
            resultsFile = analysisPath.resolve(Paths.get("results.js"));
            manifest = getManifest();
            execution = getExecution();
        }
    }

    /*
     * CommandLine creation methods
     */

    private boolean checkRequiredParams(Map<String, List<String>> params, List<Option> validParams) {
        for (Option param : validParams) {
            if (param.isRequired() && !params.containsKey(param.getName())) {
                System.out.println("Missing param: " + param);
                return false;
            }
        }
        return true;
    }

    private Map<String, List<String>> removeUnknownParams(Map<String, List<String>> params, List<Option> validOptions) {
        Set<String> validKeyParams = new HashSet<String>();
        for (Option param : validOptions) {
            validKeyParams.add(param.getName());
        }

        Map<String, List<String>> paramsCopy = new HashMap<String, List<String>>(params);
        for (String param : params.keySet()) {
            if (!validKeyParams.contains(param)) {
                paramsCopy.remove(param);
            }
        }

        return paramsCopy;
    }

    public String createCommandLine(Map<String, List<String>> params)
            throws AnalysisExecutionException {
        return createCommandLine(execution.getExecutable(), params);
    }

    public String createCommandLine(String executable, Map<String, List<String>> params)
            throws AnalysisExecutionException {
        logger.debug("params received in createCommandLine: " + params);
        String binaryPath = analysisPath.resolve(executable).toString();

        // Check required params
        List<Option> validParams = new LinkedList<>(execution.getValidParams());
        validParams.addAll(manifest.getGlobalParams());
        validParams.add(new Option(execution.getOutputParam(), "Outdir", false));
        if (checkRequiredParams(params, validParams)) {
            params = new HashMap<>(removeUnknownParams(params, validParams));
        } else {
            throw new AnalysisExecutionException("ERROR: missing some required params.");
        }

        StringBuilder cmdLine = new StringBuilder();
        cmdLine.append(binaryPath);

        if (params.containsKey("tool")) {
            String tool = params.get("tool").get(0);
            cmdLine.append(" --tool ").append(tool);
            params.remove("tool");
        }

        for (String key : params.keySet()) {
            // Removing renato param
            if (!key.equals("renato")) {
                if (key.length() == 1) {
                    cmdLine.append(" -").append(key);
                } else {
                    cmdLine.append(" --").append(key);
                }
                if (params.get(key) != null) {
                    String paramsArray = params.get(key).toString();
                    String paramValue = paramsArray.substring(1, paramsArray.length() - 1).replaceAll("\\s", "");
                    cmdLine.append(" ").append(paramValue);
                }
            }
        }
        return cmdLine.toString();
    }

    /*
     * Execute util methods
     * Will create the required ExecutorManager and run the job
     */

    public String getAnalysisName() {
        return analysisName;
    }

    public boolean isPlugin() {
        return plugin;
    }

    public Manifest getManifest() throws IOException, AnalysisExecutionException {
        if (manifest == null) {
            manifest = jsonObjectMapper.readValue(manifestFile.toFile(), Manifest.class);
//            analysis = gson.fromJson(IOUtils.toString(manifestFile.toFile()), Analysis.class);
        }
        return manifest;
    }

    public Execution getExecution() {
        if (execution == null) {
            if (executionName == null || executionName.isEmpty()) {
                execution = manifest.getExecutions().get(0);
            } else {
                for (Execution exe : manifest.getExecutions()) {
                    if (exe.getId().equalsIgnoreCase(executionName)) {
                        execution = exe;
                        break;
                    }
                }
                if (execution == null) {
                    throw new IllegalArgumentException("Unknown execution name : " + executionName);
                }
            }
        }
        return execution;
    }

    public String getExamplePath(String fileName) {
        return analysisPath.resolve("examples").resolve(fileName).toString();
    }

    public String help(String baseUrl) {
        if (!Files.exists(manifestFile)) {
            return "Manifest for " + analysisName + " not found.";
        }

        String execName = "";
        if (executionName != null)
            execName = "." + executionName;
        StringBuilder sb = new StringBuilder();
        sb.append("Analysis: " + manifest.getName() + "\n");
        sb.append("Description: " + manifest.getDescription() + "\n");
        sb.append("Version: " + manifest.getVersion() + "\n\n");
        sb.append("Author: " + manifest.getAuthor().getName() + "\n");
        sb.append("Email: " + manifest.getAuthor().getEmail() + "\n");
        if (!manifest.getWebsite().equals(""))
            sb.append("Website: " + manifest.getWebsite() + "\n");
        if (!manifest.getPublication().equals(""))
            sb.append("Publication: " + manifest.getPublication() + "\n");
        sb.append("\nUsage: \n");
        sb.append(baseUrl + "analysis/" + analysisName + execName + "/{action}?{params}\n\n");
        sb.append("\twhere: \n");
        sb.append("\t\t{action} = [run, help, params, test, status]\n");
        sb.append("\t\t{params} = " + baseUrl + "analysis/" + analysisName + execName + "/params\n");
        return sb.toString();
    }

    public String params() {
        if (!Files.exists(manifestFile)) {
            return "Manifest for " + analysisName + " not found.";
        }

        if (execution == null) {
            return "ERROR: Executable not found.";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Valid params for " + manifest.getName() + ":\n\n");
        for (Option param : execution.getValidParams()) {
            String required = "";
            if (param.isRequired())
                required = "*";
            sb.append("\t" + param.getName() + ": " + param.getDescription() + " " + required + "\n");
        }
        sb.append("\n\t*: required parameters.\n");
        return sb.toString();
    }

    public String test(String jobName, int jobId, String jobFolder) throws AnalysisExecutionException, IOException {
        // TODO test

        if (!Files.exists(manifestFile)) {
            return "Manifest for " + analysisName + " not found.";
        }

        if (execution == null) {
            return "ERROR: Executable not found.";
        }

//        executeCommandLine(execution.getTestCmd(), jobName, jobId, jobFolder, analysisName);
//        executeLocal();

        return String.valueOf(jobName);
    }

    public String getResult() throws AnalysisExecutionException {
        return execution.getResult();
    }

    public InputStream getResultInputStream() throws AnalysisExecutionException, IOException {
        System.out.println(resultsFile.toAbsolutePath().toString());
        if (!Files.exists(resultsFile)) {
            resultsFile = analysisPath.resolve(Paths.get("results.js"));
        }

        if (Files.exists(resultsFile)) {
            return Files.newInputStream(resultsFile);
        }
        throw new AnalysisExecutionException("result.js not found.");
    }
}
