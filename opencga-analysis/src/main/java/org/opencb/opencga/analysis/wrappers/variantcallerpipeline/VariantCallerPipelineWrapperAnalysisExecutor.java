package org.opencb.opencga.analysis.wrappers.variantcallerpipeline;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.apache.commons.io.FileUtils.readLines;

@ToolExecutor(id = VariantCallerPipelineWrapperAnalysisExecutor.ID,
        tool = VariantCallerPipelineWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class VariantCallerPipelineWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public static final String ID = VariantCallerPipelineWrapperAnalysis.ID + "-local";

    public static final String PREPARE_CMD = "prepare";
    public static final String PIPELINE_CMD = "pipeline";

    private static final String VARIANT_CALLER_PIPELINE_SCRIPT = "main.py";


    private String study;
    private Path scriptPath;
    private String command;

    private List<String> input;
    private Path resourcePath;


    @Override
    protected void run() throws Exception {
        // Add parameters
        addParameters(getParameters());

        // Command was checked in the tool, but we double check here
        switch (command) {
            case PREPARE_CMD:
                runPrepare();
                break;
            case PIPELINE_CMD: {
                runVariantCallerPipeline();
                break;
            }
            default: {
                throw new ToolExecutorException("Unknown variant caller pipeline command '" + command + "'. Valid commands are: '"
                        + PREPARE_CMD + "' and '" + PIPELINE_CMD + "'");
            }
        }

//        // Add resources as executor attributes
//        addResources(resourcePath);
    }

    private void runPrepare() throws ToolExecutorException {
        try {

            // Input bindings
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
            Path virtualScriptPath = Paths.get(SCRIPT_VIRTUAL_PATH).resolve(VARIANT_CALLER_PIPELINE_SCRIPT);
            inputBindings.add(new AbstractMap.SimpleEntry<>(scriptPath.resolve(VARIANT_CALLER_PIPELINE_SCRIPT).toAbsolutePath().toString(),
                    virtualScriptPath.toString()));

            String reference = input.get(0);
            Path virtualRefPath = null;
            if (!reference.startsWith("http://")
                    && !reference.startsWith("https://")
                    && !reference.startsWith("ftp://")) {
                // We need to bind to a virtual path
                virtualRefPath = Paths.get(INPUT_VIRTUAL_PATH).resolve(Paths.get(reference).getFileName());
                inputBindings.add(new AbstractMap.SimpleEntry<>(reference, virtualRefPath.toString()));
            }

            // Read only input bindings
            Set<String> readOnlyInputBindings = new HashSet<>();
            readOnlyInputBindings.add(virtualScriptPath.toString());
            if (virtualRefPath != null) {
                readOnlyInputBindings.add(virtualRefPath.toString());
            }

            // Output binding
            AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(resourcePath.toAbsolutePath().toString(),
                    OUTPUT_VIRTUAL_PATH);

            // Main command line and params, e.g.:
            // ./analysis/variant-caller-pipeline/main.py prepare
            // -r https://ftp.ensembl.org/pub/release-115/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.chromosome.22.fa.gz
            // -o /tmp/bbb
            String params = "python3 " + virtualScriptPath
                    + " -r " + (virtualRefPath != null ? virtualRefPath : reference)
                    + " -o " + OUTPUT_VIRTUAL_PATH;

            // Execute Python script in docker
            String dockerImage = getDockerImageName() + ":" + getDockerImageVersion();

            String dockerCli = buildCommandLine(dockerImage, inputBindings, readOnlyInputBindings, outputBinding, params, null);
            addEvent(Event.Type.INFO, "Docker command line: " + dockerCli);
            logger.info("Docker command line: {}", dockerCli);

            // Execute docker command line
            runCommandLine(dockerCli);
        } catch (IOException | ToolException e) {
            throw new ToolExecutorException(e);
        }

    }

    private void runVariantCallerPipeline() throws ToolExecutorException {
        try {
            File file = null;
            Path outPath = getOutDir();

            // Input bindings
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
            Path virtualScriptPath = Paths.get(SCRIPT_VIRTUAL_PATH).resolve(VARIANT_CALLER_PIPELINE_SCRIPT);
            inputBindings.add(new AbstractMap.SimpleEntry<>(scriptPath.resolve(VARIANT_CALLER_PIPELINE_SCRIPT).toAbsolutePath().toString(),
                    virtualScriptPath.toString()));
            Path virtualVcfPath = Paths.get(INPUT_VIRTUAL_PATH).resolve(file.getName());
            inputBindings.add(new AbstractMap.SimpleEntry<>(file.getAbsolutePath(), virtualVcfPath.toString()));
            inputBindings.add(new AbstractMap.SimpleEntry<>(resourcePath.toAbsolutePath().toString(), RESOURCES_VIRTUAL_PATH));

            // Read only input bindings
            Set<String> readOnlyInputBindings = new HashSet<>();
            readOnlyInputBindings.add(virtualScriptPath.toString());
            readOnlyInputBindings.add(virtualVcfPath.toString());

            // Output binding
            AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(outPath.toAbsolutePath().toString(),
                    OUTPUT_VIRTUAL_PATH);

            // Main command line and params
            String params = "bash " + virtualScriptPath
                    + " " + virtualVcfPath
                    + " " + OUTPUT_VIRTUAL_PATH
                    + " " + RESOURCES_VIRTUAL_PATH;

            // Execute Python script in docker
            String dockerImage = getDockerImageName() + ":" + getDockerImageVersion();

            String dockerCli = buildCommandLine(dockerImage, inputBindings, readOnlyInputBindings, outputBinding, params, null);
            addEvent(Event.Type.INFO, "Docker command line: " + dockerCli);
            logger.info("Docker command line: {}", dockerCli);
            runCommandLine(dockerCli);

            // Check result file
            String outFilename = "";
            Path outFile = outPath.resolve(outFilename);
            if (!Files.exists(outFile) || outFile.toFile().length() == 0) {
                File stdoutFile = getOutDir().resolve(DockerWrapperAnalysisExecutor.STDOUT_FILENAME).toFile();
                List<String> stdoutLines = readLines(stdoutFile, Charset.defaultCharset());

                File stderrFile = getOutDir().resolve(DockerWrapperAnalysisExecutor.STDERR_FILENAME).toFile();
                List<String> stderrLines = readLines(stderrFile, Charset.defaultCharset());

                throw new ToolExecutorException("Error executing Liftover: the result file was not created.\nStdout messages: "
                        + StringUtils.join("\n", stdoutLines) + "\nStderr messages: " + StringUtils.join("\n", stderrLines));
            }
        } catch (IOException | ToolException e) {
            throw new ToolExecutorException(e);
        }
    }


    private void runQcAndMapper() {

    }

    private void runVariantCalling() {
        // Execute Python script in docker
        String dockerImage =  "broadinstitute/gatk:4.6.2.0";
    }


    private ObjectMap getParameters() {
        ObjectMap params = new ObjectMap();
        params.put("scriptPath", scriptPath);
        params.put("cmd", command);
        params.put("input", input);
        params.put("resourcePath", resourcePath);
        return params;
    }

    public String getStudy() {
        return study;
    }

    public VariantCallerPipelineWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public Path getScriptPath() {
        return scriptPath;
    }

    public VariantCallerPipelineWrapperAnalysisExecutor setScriptPath(Path scriptPath) {
        this.scriptPath = scriptPath;
        return this;
    }

    public String getCommand() {
        return command;
    }

    public VariantCallerPipelineWrapperAnalysisExecutor setCommand(String command) {
        this.command = command;
        return this;
    }

    public List<String> getInput() {
        return input;
    }

    public VariantCallerPipelineWrapperAnalysisExecutor setInput(List<String> input) {
        this.input = input;
        return this;
    }

    public Path getResourcePath() {
        return resourcePath;
    }

    public VariantCallerPipelineWrapperAnalysisExecutor setResourcePath(Path resourcePath) {
        this.resourcePath = resourcePath;
        return this;
    }
}
