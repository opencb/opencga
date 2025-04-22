package org.opencb.opencga.analysis.wrappers.regenie;

import org.opencb.commons.datastore.core.Event;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.analysis.wrappers.regenie.RegenieUtils.STEP1_PRED_LIST_FILNEMANE;

@ToolExecutor(id = RegenieStep1WrapperAnalysisExecutor.ID,
        tool = RegenieStep1WrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class RegenieStep1WrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public static final String ID = RegenieStep1WrapperAnalysis.ID + "-local";

    private String study;
    private Path step1ScriptPath;
    private Path inputPath;
    private Path outputPath;

    @Override
    protected void run() throws Exception {
        try {
            // Input bindings
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
            Path virtualScriptPath = Paths.get(SCRIPT_VIRTUAL_PATH).resolve(step1ScriptPath.getFileName());
            inputBindings.add(new AbstractMap.SimpleEntry<>(step1ScriptPath.toAbsolutePath().toString(), virtualScriptPath.toString()));
            inputBindings.add(new AbstractMap.SimpleEntry<>(inputPath.toAbsolutePath().toString(), INPUT_VIRTUAL_PATH));

            // Read only input bindings
            Set<String> readOnlyInputBindings = new HashSet<>();
            readOnlyInputBindings.add(virtualScriptPath.toString());

            // Output binding
            AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(outputPath.toAbsolutePath().toString(),
                    OUTPUT_VIRTUAL_PATH);

            // Main command line and params
            String params = "bash " + virtualScriptPath
                    + " " + INPUT_VIRTUAL_PATH
                    + " " + RegenieUtils.VCF_BASENAME
                    + " " + RegenieUtils.PHENO_FILENAME
                    + " " + OUTPUT_VIRTUAL_PATH;

            // Execute Pythong script in docker
            String dockerImage = getDockerImageName() + ":" + getDockerImageVersion();

            String dockerCli = buildCommandLine(dockerImage, inputBindings, readOnlyInputBindings, outputBinding, params, null);
            addEvent(Event.Type.INFO, "Docker command line: " + dockerCli);
            logger.info("Docker command line: {}", dockerCli);
            runCommandLine(dockerCli);

            // Check result files
            Path step1PredPath = getOutputPath().resolve(STEP1_PRED_LIST_FILNEMANE);
            if (!Files.exists(step1PredPath)) {
                throw new ToolExecutorException("Could not find the regenie-step1 predictions file: " + STEP1_PRED_LIST_FILNEMANE);
            }
            List<String> lines = Files.readAllLines(step1PredPath);
            for (String line : lines) {
                String[] split = line.split(" ");
                Path locoPath = getOutputPath().resolve(Paths.get(split[1]).getFileName());
                if (!Files.exists(locoPath)) {
                    throw new ToolExecutorException("Could not find the regenie-step1 loco file: " + locoPath.getFileName());
                }
            }
        } catch (IOException | ToolException e) {
            throw new ToolExecutorException(e);
        }
    }

    @Override
    public String getDockerImageName() throws ToolException {
        return "opencb/opencga-regenie";
    }

    @Override
    public String getDockerImageVersion() {
        return "4.0.0-SNAPSHOT";
    }

    public String getStudy() {
        return study;
    }

    public RegenieStep1WrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public Path getStep1ScriptPath() {
        return step1ScriptPath;
    }

    public RegenieStep1WrapperAnalysisExecutor setStep1ScriptPath(Path step1ScriptPath) {
        this.step1ScriptPath = step1ScriptPath;
        return this;
    }

    public Path getInputPath() {
        return inputPath;
    }

    public RegenieStep1WrapperAnalysisExecutor setInputPath(Path inputPath) {
        this.inputPath = inputPath;
        return this;
    }

    public Path getOutputPath() {
        return outputPath;
    }

    public RegenieStep1WrapperAnalysisExecutor setOutputPath(Path outputPath) {
        this.outputPath = outputPath;
        return this;
    }
}
