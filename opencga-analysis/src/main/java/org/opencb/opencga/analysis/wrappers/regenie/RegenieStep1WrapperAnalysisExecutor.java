package org.opencb.opencga.analysis.wrappers.regenie;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.analysis.wrappers.regenie.RegenieUtils.*;

@ToolExecutor(id = RegenieStep1WrapperAnalysisExecutor.ID,
        tool = RegenieStep1WrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class RegenieStep1WrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public static final String ID = RegenieStep1WrapperAnalysis.ID + "-local";

    private String study;
    private ObjectMap options;
    private Path outputPath;

    @Override
    protected void run() throws Exception {
        try {
            // Add regenie parameters
            addParameters(options);

            // Main command line and params
            Path inputPath = null;
            StringBuilder params = new StringBuilder("regenie --step 1");
            params.append(" --out ").append(OUTPUT_VIRTUAL_PATH).append("/step1");
            if (options != null) {
                logger.info("Regenie options: {}", options.toJson());
                for (String key : options.keySet()) {
                    if (SKIP_OPTIONS.contains(key)) {
                        continue;
                    }
                    if (ALL_FILE_OPTIONS.contains(key)) {
                        // Sanity check
                        if (StringUtils.isEmpty(options.getString(key))) {
                            throw new ToolExecutorException("Missing value for file option: " + key);
                        }
                        Path path = Paths.get(options.getString(key));
                        if (!Files.exists(path)) {
                            throw new ToolExecutorException("File not found: " + path);
                        }
                        // Add to input bindings
                        if (inputPath == null) {
                            inputPath = path.getParent();
                        } else if (!inputPath.toAbsolutePath().toString().equals(path.getParent().toAbsolutePath().toString())) {
                            throw new ToolExecutorException("All input files are expected to be in the same directory: " + inputPath
                                    + " vs " + path);
                        }

                        // Remove extension
                        String filename = path.getFileName().toString();
                        if (filename.endsWith(".bed")) {
                            filename = filename.substring(0, filename.length() - 4);
                        } else if (filename.endsWith(".bgen") || filename.endsWith(".pgen")) {
                            filename = filename.substring(0, filename.length() - 5);
                        }
                        params.append(" ").append(key).append(" ").append(INPUT_VIRTUAL_PATH).append("/").append(filename);
                    } else {
                        if (options.get(key) instanceof String) {
                            String value = options.getString(key);
                            if ("TRUE".equalsIgnoreCase(value)) {
                                params.append(" ").append(key);    // Sanity check
                            } else if (!"FALSE".equalsIgnoreCase(value)) {
                                params.append(" ").append(key).append(" ").append(value);
                            }
                        } else {
                            params.append(" ").append(key).append(" ").append(options.get(key));
                        }
                    }
                }
            }

            // Input bindings
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
            inputBindings.add(new AbstractMap.SimpleEntry<>(inputPath.toAbsolutePath().toString(), INPUT_VIRTUAL_PATH));

            // Read only input bindings
            Set<String> readOnlyInputBindings = new HashSet<>();
            readOnlyInputBindings.add(inputPath.toAbsolutePath().toString());

            // Output binding
            AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(outputPath.toAbsolutePath().toString(),
                    OUTPUT_VIRTUAL_PATH);

            // Execute Pythong script in docker
            String dockerImage = getDockerImageName() + ":" + getDockerImageVersion();

            String dockerCli = buildCommandLine(dockerImage, inputBindings, readOnlyInputBindings, outputBinding, params.toString(), null);
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

    public String getStudy() {
        return study;
    }

    public RegenieStep1WrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public ObjectMap getOptions() {
        return options;
    }

    public RegenieStep1WrapperAnalysisExecutor setOptions(ObjectMap options) {
        this.options = options;
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
