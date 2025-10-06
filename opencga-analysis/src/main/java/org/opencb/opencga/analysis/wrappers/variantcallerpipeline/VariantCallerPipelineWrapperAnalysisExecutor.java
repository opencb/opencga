package org.opencb.opencga.analysis.wrappers.variantcallerpipeline;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@ToolExecutor(id = VariantCallerPipelineWrapperAnalysisExecutor.ID,
        tool = VariantCallerPipelineWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class VariantCallerPipelineWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public static final String ID = VariantCallerPipelineWrapperAnalysis.ID + "-local";

    public static final String PREPARE_CMD = "prepare";
    public static final String PIPELINE_CMD = "pipeline";

    private static final String VARIANT_CALLER_PIPELINE_SCRIPT = "main.py";
    private static final String INDEX_VIRTUAL_PATH = "/index";
    public static final String PIPELINE_PARAMS_FILENAME = "pipeline.json";

    private String study;
    private Path scriptPath;
    private String command;

    private List<String> input;
    private String indexPath;

    private ObjectMap pipelineParams;


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
            AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(),
                    OUTPUT_VIRTUAL_PATH);

            // Main command line and params, e.g.:
            // ./analysis/variant-caller-pipeline/main.py prepare
            // -r https://ftp.ensembl.org/pub/release-115/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.chromosome.22.fa.gz
            // -o /tmp/bbb
            String params = "python3 " + virtualScriptPath + " prepare"
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
        // First, run QC (e.g. FastQC) and mapping (e.g. BWA)
        runQcAndMapper();

        // Then, run the variant calling (e.g. GATK HaplotypeCaller)
        runVariantCalling();
    }

    private void runQcAndMapper() throws ToolExecutorException {
        try {
            // Input bindings
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();

            // Read only input bindings
            Set<String> readOnlyInputBindings = new HashSet<>();

            // Script binding
            Path virtualScriptPath = Paths.get(SCRIPT_VIRTUAL_PATH).resolve(VARIANT_CALLER_PIPELINE_SCRIPT);
            inputBindings.add(new AbstractMap.SimpleEntry<>(scriptPath.resolve(VARIANT_CALLER_PIPELINE_SCRIPT).toAbsolutePath().toString(),
                    virtualScriptPath.toString()));
            readOnlyInputBindings.add(virtualScriptPath.toString());

            // Index binding
            Path virtualIndexPath = Paths.get(INDEX_VIRTUAL_PATH);
            inputBindings.add(new AbstractMap.SimpleEntry<>(indexPath, virtualIndexPath.toString()));
            readOnlyInputBindings.add(virtualIndexPath.toString());

            // Input binding
            List<Path> virtualInputPaths = new ArrayList<>();
            for (int i = 0; i < input.size(); i++) {
                Path virtualInputPath = Paths.get(INPUT_VIRTUAL_PATH + "_" + i).resolve(Paths.get(input.get(i)).getFileName());
                inputBindings.add(new AbstractMap.SimpleEntry<>(Paths.get(input.get(i)).toAbsolutePath().toString(),
                        virtualInputPath.toString()));
                readOnlyInputBindings.add(virtualInputPath.toString());
                virtualInputPaths.add(virtualInputPath);
            }

            // Pipeline params binding
            Path pipelineParamsPath = getOutDir().resolve(PIPELINE_PARAMS_FILENAME);
            Path virtualPipelineParamsPath = Paths.get(INPUT_VIRTUAL_PATH).resolve(pipelineParamsPath.getFileName());
            JacksonUtils.getDefaultObjectMapper().writerFor(ObjectMap.class).writeValue(pipelineParamsPath.toFile(), pipelineParams);
            inputBindings.add(new AbstractMap.SimpleEntry<>(pipelineParamsPath.toAbsolutePath().toString(),
                    virtualPipelineParamsPath.toString()));

            // Output binding
            AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(),
                    OUTPUT_VIRTUAL_PATH);

            // Main command line and params, e.g.:
            // ./analysis/variant-caller-pipeline/main.py run
            // -o /tmp/ngs/
            // -p pipeline.json
            // -i /data/HI.4019.002.index_7.ANN0831_R1.fastq.gz,/data/HI.4019.002.index_7.ANN0831_R2.fastq.gz

            String params = "python3 " + virtualScriptPath + " run"
                    + " -o " + OUTPUT_VIRTUAL_PATH
                    + " -p " + virtualPipelineParamsPath
                    + " -i " + StringUtils.join(virtualInputPaths, ",")
                    + " --index " + virtualIndexPath
                    + " --steps quality_control,alignment";

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

    private void runVariantCalling() throws ToolExecutorException {
        try {
            // Input bindings
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();

            // Read only input bindings
            Set<String> readOnlyInputBindings = new HashSet<>();

            // Script binding
            Path virtualScriptPath = Paths.get(SCRIPT_VIRTUAL_PATH).resolve(VARIANT_CALLER_PIPELINE_SCRIPT);
            inputBindings.add(new AbstractMap.SimpleEntry<>(scriptPath.resolve(VARIANT_CALLER_PIPELINE_SCRIPT).toAbsolutePath().toString(),
                    virtualScriptPath.toString()));
            readOnlyInputBindings.add(virtualScriptPath.toString());

            // Index binding
            Path virtualIndexPath = Paths.get(INDEX_VIRTUAL_PATH);
            inputBindings.add(new AbstractMap.SimpleEntry<>(indexPath, virtualIndexPath.toString()));
            readOnlyInputBindings.add(virtualIndexPath.toString());

            // Pipeline params binding
            Path pipelineParamsPath = getOutDir().resolve(PIPELINE_PARAMS_FILENAME);
            Path virtualPipelineParamsPath = Paths.get(INPUT_VIRTUAL_PATH).resolve(pipelineParamsPath.getFileName());
            JacksonUtils.getDefaultObjectMapper().writerFor(ObjectMap.class).writeValue(pipelineParamsPath.toFile(), pipelineParams);
            inputBindings.add(new AbstractMap.SimpleEntry<>(pipelineParamsPath.toAbsolutePath().toString(),
                    virtualPipelineParamsPath.toString()));

            // Output binding
            AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(),
                    OUTPUT_VIRTUAL_PATH);

            // Main command line and params, e.g.:
            // ./analysis/variant-caller-pipeline/main.py run
            // -o /tmp/ngs/
            // -p pipeline.json
            // -i /data/HI.4019.002.index_7.ANN0831_R1.fastq.gz,/data/HI.4019.002.index_7.ANN0831_R2.fastq.gz

            String params = "python3 " + virtualScriptPath + " run"
                    + " -o " + OUTPUT_VIRTUAL_PATH
                    + " -p " + virtualPipelineParamsPath
                    + " --index " + virtualIndexPath
                    + " --steps variant_calling";

            // Execute Python script in docker
            String dockerImage =  "broadinstitute/gatk:4.6.2.0";

            String dockerCli = buildCommandLine(dockerImage, inputBindings, readOnlyInputBindings, outputBinding, params, null);
            addEvent(Event.Type.INFO, "Docker command line: " + dockerCli);
            logger.info("Docker command line: {}", dockerCli);

            // Execute docker command line
            runCommandLine(dockerCli);
        } catch (IOException | ToolException e) {
            throw new ToolExecutorException(e);
        }
    }


    private ObjectMap getParameters() {
        ObjectMap params = new ObjectMap();
        params.put("scriptPath", scriptPath);
        params.put("cmd", command);
        params.put("input", input);
        params.put("indexPath", indexPath);
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

    public String getIndexPath() {
        return indexPath;
    }

    public VariantCallerPipelineWrapperAnalysisExecutor setIndexPath(String indexPath) {
        this.indexPath = indexPath;
        return this;
    }

    public ObjectMap getPipelineParams() {
        return pipelineParams;
    }

    public VariantCallerPipelineWrapperAnalysisExecutor setPipelineParams(ObjectMap pipelineParams) {
        this.pipelineParams = pipelineParams;
        return this;
    }
}
