package org.opencb.opencga.analysis.wrappers.clinicalpipeline;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.ngspipeline.NgsPipelineWrapperAnalysis;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.clinical.pipeline.ClinicalPipelineExecuteParams;
import org.opencb.opencga.core.models.clinical.pipeline.ClinicalPipelineWrapperParams;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@ToolExecutor(id = ClinicalPipelineWrapperAnalysisExecutor.ID,
        tool = NgsPipelineWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class ClinicalPipelineWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public static final String ID = NgsPipelineWrapperAnalysis.ID + "-local";

    public static final String PREPARE_CMD = "prepare";
    public static final String RUN_CMD = "run";

    public static final String QUALITY_CONTROL_STEP = "quality-control";
    public static final String ALIGNMENT_STEP = "alignment";
    public static final String VARIANT_CALLING_STEP = "variant-calling";
    public static final Set<String> VALID_PIPELINE_STEPS = new HashSet<>(
            Arrays.asList(QUALITY_CONTROL_STEP, ALIGNMENT_STEP, VARIANT_CALLING_STEP));

    public static final String REFERENCE_GENOME_INDEX = "reference-genome";
    public static final String BWA_INDEX = "bwa";
    public static final String BWA_MEM2_INDEX = "bwa-mem2";

    private static final String NGS_PIPELINE_SCRIPT = "main.py";
    private static final String INDEX_VIRTUAL_PATH = "/index";
    public static final String PIPELINE_PARAMS_FILENAME = "pipeline.json";

    private String study;
    private Path scriptPath;

    private ClinicalPipelineWrapperParams pipelineParams;

    @Override
    protected void run() throws Exception {
        // Add parameters
        addParameters(getParameters());

        // Command was checked in the tool, but we double check here
        if (pipelineParams.getPrepareParams() != null) {
            preparePipeline();
        } else if (pipelineParams.getExecuteParams() != null) {
            executePipeline();
        } else {
            throw new ToolExecutorException("Missing 'prepareParams' or 'executeParams' parameters");
        }
    }

    private void preparePipeline() throws ToolExecutorException {
        try {
            // Input bindings
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
            Path virtualScriptPath = Paths.get(SCRIPT_VIRTUAL_PATH);
            inputBindings.add(new AbstractMap.SimpleEntry<>(scriptPath.toAbsolutePath().toString(), virtualScriptPath.toString()));

            String reference = pipelineParams.getPrepareParams().getReferenceGenome();
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
            String params = "python3 " + virtualScriptPath + "/" +  NGS_PIPELINE_SCRIPT + " prepare"
                    + " -r " + (virtualRefPath != null ? virtualRefPath : reference)
                    + " -o " + OUTPUT_VIRTUAL_PATH;
            params += (" -i " + REFERENCE_GENOME_INDEX);
            if (CollectionUtils.isNotEmpty(pipelineParams.getPrepareParams().getAlignerIndexes())) {
                params += ("," + StringUtils.join(pipelineParams.getPrepareParams().getAlignerIndexes(), ","));
            }

            // Execute Python script in docker
            // TODO: fix and use 5.0.0-SNAPSHOT to include bwa
//            String dockerImage = getDockerImageName() + ":" + getDockerImageVersion();
            String dockerImage = getDockerImageName() + ":4.1.0";

            String dockerCli = buildCommandLine(dockerImage, inputBindings, readOnlyInputBindings, outputBinding, params, null);
            addEvent(Event.Type.INFO, "Docker command line: " + dockerCli);
            logger.info("Docker command line: {}", dockerCli);

            // Execute docker command line
            runCommandLine(dockerCli);
        } catch (IOException | ToolException e) {
            throw new ToolExecutorException(e);
        }
    }

    private void executePipeline() throws ToolExecutorException {
        // Check steps
        List<String> steps = pipelineParams.getExecuteParams().getSteps();
        if (CollectionUtils.isEmpty(steps)) {
            throw new ToolExecutorException("Missing 'steps' parameter");
        }

        // First, run QC (e.g. FastQC) and alignment (e.g. BWA)
        if (steps.contains(QUALITY_CONTROL_STEP) || steps.contains(ALIGNMENT_STEP)) {
            runQcAndAlignment();
        }

        // Then, run the variant calling (e.g. GATK HaplotypeCaller)
        if (steps.contains(VARIANT_CALLING_STEP)) {
            runVariantCalling();
        }
    }

    private void runQcAndAlignment() throws ToolExecutorException {
        try {
            ClinicalPipelineExecuteParams executeParams = pipelineParams.getExecuteParams();

            // Input bindings
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();

            // Read only input bindings
            Set<String> readOnlyInputBindings = new HashSet<>();

            // Script binding
            Path virtualScriptPath = Paths.get(SCRIPT_VIRTUAL_PATH);
            inputBindings.add(new AbstractMap.SimpleEntry<>(scriptPath.toAbsolutePath().toString(), virtualScriptPath.toString()));
            readOnlyInputBindings.add(virtualScriptPath.toString());

            // Index binding
            Path virtualIndexPath = Paths.get(INDEX_VIRTUAL_PATH);
            inputBindings.add(new AbstractMap.SimpleEntry<>(executeParams.getIndexDir(), virtualIndexPath.toString()));
            readOnlyInputBindings.add(virtualIndexPath.toString());

            // Input binding
            List<Path> virtualInputPaths = new ArrayList<>();
            for (int i = 0; i < executeParams.getInput().size(); i++) {
                Path virtualInputPath = Paths.get(INPUT_VIRTUAL_PATH + "_" + i).resolve(Paths.get(executeParams.getInput().get(i))
                        .getFileName());
                inputBindings.add(new AbstractMap.SimpleEntry<>(Paths.get(executeParams.getInput().get(i)).toAbsolutePath().toString(),
                        virtualInputPath.toString()));
                readOnlyInputBindings.add(virtualInputPath.toString());
                virtualInputPaths.add(virtualInputPath);
            }

            // Pipeline params binding
            Path pipelineParamsPath = getOutDir().resolve(PIPELINE_PARAMS_FILENAME);
            Path virtualPipelineParamsPath = Paths.get(INPUT_VIRTUAL_PATH).resolve(pipelineParamsPath.getFileName());

            // Modify BWA index according to the indexPath
            Path bwaIndexPath = null;
            Path bwaIndexDirPath = Paths.get(executeParams.getIndexDir()).resolve("bwa-index");
            for (File file : bwaIndexDirPath.toFile().listFiles()) {
                if (file.getName().endsWith(".fa")) {
                    bwaIndexPath = virtualIndexPath.resolve("bwa-index").resolve(file.getName());
                    break;
                }
            }
            if (bwaIndexPath == null) {
                throw new ToolExecutorException("Could not find the BWA index at " + bwaIndexDirPath);
            }
            List<Map<String, Object>> steps = (List<Map<String, Object>>) executeParams.getPipeline().get("steps");
            for (Map<String, Object> step : steps) {
                if (ALIGNMENT_STEP.equals(step.get("name"))) {
                    Map<String, Object> tool = (Map<String, Object>) step.get("tool");
                    tool.put("index", bwaIndexPath.toAbsolutePath().toString());
                }
            }
            // Write the JSON file containing the pipeline parameters
            JacksonUtils.getDefaultObjectMapper().writerFor(ObjectMap.class).writeValue(pipelineParamsPath.toFile(),
                    executeParams.getPipeline());
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

            String pipelineSteps = "";
            if (executeParams.getSteps().contains(QUALITY_CONTROL_STEP)) {
                pipelineSteps += QUALITY_CONTROL_STEP;
            }
            if (executeParams.getSteps().contains(ALIGNMENT_STEP)) {
                if (!pipelineSteps.isEmpty()) {
                    pipelineSteps += ",";
                }
                pipelineSteps += ALIGNMENT_STEP;
            }

            String params = "python3 " + virtualScriptPath + "/" +  NGS_PIPELINE_SCRIPT + " run"
                    + " -o " + OUTPUT_VIRTUAL_PATH
                    + " -p " + virtualPipelineParamsPath
                    + " -i " + StringUtils.join(virtualInputPaths, ",")
                    + " --index " + virtualIndexPath
                    + " --steps " + pipelineSteps;

            // Execute Python script in docker
            // TODO: fix and use 5.0.0-SNAPSHOT to include bwa
//            String dockerImage = getDockerImageName() + ":" + getDockerImageVersion();
            String dockerImage = getDockerImageName() + ":4.1.0";

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
            ClinicalPipelineExecuteParams executeParams = pipelineParams.getExecuteParams();

            // Input bindings
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();

            // Read only input bindings
            Set<String> readOnlyInputBindings = new HashSet<>();

            // Script binding
            Path virtualScriptPath = Paths.get(SCRIPT_VIRTUAL_PATH);
            inputBindings.add(new AbstractMap.SimpleEntry<>(scriptPath.toAbsolutePath().toString(), virtualScriptPath.toString()));
            readOnlyInputBindings.add(virtualScriptPath.toString());

            // Index binding
            Path virtualIndexPath = Paths.get(INDEX_VIRTUAL_PATH);
            inputBindings.add(new AbstractMap.SimpleEntry<>(executeParams.getIndexDir(), virtualIndexPath.toString()));
            readOnlyInputBindings.add(virtualIndexPath.toString());

            // Pipeline params binding
            Path pipelineParamsPath = getOutDir().resolve(PIPELINE_PARAMS_FILENAME);
            Path virtualPipelineParamsPath = Paths.get(INPUT_VIRTUAL_PATH).resolve(pipelineParamsPath.getFileName());

            // Modify BWA index according to the indexPath
            Path refIndexPath = null;
            Path refIndexDirPath = Paths.get(executeParams.getIndexDir()).resolve("reference-genome-index");
            for (File file : refIndexDirPath.toFile().listFiles()) {
                if (file.getName().endsWith(".fa")) {
                    refIndexPath = virtualIndexPath.resolve("reference-genome-index").resolve(file.getName());
                    break;
                }
            }
            if (refIndexPath == null) {
                throw new ToolExecutorException("Could not find the reference index at " + refIndexDirPath);
            }
            List<Map<String, Object>> steps = (List<Map<String, Object>>) executeParams.getPipeline().get("steps");
            for (Map<String, Object> step : steps) {
                if (VARIANT_CALLING_STEP.equals(step.get("name"))) {
                    List<Map<String, Object>> tools = (List<Map<String, Object>>) step.get("tools");
                    for (Map<String, Object> tool : tools) {
                        if ("gatk".equals(tool.get("name"))) {
                            tool.put("reference", refIndexPath.toAbsolutePath().toString());
                            break;
                        }
                    }
                }
            }
            // Write the JSON file containing the pipeline parameters
            JacksonUtils.getDefaultObjectMapper().writerFor(ObjectMap.class).writeValue(pipelineParamsPath.toFile(),
                    executeParams.getPipeline());
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

            String params = "python3 " + virtualScriptPath + "/" +  NGS_PIPELINE_SCRIPT + " run"
                    + " -o " + OUTPUT_VIRTUAL_PATH
                    + " -p " + virtualPipelineParamsPath
                    + " --index " + virtualIndexPath
                    + " --steps " + VARIANT_CALLING_STEP;

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
        params.put("params", pipelineParams);
        return params;
    }

    public String getStudy() {
        return study;
    }

    public ClinicalPipelineWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public Path getScriptPath() {
        return scriptPath;
    }

    public ClinicalPipelineWrapperAnalysisExecutor setScriptPath(Path scriptPath) {
        this.scriptPath = scriptPath;
        return this;
    }

    public ClinicalPipelineWrapperParams getPipelineParams() {
        return pipelineParams;
    }

    public ClinicalPipelineWrapperAnalysisExecutor setPipelineParams(ClinicalPipelineWrapperParams pipelineParams) {
        this.pipelineParams = pipelineParams;
        return this;
    }
}
