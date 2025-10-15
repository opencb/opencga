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
import org.opencb.opencga.core.models.clinical.pipeline.*;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.analysis.wrappers.clinicalpipeline.ClinicalPipelineWrapperAnalysis.SAMPLE_FIELD_SEP;
import static org.opencb.opencga.analysis.wrappers.clinicalpipeline.ClinicalPipelineWrapperAnalysis.SAMPLE_FILE_SEP;

@ToolExecutor(id = ClinicalPipelineWrapperAnalysisExecutor.ID,
        tool = NgsPipelineWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class ClinicalPipelineWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public static final String ID = NgsPipelineWrapperAnalysis.ID + "-local";

    public static final String PREPARE_CMD = "prepare";
    public static final String GENOMICS_CMD = "genomics";

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

    public static boolean isURL(String input) {
        return input.startsWith("http://") || input.startsWith("https://") || input.startsWith("ftp://");
    }

    private void preparePipeline() throws ToolExecutorException {
        try {
            // Input bindings
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
            Path virtualScriptPath = Paths.get(SCRIPT_VIRTUAL_PATH);
            inputBindings.add(new AbstractMap.SimpleEntry<>(scriptPath.toAbsolutePath().toString(), virtualScriptPath.toString()));

            String reference = pipelineParams.getPrepareParams().getReferenceGenome();
            Path virtualRefPath = null;
            if (!isURL(reference)) {
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
            String params = "python3 " + virtualScriptPath + "/" + NGS_PIPELINE_SCRIPT + " " + PREPARE_CMD
                    + " -r " + (virtualRefPath != null ? virtualRefPath : reference)
                    + " -o " + OUTPUT_VIRTUAL_PATH;
            params += (" -i " + REFERENCE_GENOME_INDEX);
            if (CollectionUtils.isNotEmpty(pipelineParams.getPrepareParams().getAlignerIndexes())) {
                params += ("," + StringUtils.join(pipelineParams.getPrepareParams().getAlignerIndexes(), ","));
            }

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

    private void executePipeline() throws ToolExecutorException {
        // Check samples (mandatory)
        List<String> samples = pipelineParams.getExecuteParams().getSamples();
        if (CollectionUtils.isEmpty(samples)) {
            throw new ToolExecutorException("Missing 'samples' parameter");
        }

        // Check steps (mandatory)
        List<String> steps = pipelineParams.getExecuteParams().getSteps();
        if (CollectionUtils.isEmpty(steps)) {
            throw new ToolExecutorException("Missing 'steps' parameter");
        }

        // First, run QC (e.g. FastQC) and alignment (e.g. BWA)
        List<String> variantCallingSamples = samples;
        if (steps.contains(QUALITY_CONTROL_STEP) || steps.contains(ALIGNMENT_STEP)) {
            variantCallingSamples = runQcAndAlignment();
        }

        // Then, run the variant calling (e.g. GATK HaplotypeCaller)
        if (steps.contains(VARIANT_CALLING_STEP)) {
            runVariantCalling(variantCallingSamples);
        }
    }

    private List<String> runQcAndAlignment() throws ToolExecutorException {
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
            Path virtualIndexPath = null;
            if (!StringUtils.isEmpty(executeParams.getIndexDir())) {
                virtualIndexPath = Paths.get(INDEX_VIRTUAL_PATH);
                inputBindings.add(new AbstractMap.SimpleEntry<>(executeParams.getIndexDir(), virtualIndexPath.toString()));
                readOnlyInputBindings.add(virtualIndexPath.toString());
            }

            // Input binding, and update samples with virtual paths
            List<String> updatedSamples = new ArrayList<>(executeParams.getSamples().size());
            int inputCounter = 0;
            for (String sample : executeParams.getSamples()) {
                String[] fields = sample.split(SAMPLE_FIELD_SEP);
                String[] files = fields[1].split(SAMPLE_FILE_SEP);
                String updatedSample = fields[0];
                List<String> updatedFiles = new ArrayList<>(files.length);
                for (String file : files) {
                    Path virtualInputPath = Paths.get(INPUT_VIRTUAL_PATH + "_" + inputCounter).resolve(Paths.get(file).getFileName());
                    inputBindings.add(new AbstractMap.SimpleEntry<>(Paths.get(file).toAbsolutePath().toString(),
                            virtualInputPath.toString()));
                    readOnlyInputBindings.add(virtualInputPath.toString());
                    inputCounter++;

                    // Updated files
                    updatedFiles.add(virtualInputPath.toString());
                }
                // Add updated files
                updatedSample += SAMPLE_FIELD_SEP + StringUtils.join(updatedFiles, SAMPLE_FILE_SEP);

                // Add the remain fields
                for (int i = 2; i < fields.length; i++) {
                    updatedSample += SAMPLE_FIELD_SEP + fields[i];
                }

                updatedSamples.add(updatedSample);
            }

            // Pipeline params binding
            Path pipelineParamsPath = getOutDir().resolve(PIPELINE_PARAMS_FILENAME);
            Path virtualPipelineParamsPath = Paths.get(INPUT_VIRTUAL_PATH).resolve(pipelineParamsPath.getFileName());

            // Modify BWA index according to the indexPath
            updateAlignmentIndex(inputBindings, readOnlyInputBindings);

            // Write the JSON file containing the pipeline parameters
            JacksonUtils.getDefaultObjectMapper().writerFor(PipelineConfig.class).writeValue(pipelineParamsPath.toFile(),
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

            String params = "python3 " + virtualScriptPath + "/" + NGS_PIPELINE_SCRIPT + " " + GENOMICS_CMD
                    + " -o " + OUTPUT_VIRTUAL_PATH
                    + " -p " + virtualPipelineParamsPath
                    + " -s " + StringUtils.join(updatedSamples, ClinicalPipelineWrapperAnalysis.SAMPLE_SEP)
                    + " --steps " + pipelineSteps;
            if (virtualIndexPath != null) {
                params += (" --index-dir " + virtualIndexPath);
            }

            // Execute Python script in docker
            String dockerImage = getDockerImageName() + ":" + getDockerImageVersion();

            String dockerCli = buildCommandLine(dockerImage, inputBindings, readOnlyInputBindings, outputBinding, params, null);
            addEvent(Event.Type.INFO, "Docker command line: " + dockerCli);
            logger.info("Docker command line: {}", dockerCli);

            // Execute docker command line
            runCommandLine(dockerCli);

            // Check output files from the alignment step from the output directory (alignment directory) and create the sample list
            // for the variant-calling step
            List<String> variantCallingSamples = new ArrayList<>(updatedSamples.size());
            Path alignmentPath = getOutDir().resolve(ALIGNMENT_STEP);
            if (Files.exists(alignmentPath) && Files.exists(alignmentPath.resolve("DONE"))) {
                for (String sample : updatedSamples) {
                    String[] fields = sample.split(SAMPLE_FIELD_SEP);
                    String sampleId = fields[0];
                    Path bamFile = alignmentPath.resolve(sampleId + ".sorted.bam");
                    if (!Files.exists(bamFile) || !Files.isRegularFile(bamFile) || Files.size(bamFile) == 0) {
                        throw new ToolExecutorException("Missing or empty BAM file for sample " + sampleId + ": " + bamFile);
                    }
                    Path baiFile = alignmentPath.resolve(sampleId + ".sorted.bam.bai");
                    if (!Files.exists(baiFile) || !Files.isRegularFile(baiFile) || Files.size(baiFile) == 0) {
                        throw new ToolExecutorException("Missing or empty BAI file for sample " + sampleId + ": " + baiFile);
                    }

                    // Create the variant calling sample
                    StringBuilder variantCallingSample = new StringBuilder();
                    // Add sample ID and the BAM file
                    variantCallingSample.append(sampleId).append(SAMPLE_FIELD_SEP).append(bamFile.toAbsolutePath());
                    // Add somatic and role fields if provided
                    for (int i = 2; i < fields.length; i++) {
                        variantCallingSample.append(SAMPLE_FIELD_SEP).append(fields[i]);
                    }

                    // Add to the list
                    variantCallingSamples.add(variantCallingSample.toString());
                }
                return variantCallingSamples;
            } else {
                throw new ToolExecutorException("Missing DONE file from alignment step: " + alignmentPath.resolve("DONE"));
            }
        } catch (IOException | ToolException e) {
            throw new ToolExecutorException(e);
        }
    }

    private void updateAlignmentIndex(List<AbstractMap.SimpleEntry<String, String>> inputBindings,
                                      Set<String> readOnlyInputBindings) throws ToolExecutorException {
        Path virtualAlignmentIndexPath;
        String indexDir = pipelineParams.getExecuteParams().getIndexDir();
        for (PipelineStep step : pipelineParams.getExecuteParams().getPipeline().getSteps()) {
            if ("alignment".equalsIgnoreCase(step.getId())) {
                if (StringUtils.isEmpty(step.getTool().getIndex())) {
                    // Use the index directory for the alignment step
                    if (StringUtils.isEmpty(indexDir)) {
                        throw new ToolExecutorException("Missing index directory for step alignment");
                    }
                    String subfolder = step.getTool().getId() + "-index";
                    Path alignmentIndexPath = Paths.get(indexDir).resolve(subfolder);
                    if (Files.exists(alignmentIndexPath)) {
                        virtualAlignmentIndexPath = Paths.get(INDEX_VIRTUAL_PATH).resolve(subfolder);
                        step.getTool().setIndex(virtualAlignmentIndexPath.toAbsolutePath().toString());
                        logger.info("Using index {} for step alignment", step.getTool().getIndex());
                        return;
                    } else {
                        throw new ToolExecutorException("Index directory for step alignment not found: " + alignmentIndexPath);
                    }
                } else {
                    // Use the index defined in the tool
                    Path parent;
                    Path alignmentIndexPath = Paths.get(step.getTool().getIndex());
                    if (Files.exists(alignmentIndexPath)) {
                        if (Files.isRegularFile(alignmentIndexPath)) {
                            parent = alignmentIndexPath.getParent();
                        } else if (Files.isDirectory(alignmentIndexPath)) {
                            parent = alignmentIndexPath;
                        } else {
                            throw new ToolExecutorException("Index path for step alignment is neither a file nor a directory: "
                                    + alignmentIndexPath);
                        }
                        virtualAlignmentIndexPath = Paths.get(INDEX_VIRTUAL_PATH + "_alignment");
                        inputBindings.add(new AbstractMap.SimpleEntry<>(parent.toAbsolutePath().toString(),
                                virtualAlignmentIndexPath.toAbsolutePath().toString()));
                        readOnlyInputBindings.add(virtualAlignmentIndexPath.toAbsolutePath().toString());
                        step.getTool().setIndex(virtualAlignmentIndexPath.toAbsolutePath().toString());
                        logger.info("Using index {} for step alignment", step.getTool().getIndex());
                        return;
                    } else {
                        throw new ToolExecutorException("Index directory for step alignment not found: " + alignmentIndexPath);
                    }
                }
            }
        }
        throw new ToolExecutorException("Could not set index directory for step alignment");
    }

    private void updateVariantCallingIndex(List<AbstractMap.SimpleEntry<String, String>> inputBindings,
                                           Set<String> readOnlyInputBindings) throws ToolExecutorException {
        Path virtualRefGenomeIndexPath;
        String indexDir = pipelineParams.getExecuteParams().getIndexDir();
        for (PipelineStep step : pipelineParams.getExecuteParams().getPipeline().getSteps()) {
            if ("variant-calling".equalsIgnoreCase(step.getId())) {
                // Variant calling may have a different tools to run
                if (CollectionUtils.isEmpty(step.getTools())) {
                    throw new ToolExecutorException("Missing tools for step variant-calling");
                }

                // Update all the tools in the step variant-calling
                for (PipelineTool tool : step.getTools()) {
                    if (StringUtils.isEmpty(tool.getReference())) {
                        // Use the index directory for the step variant-calling
                        // Sanity check
                        if (StringUtils.isEmpty(indexDir)) {
                            throw new ToolExecutorException("Missing index directory for step variant-calling");
                        }
                        String subfolder = "reference-genome-index";
                        Path refGenomeIndexPath = Paths.get(indexDir).resolve(subfolder);
                        if (!Files.exists(refGenomeIndexPath)) {
                            throw new ToolExecutorException("Reference genome index directory for step variant-calling not found: "
                                    + refGenomeIndexPath);
                        }
                        virtualRefGenomeIndexPath = Paths.get(INDEX_VIRTUAL_PATH).resolve(subfolder);
                        tool.setReference(virtualRefGenomeIndexPath.toAbsolutePath().toString());
                        logger.info("Using reference genome index {} for step variant-calling with {}", tool.getIndex(), tool.getId());
                    } else {
                        // Use the reference dir defined in the tool
                        Path refGenomeIndexPath = Paths.get(tool.getReference());
                        // Sanity check
                        if (!Files.exists(refGenomeIndexPath)) {
                            throw new ToolExecutorException("Reference genome index directory for step variant-calling with "
                                    + tool.getId() + ": " + refGenomeIndexPath);
                        }
                        if (!Files.isDirectory(refGenomeIndexPath)) {
                            throw new ToolExecutorException("Reference genome index path for step variant-calling with "
                                    + tool.getId() + " is not a directory: " + refGenomeIndexPath);
                        }
                        virtualRefGenomeIndexPath = Paths.get(INDEX_VIRTUAL_PATH + "_ref_genome_index_" + tool.getId());
                        inputBindings.add(new AbstractMap.SimpleEntry<>(refGenomeIndexPath.toAbsolutePath().toString(),
                                virtualRefGenomeIndexPath.toAbsolutePath().toString()));
                        readOnlyInputBindings.add(virtualRefGenomeIndexPath.toAbsolutePath().toString());
                        tool.setReference(virtualRefGenomeIndexPath.toAbsolutePath().toString());
                        logger.info("Using reference genome index {} for step variant-calling with {}", tool.getReference(), tool.getId());
                    }
                }
            }
        }
    }

    private void runVariantCalling(List<String> samples) throws ToolExecutorException {
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

            // Input binding, and update samples with virtual paths
            List<String> updatedSamples = new ArrayList<>();
            int inputCounter = 0;
            for (String sample : samples) {
                String[] fields = sample.split(SAMPLE_FIELD_SEP);
                String[] files = fields[1].split(SAMPLE_FILE_SEP);
                StringBuilder updatedSample = new StringBuilder();
                updatedSample.append(fields[0]);
                List<String> updatedFiles = new ArrayList<>(files.length);
                for (String file : files) {
                    Path virtualInputPath = Paths.get(INPUT_VIRTUAL_PATH + "_" + inputCounter).resolve(Paths.get(file).getFileName());
                    inputBindings.add(new AbstractMap.SimpleEntry<>(Paths.get(file).toAbsolutePath().toString(),
                            virtualInputPath.toString()));
                    readOnlyInputBindings.add(virtualInputPath.toString());
                    inputCounter++;

                    // Updated files
                    updatedFiles.add(virtualInputPath.toString());
                }
                // Add updated files
                updatedSample.append(SAMPLE_FIELD_SEP).append(StringUtils.join(updatedFiles, SAMPLE_FILE_SEP));

                // Add the remain fields
                for (int i = 2; i < fields.length; i++) {
                    updatedSample.append(SAMPLE_FIELD_SEP).append(fields[i]);
                }

                updatedSamples.add(updatedSample.toString());
            }

            // Pipeline params binding
            Path pipelineParamsPath = getOutDir().resolve(PIPELINE_PARAMS_FILENAME);
            Path virtualPipelineParamsPath = Paths.get(INPUT_VIRTUAL_PATH).resolve(pipelineParamsPath.getFileName());

            // Modify variant calling index
            updateVariantCallingIndex(inputBindings, readOnlyInputBindings);

            // Write the JSON file containing the pipeline parameters
            JacksonUtils.getDefaultObjectMapper().writerFor(PipelineConfig.class).writeValue(pipelineParamsPath.toFile(),
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

            String params = "python3 " + virtualScriptPath + "/" +  NGS_PIPELINE_SCRIPT + " " + GENOMICS_CMD
                    + " -o " + OUTPUT_VIRTUAL_PATH
                    + " -p " + virtualPipelineParamsPath
                    + " -s " + StringUtils.join(updatedSamples, ClinicalPipelineWrapperAnalysis.SAMPLE_SEP)
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
