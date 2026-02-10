package org.opencb.opencga.analysis.wrappers.clinicalpipeline.genomics;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.pipeline.PipelineSample;
import org.opencb.opencga.core.models.clinical.pipeline.genomics.*;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.operations.variant.VariantIndexParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

import static org.opencb.opencga.analysis.wrappers.clinicalpipeline.ClinicalPipelineUtils.*;
import static org.opencb.opencga.catalog.utils.ResourceManager.ANALYSIS_DIRNAME;

@Tool(id = GenomicsClinicalPipelineWrapperAnalysis.ID, resource = Enums.Resource.VARIANT,
        description = GenomicsClinicalPipelineWrapperAnalysis.DESCRIPTION)
public class GenomicsClinicalPipelineWrapperAnalysis extends OpenCgaTool {

    public static final String ID = "ngs-pipeline-genomics";
    public static final String DESCRIPTION = "Execute the clinical genomics pipeline that performs QC (FastQC,...), mapping (BWA,"
        + " Bowtie,...) , variant calling (GATK,...) and variant indexing in OpenCGA storage.";

    private static final String GENOMICS_PIPELINE_STEP = "genomics-pipeline";
    private static final String VARIANT_INDEX_STEP = "variant-index";

    // Genomics pipeline
    private static final String ALIGNMENT_PIPELINE_STEP = "alignment";
    private static final String VARIANT_CALLING_PIPELINE_STEP = "variant-calling";
    private static final Set<String> VALID_GENOMIC_PIPELINE_STEPS = new HashSet<>(Arrays.asList(QUALITY_CONTROL_PIPELINE_STEP,
            ALIGNMENT_PIPELINE_STEP, VARIANT_CALLING_PIPELINE_STEP));

    private List<String> pipelineSteps;
    private GenomicsPipelineConfig updatedPipelineConfig = new GenomicsPipelineConfig();

    private List<String> variantCallingToolIds = new ArrayList<>();

    @ToolParams
    protected final GenomicsClinicalPipelineWrapperParams analysisParams = new GenomicsClinicalPipelineWrapperParams();

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        // Check pipeline configuration
        updatedPipelineConfig = checkPipelineConfig();

        // Update from params: samples, data dir and index dir
        updatePipelineConfigFromParams();

        // Check pipeline steps
        checkPipelineSteps();
    }

    @Override
    protected List<String> getSteps() {
        if (analysisParams.getPipelineParams().getVariantIndexParams() != null) {
            return Arrays.asList(GENOMICS_PIPELINE_STEP, VARIANT_INDEX_STEP);
        } else {
            return Collections.singletonList(GENOMICS_PIPELINE_STEP);
        }
    }

    @Override
    protected void run() throws ToolException, IOException {
        // Execute the pipeline
        step(GENOMICS_PIPELINE_STEP, this::runGenomicsPipeline);

        if (analysisParams.getPipelineParams().getVariantIndexParams() != null) {
            // Index variants in OpenCGA storage
            step(VARIANT_INDEX_STEP, this::runVariantIndex);
        }
    }

    private void runGenomicsPipeline() throws  ToolException {
        // Get executor
        GenomicsClinicalPipelineWrapperAnalysisExecutor executor = getToolExecutor(GenomicsClinicalPipelineWrapperAnalysisExecutor.class);

        // Set parameters and execute
        executor.setStudy(study)
                .setScriptPath(getOpencgaHome().resolve(ANALYSIS_DIRNAME).resolve(PIPELINE_ANALYSIS_DIRNAME))
                .setPipelineConfig(updatedPipelineConfig)
                .setPipelineSteps(pipelineSteps)
                .execute();
    }

    private void runVariantIndex() throws CatalogException, StorageEngineException, ToolException, IOException {
        // Find the .sorted.<variant calling tool ID>.vcf.gz file within the variant-calling folder
        Path vcfPath;
        for (String variantCallingToolId : variantCallingToolIds) {
            try (Stream<Path> stream = Files.list(getOutDir().resolve(VARIANT_CALLING_PIPELINE_STEP).resolve(variantCallingToolId))) {
                vcfPath = stream
                        .filter(Files::isRegularFile)
                        .filter(path -> path.getFileName().toString().endsWith(variantCallingToolId + ".vcf.gz"))
                        .findFirst()
                        .orElse(null);
            }

            if (vcfPath == null || !Files.exists(vcfPath)) {
                throw new ToolException("Could not find the generated VCF: " + vcfPath + " from variant caller " + variantCallingToolId);
            }
            File vcfFile = catalogManager.getFileManager().link(study, new FileLinkParams(vcfPath.toAbsolutePath().toString(),
                    "", "", "", null, null, null, null, null), false, token).first();

            VariantIndexParams storageOptions = analysisParams.getPipelineParams().getVariantIndexParams() != null
                    ? analysisParams.getPipelineParams().getVariantIndexParams()
                    : new VariantIndexParams();

            getVariantStorageManager().index(study, vcfFile.getId(), getScratchDir().toAbsolutePath().toString(), storageOptions, token);
        }
    }

    private GenomicsPipelineConfig checkPipelineConfig() throws ToolException, CatalogException, IOException {
        // Check pipeline configuration, if a pipeline file is provided, check the file exists and then read the file content
        // to add it to the params
        GenomicsClinicalPipelineParams params = analysisParams.getPipelineParams();
        if (StringUtils.isEmpty(params.getPipelineFile()) && params.getPipeline() == null) {
            throw new ToolException("Missing genomics pipeline configuration. You can either provide a pipeline configuration file or"
                    + " directly the pipeline configuration.");
        }

        // Get pipeline config
        if (!StringUtils.isEmpty(params.getPipelineFile())) {
            String pipelineFile = params.getPipelineFile();
            logger.info("Checking genomics pipeline configuration file {}", pipelineFile);
            File opencgaFile = catalogManager.getFileManager().get(study, pipelineFile, QueryOptions.empty(), token).first();
            if (opencgaFile.getType() != File.Type.FILE) {
                throw new ToolException("Genomics pipeline configuration file '" + pipelineFile + "' is not a file.");
            }
            Path pipelinePath = Paths.get(opencgaFile.getUri()).toAbsolutePath();
            return JacksonUtils.getDefaultObjectMapper().readerFor(GenomicsPipelineConfig.class).readValue(pipelinePath.toFile());
        } else {
            logger.info("Getting genomics pipeline configuration provided directly in the parameters");
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(mapper.writeValueAsString(params.getPipeline()), GenomicsPipelineConfig.class);
        }
    }

    private void updatePipelineConfigFromParams() throws ToolException, CatalogException {
        // Init input if not present
        if (updatedPipelineConfig.getInput() == null) {
            updatedPipelineConfig.setInput(new GenomicsPipelineInput());
        }

        // Update sample files (by getting the real paths) in the pipeline configuration
        if (CollectionUtils.isNotEmpty(analysisParams.getPipelineParams().getSamples())) {
            List<PipelineSample> pipelineSamples = new ArrayList<>();
            for (String sample : analysisParams.getPipelineParams().getSamples()) {
                PipelineSample pipelineSample = createPipelineSampleFromString(sample);
                List<String> updatedFiles = new ArrayList<>();
                for (String file : pipelineSample.getFiles()) {
                    Path path = getPhysicalFilePath(file, study, catalogManager, token);
                    updatedFiles.add(path.toString());
                }
                // Set updated files in the sample
                pipelineSample.setFiles(updatedFiles);
                // And add to the list of samples
                pipelineSamples.add(pipelineSample);
            }
            // Add updated samples to the pipeline config
            if (CollectionUtils.isNotEmpty(pipelineSamples)) {
                updatedPipelineConfig.getInput().setSamples(pipelineSamples);
            }
        }

        // If index dir is provided in the pipeline params, set it in the pipeline config to be processed later
        if (StringUtils.isNotEmpty(analysisParams.getPipelineParams().getIndexDir())) {
            Path path = getPhysicalDirPath(analysisParams.getPipelineParams().getIndexDir(), study, catalogManager, token);
            updatedPipelineConfig.getInput().setIndexDir(path.toString());
        }
    }

    private void checkPipelineSteps() throws ToolException, CatalogException {
        // Ensure pipeline config has steps
        if (updatedPipelineConfig.getSteps() == null || (updatedPipelineConfig.getSteps().getQualityControl() == null
                && updatedPipelineConfig.getSteps().getAlignment() == null
                && updatedPipelineConfig.getSteps().getVariantCalling() == null)) {
            throw new ToolException("All clinical pipeline configuration steps are missing.");
        }

        // Validate each step exists in config and has required tools
        validateStepsInConfiguration();
    }

    private void validateStepsInConfiguration() throws ToolException, CatalogException {
        pipelineSteps = analysisParams.getPipelineParams().getSteps();
        // If no steps are provided, set all the steps by default
        if (CollectionUtils.isEmpty(pipelineSteps)) {
            pipelineSteps = Arrays.asList(QUALITY_CONTROL_PIPELINE_STEP, ALIGNMENT_PIPELINE_STEP, VARIANT_CALLING_PIPELINE_STEP);
        }

        // Validate provided steps in the
        for (String step : pipelineSteps) {
            if (!VALID_GENOMIC_PIPELINE_STEPS.contains(step)) {
                throw new ToolException("Clinical pipeline step '" + step + "' is not valid. Supported steps are: "
                        + String.join(", ", VALID_GENOMIC_PIPELINE_STEPS));
            }

            switch (step) {
                case QUALITY_CONTROL_PIPELINE_STEP: {
                    if (updatedPipelineConfig.getSteps().getQualityControl() == null) {
                        throw new ToolException("Clinical pipeline step '" + QUALITY_CONTROL_PIPELINE_STEP + "' is not present in the"
                                + " pipeline configuration.");
                    }
                    validateTool(QUALITY_CONTROL_PIPELINE_STEP, updatedPipelineConfig.getSteps().getQualityControl().getTool());
                    break;
                }

                case ALIGNMENT_PIPELINE_STEP: {
                    if (updatedPipelineConfig.getSteps().getAlignment() == null) {
                        throw new ToolException("Clinical pipeline step '" + ALIGNMENT_PIPELINE_STEP + "' is not present in the"
                                + " pipeline configuration.");
                    }
                    validateAlignmentTool(updatedPipelineConfig.getSteps().getAlignment().getTool());
                    break;
                }

                case VARIANT_CALLING_PIPELINE_STEP: {
                    if (updatedPipelineConfig.getSteps().getVariantCalling() == null) {
                        throw new ToolException("Clinical pipeline step '" + VARIANT_CALLING_PIPELINE_STEP + "' is not present in the"
                                + " pipeline configuration.");
                    }
                    validateVariantCallingTools(updatedPipelineConfig.getSteps().getVariantCalling().getTools());
                    break;
                }

                default: {
                    throw new ToolException("Clinical pipeline step '" + step + "' is not valid. Supported steps are: "
                            + String.join(", ", VALID_GENOMIC_PIPELINE_STEPS));
                }
            }
        }
    }

    private void validateAlignmentTool(GenomicsAlignmentPipelineTool tool) throws ToolException, CatalogException {
        validateTool(ALIGNMENT_PIPELINE_STEP, tool);

        // Check the index is provided, and update with the real path (from OpenCGA catalog)
        String index = tool.getIndex();
        if (StringUtils.isNotEmpty(index)) {
            logger.info("Checking alignment tool '{}' index path: {}", tool.getId(), index);
            File opencgaFile = getCatalogManager().getFileManager().get(study, index, QueryOptions.empty(), token).first();
            if (opencgaFile.getType() != File.Type.DIRECTORY) {
                throw new ToolException("Alignment tool '" + tool.getId() + "', index dir '" + index + "' is not a folder.");
            }

            // Update the index in the alignment tool
            Path path = Paths.get(opencgaFile.getUri()).toAbsolutePath();
            if (!Files.exists(path) || !Files.isDirectory(path)) {
                throw new ToolException("Alignemnt tool '" + tool.getId() + "', index path '" + path + "' does not exist or is not"
                        + " a folder.");
            }
            tool.setIndex(path.toString());
        }
    }

    private void validateVariantCallingTools(List<GenomicsVariantCallingPipelineTool> pipelineTools) throws ToolException, CatalogException {
        if (CollectionUtils.isEmpty(pipelineTools)) {
            throw new ToolException("Missing tools for clinical pipeline step: " + VARIANT_CALLING_PIPELINE_STEP);
        }
        for ( GenomicsVariantCallingPipelineTool tool : pipelineTools) {
            validateTool(VARIANT_CALLING_PIPELINE_STEP, tool);

            // Check the reference is provided, and update with the real path (from OpenCGA catalog)
            String reference = tool.getReference();
            if (StringUtils.isNotEmpty(reference)) {
                logger.info("Checking variant calling tool '{}' reference path: {}", tool.getId(), reference);
                File opencgaFile = getCatalogManager().getFileManager().get(study, reference, QueryOptions.empty(), token).first();
                if (opencgaFile.getType() != File.Type.DIRECTORY) {
                    throw new ToolException("Variant calling tool '" + tool.getId() + "', reference dir '" + reference
                            + "' is not a folder.");
                }

                // Update the reference in the variant calling tool
                Path path = Paths.get(opencgaFile.getUri()).toAbsolutePath();
                if (!Files.exists(path) || !Files.isDirectory(path)) {
                    throw new ToolException("Variant calling tool '" + tool.getId() + "', reference path '" + reference
                            + "' does not exist or is not a folder.");
                }
                tool.setReference(path.toString());
            }

            // Add tool ID to the list of variant calling tool IDs to be used later
            variantCallingToolIds.add(tool.getId());
        }
    }
}
