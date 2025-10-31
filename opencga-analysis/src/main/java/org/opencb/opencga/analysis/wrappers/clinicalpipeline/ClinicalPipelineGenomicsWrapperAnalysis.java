package org.opencb.opencga.analysis.wrappers.clinicalpipeline;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.pipeline.*;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.opencb.opencga.analysis.wrappers.clinicalpipeline.ClinicalPipelineUtils.*;
import static org.opencb.opencga.catalog.utils.ResourceManager.ANALYSIS_DIRNAME;

@Tool(id = ClinicalPipelineGenomicsWrapperAnalysis.ID, resource = Enums.Resource.VARIANT,
        description = ClinicalPipelineGenomicsWrapperAnalysis.DESCRIPTION)
public class ClinicalPipelineGenomicsWrapperAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "ngs-pipeline-genomics";
    public static final String DESCRIPTION = "Execute the clinical genomics pipeline that performs QC (e.g.: FastQC), mapping (e.g.: BWA),"
            + " variant calling (e.g., GATK) and variant indexing in OpenCGA storage.";

    private static final String GENOMICS_PIPELINE_STEP = "genomics-pipeline";
    private static final String VARIANT_INDEX_STEP = "variant-index";

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
        checkPipelineConfig();

        // If samples are provided in the pipeline params, set them in the pipeline config to be processed later
        if (CollectionUtils.isNotEmpty(analysisParams.getPipelineParams().getSamples())) {
            List<PipelineSample> pipelineSamples = new ArrayList<>();
            for (String sample : analysisParams.getPipelineParams().getSamples()) {
                pipelineSamples.add(createPipelineSampleFromString(sample));
            }
            updatedPipelineConfig.getInput().setSamples(pipelineSamples);
        }

        // If index dir is provided in the pipeline params, set it in the pipeline config to be processed later
        String indexDir = analysisParams.getPipelineParams().getIndexDir();
        if (StringUtils.isNotEmpty(indexDir)) {
            updatedPipelineConfig.getInput().setIndexDir(analysisParams.getPipelineParams().getIndexDir());
        }

        // Update sample files (by getting the real paths) in the pipeline configuration
        for (PipelineSample sample : updatedPipelineConfig.getInput().getSamples()) {
            List<String> updatedFiles = new ArrayList<>();
            for (String file : sample.getFiles()) {
                logger.info("Checking sample file {}", file);
                File opencgaFile = getCatalogManager().getFileManager().get(study, file, QueryOptions.empty(), token).first();
                if (opencgaFile.getType() != File.Type.FILE) {
                    throw new ToolException("Clinical pipeline sample file '" + file + "' for sample ID '" + sample.getId()
                            + "' is not a file.");
                }
                // Add the real path to the updated files
                updatedFiles.add(Paths.get(opencgaFile.getUri()).toAbsolutePath().toString());
            }
            // Set updated files in the sample
            sample.setFiles(updatedFiles);
        }

        // Update index dir (by getting the real path) in the pipeline configuration
        indexDir = updatedPipelineConfig.getInput().getIndexDir();
        if (StringUtils.isEmpty(indexDir)) {
            throw new ToolException("Missing clinical pipeline index directory. You can either provide an index directory in the"
                    + " pipeline configuration or in the execute parameters.");
        }
        logger.info("Checking index dir {}", indexDir);
        File opencgaFile = getCatalogManager().getFileManager().get(study, indexDir, QueryOptions.empty(), token).first();
        if (opencgaFile.getType() != File.Type.DIRECTORY) {
            throw new ToolException("Clinical pipeline index dir '" + indexDir + "' is not a folder.");
        }
        // Update the index dir in the pipeline config
        updatedPipelineConfig.getInput().setIndexDir(Paths.get(opencgaFile.getUri()).toAbsolutePath().toString());

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
        ClinicalPipelineGenomicsWrapperAnalysisExecutor executor = getToolExecutor(ClinicalPipelineGenomicsWrapperAnalysisExecutor.class);

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

            ObjectMap storageOptions = analysisParams.getPipelineParams().getVariantIndexParams() != null
                    ? analysisParams.getPipelineParams().getVariantIndexParams().toObjectMap()
                    : new ObjectMap();

            getVariantStorageManager().index(study, vcfFile.getId(), getScratchDir().toAbsolutePath().toString(), storageOptions, token);
        }
    }

    private void checkPipelineConfig() throws ToolException, CatalogException, IOException {
        // Check pipeline configuration, if a pipeline file is provided, check the file exists and then read the file content
        // to add it to the params
        if (StringUtils.isEmpty(analysisParams.getPipelineParams().getPipelineFile())
                && analysisParams.getPipelineParams().getPipeline() == null) {
            throw new ToolException("Missing clinical pipeline configuration. You can either provide a pipeline configuration JSON"
                    + " file or directly the pipeline configuration.");
        }

        // Get pipeline config
        String pipelineFile = analysisParams.getPipelineParams().getPipelineFile();
        if (!StringUtils.isEmpty(pipelineFile)) {
            logger.info("Checking clinical pipeline configuration file {}", pipelineFile);
            File opencgaFile = getCatalogManager().getFileManager().get(study, pipelineFile, QueryOptions.empty(), token).first();
            if (opencgaFile.getType() != File.Type.FILE) {
                throw new ToolException("Clinical pipeline configuration file '" + pipelineFile + "' is not a file.");
            }
            Path pipelinePath = Paths.get(opencgaFile.getUri()).toAbsolutePath();
            updatedPipelineConfig = JacksonUtils.getDefaultObjectMapper().readerFor(PipelineConfig.class).readValue(pipelinePath.toFile());
        } else {
            logger.info("Getting clinical pipeline configuration provided directly in the parameters");
            updatedPipelineConfig = copyPipelineConfig(analysisParams.getPipelineParams().getPipeline());
        }

        // Check mandatory parameters in pipeline config: input and steps
        if (updatedPipelineConfig.getInput() == null) {
            throw new ToolException("Missing clinical pipeline configuration input.");
        }
    }

    private PipelineSample createPipelineSampleFromString(String sampleString) throws ToolException {
        // Parse the input format: sample_id::file_id1[,file_id2][::somatic::role]
        String[] fields = sampleString.split(SAMPLE_FIELD_SEP);
        if (fields.length < 2) {
            throw new ToolException("Invalid input format. Expected format: sample_id" + SAMPLE_FIELD_SEP + "file_id1["
                    + SAMPLE_FILE_SEP + "file_id2][" + SAMPLE_FIELD_SEP + "somatic" + SAMPLE_FIELD_SEP + "role],"
                    + " but got: " + sampleString);
        }

        PipelineSample pipelineSample = new PipelineSample();

        // Set sample ID
        pipelineSample.setId(fields[0]);

        // Parse and set files
        String inputFiles = fields[1];
        if (StringUtils.isEmpty(inputFiles)) {
            throw new ToolException("Missing input files for sample '" + fields[0] + "': " + sampleString);
        }
        pipelineSample.setFiles(Arrays.asList(inputFiles.split(SAMPLE_FILE_SEP)));

        // Parse optional somatic field (default: false)
        if (fields.length > 2 && StringUtils.isNotEmpty(fields[2])) {
            pipelineSample.setSomatic("somatic".equalsIgnoreCase(fields[2]));
        } else {
            pipelineSample.setSomatic(false);
        }

        // Parse optional role field (default: null)
        if (fields.length > 3 && StringUtils.isNotEmpty(fields[3])) {
            pipelineSample.setRole(fields[3]);
        }

        return pipelineSample;
    }

    private void checkPipelineSteps() throws ToolException, CatalogException {
        // Ensure pipeline config has steps
        validatePipelineConfigSteps();

        // Validate each step exists in config and has required tools
        validateStepsInConfiguration();
    }

    private void validatePipelineConfigSteps() throws ToolException {
        if (updatedPipelineConfig.getSteps() == null || (updatedPipelineConfig.getSteps().getQualityControl() == null
                && updatedPipelineConfig.getSteps().getAlignment() == null
                && updatedPipelineConfig.getSteps().getVariantCalling() == null)) {
            throw new ToolException("All clinical pipeline configuration steps are missing.");
        }
    }

    private void validateStepsInConfiguration() throws ToolException, CatalogException {
        pipelineSteps = analysisParams.getPipelineParams().getSteps();
        // If no steps are provided, set all the steps by default
        if (CollectionUtils.isEmpty(pipelineSteps)) {
            pipelineSteps = Arrays.asList(QUALITY_CONTROL_PIPELINE_STEP, ALIGNMENT_PIPELINE_STEP, VARIANT_CALLING_PIPELINE_STEP);
        }

        // Validate provided steps in the
        for (String step : pipelineSteps) {
            if (!VALID_PIPELINE_STEPS.contains(step)) {
                throw new ToolException("Clinical pipeline step '" + step + "' is not valid. Supported steps are: "
                        + String.join(", ", VALID_PIPELINE_STEPS));
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
                            + String.join(", ", VALID_PIPELINE_STEPS));
                }
            }
        }
    }

    private void validateTool(String step, PipelineTool pipelineTool) throws ToolException {
        if (pipelineTool == null) {
            throw new ToolException("Missing tool for clinical pipeline step: " + step);
        }
        if (StringUtils.isEmpty(pipelineTool.getId())) {
            throw new ToolException("Missing tool ID for clinical pipeline step: " + step);
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
