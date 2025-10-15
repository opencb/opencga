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
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.opencb.opencga.analysis.wrappers.clinicalpipeline.ClinicalPipelineWrapperAnalysisExecutor.*;
import static org.opencb.opencga.catalog.utils.ResourceManager.ANALYSIS_DIRNAME;

@Tool(id = ClinicalPipelineWrapperAnalysis.ID, resource = Enums.Resource.VARIANT, description = ClinicalPipelineWrapperAnalysis.DESCRIPTION)
public class ClinicalPipelineWrapperAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "ngs-pipeline";
    public static final String DESCRIPTION = "Clinical pipeline that performs QC (e.g.: FastQC), mapping (e.g.: BWA), variant calling"
            + " (e.g., GATK) and variant indexing in OpenCGA storage.";
    public static final String PREPARE_DESCR = "Prepare the clinical pipeline that performs QC (e.g.: FastQC), mapping (e.g.: BWA),"
            + " variant calling (e.g., GATK) and variant indexing in OpenCGA storage.";
    public static final String EXECUTE_DESCR = "Execute the clinical pipeline that performs QC (e.g.: FastQC), mapping (e.g.: BWA),"
            + " variant calling (e.g., GATK) and variant indexing in OpenCGA storage.";

    public static final String SAMPLE_SEP = ";";
    public static final String SAMPLE_FIELD_SEP = "::";
    public static final String SAMPLE_FILE_SEP = ",";

    private static final String PREPARE_PIPELINE_STEP = "prepare-pipeline";
    private static final String EXECUTE_PIPELINE_STEP = "execute-pipeline";
    private static final String INDEX_VARIANTS_STEP = "index-variants";

    ClinicalPipelineWrapperParams updatedParams = new ClinicalPipelineWrapperParams(null, null, null);

    @ToolParams
    protected final ClinicalPipelineWrapperParams analysisParams = new ClinicalPipelineWrapperParams();

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        // Check command
        if (analysisParams.getExecuteParams() == null && analysisParams.getPrepareParams() == null) {
            throw new ToolException("Missing clinical pipeline parameters to prepare or execute the pipeline.");
        }

        if (analysisParams.getPrepareParams() != null) {
            // Check prepare pipeline parameters
            ClinicalPipelinePrepareParams prepareParams = analysisParams.getPrepareParams();
            ClinicalPipelinePrepareParams updatedPrepareParams = new ClinicalPipelinePrepareParams();

            // Check reference genome
            String referenceGenome = prepareParams.getReferenceGenome();
            if (StringUtils.isEmpty(referenceGenome)) {
                throw new ToolException("Missing reference genome to prepare the clinical pipeline.");
            }
            if (!ClinicalPipelineWrapperAnalysisExecutor.isURL(referenceGenome)) {
                logger.info("Checking reference genome file {}", referenceGenome);
                File opencgaFile = getCatalogManager().getFileManager().get(study, referenceGenome, QueryOptions.empty(), token).first();
                referenceGenome = Paths.get(opencgaFile.getUri()).toAbsolutePath().toString();
            }
            updatedPrepareParams.setReferenceGenome(referenceGenome);

            // Add the aligner indexes if provided
            if (CollectionUtils.isNotEmpty(prepareParams.getAlignerIndexes())) {
                updatedPrepareParams.setAlignerIndexes(prepareParams.getAlignerIndexes());
            }

            // Update the prepare params
            updatedParams.setPrepareParams(updatedPrepareParams);
        } else {
            // Check execute pipeline parameters
            ClinicalPipelineExecuteParams executeParams = analysisParams.getExecuteParams();
            ClinicalPipelineExecuteParams updatedExecuteParams = new ClinicalPipelineExecuteParams();

            // Check pipeline configuration, if a pipeline file is provided, check the file exists and then read the file content
            // to add it to the params
            PipelineConfig pipelineConfig;
            if (StringUtils.isNotEmpty(executeParams.getPipelineFile())) {
                String pipelineFile = executeParams.getPipelineFile();
                logger.info("Checking clinical pipeline configuration file {}", pipelineFile);
                File opencgaFile = getCatalogManager().getFileManager().get(study, pipelineFile, QueryOptions.empty(), token).first();
                if (opencgaFile.getType() != File.Type.FILE) {
                    throw new ToolException("Clinical pipeline definition path '" + pipelineFile + "' is not a file.");
                }
                Path pipelinePath = Paths.get(opencgaFile.getUri()).toAbsolutePath();
                pipelineConfig = JacksonUtils.getDefaultObjectMapper().readerFor(PipelineConfig.class).readValue(pipelinePath.toFile());
            } else if (executeParams.getPipeline() != null) {
                logger.info("Checking clinical pipeline configuration provided directly in the parameters");
                pipelineConfig = executeParams.getPipeline();
            } else {
                throw new ToolException("Missing clinical pipeline configuration. You can either provide a pipeline configuration JSON"
                        + " file or directly the pipeline configuration.");
            }
            if (pipelineConfig == null) {
                throw new ToolException("After checking, pipeline configuration is null.");
            }

            // Check samples, and update the input files to point to the real paths in both cases, from the input samples or from
            // the execute params
            List<String> updatedSamples = new ArrayList<>();
            List<String> samples = executeParams.getSamples();
            if (CollectionUtils.isEmpty(samples)) {
                if (pipelineConfig.getInput() != null && CollectionUtils.isNotEmpty(pipelineConfig.getInput().getSamples())) {
                    for (PipelineSample pipelineSample : pipelineConfig.getInput().getSamples()) {
                        if (StringUtils.isEmpty(pipelineSample.getId())) {
                            throw new ToolException("Missing sample ID for one of the input samples");
                        }

                        // Create updated sample
                        StringBuilder updatedSample = new StringBuilder();
                        updatedSample.append(pipelineSample.getId()).append(SAMPLE_FIELD_SEP);

                        if (CollectionUtils.isNotEmpty(pipelineSample.getFiles())) {
                            for (String file : pipelineSample.getFiles()) {
                                File opencgaFile = getCatalogManager().getFileManager().get(study, file, QueryOptions.empty(), token)
                                        .first();
                                if (opencgaFile.getType() != File.Type.FILE) {
                                    throw new ToolException("Input path '" + file + "' for sample ID '" + pipelineSample.getId()
                                            + "' is not a file.");
                                } else {
                                    // Check if it is not the first file to add the separator
                                    if (!updatedSample.toString().endsWith(SAMPLE_FIELD_SEP)) {
                                        updatedSample.append(SAMPLE_FILE_SEP);
                                    }
                                    updatedSample.append(Paths.get(opencgaFile.getUri()).toAbsolutePath());
                                }
                            }
                            // Add somatic field
                            if (pipelineSample.isSomatic()) {
                                updatedSample.append(SAMPLE_FIELD_SEP).append("somatic");
                            } else {
                                updatedSample.append(SAMPLE_FIELD_SEP).append("germline");
                            }
                            // Add role field
                            if (StringUtils.isNotEmpty(pipelineSample.getRole())) {
                                updatedSample.append(SAMPLE_FIELD_SEP).append(pipelineSample.getRole());
                            } else {
                                updatedSample.append(SAMPLE_FIELD_SEP).append("unknown");
                            }

                            // Add the updated sample to the list of samples
                            updatedSamples.add(updatedSample.toString());
                        } else {
                            throw new ToolException("Missing input files for sample ID '" + pipelineSample.getId() + "'");
                        }
                    }
                } else {
                    throw new ToolException("Clinical pipeline input files are mandatory for running the pipeline.");
                }
            } else {
                for (String sample : samples) {
                    logger.info("Checking samples {}", sample);
                    // Parse the input format: sample_id::file_id1[,file_id2][::somatic::role]
                    String[] fields = sample.split(SAMPLE_FIELD_SEP);
                    if (fields.length < 2) {
                        throw new ToolException("Invalid input format. Expected format: sample_id" + SAMPLE_FIELD_SEP + "file_id1["
                                + SAMPLE_FILE_SEP + "file_id2][" + SAMPLE_FIELD_SEP + "somatic" + SAMPLE_FIELD_SEP + "role],"
                                + " but got: " + sample);
                    }
                    String inputFiles = fields[1];
                    if (StringUtils.isEmpty(inputFiles)) {
                        throw new ToolException("Missing input files for sample '" + fields[0] + "': " + sample);
                    }
                    String[] inputFile = inputFiles.split(SAMPLE_FILE_SEP);
                    StringBuilder updatedSample = new StringBuilder();
                    updatedSample.append(fields[0]).append(SAMPLE_FIELD_SEP);
                    for (int i = 0; i < inputFile.length; i++) {
                        File opencgaFile = getCatalogManager().getFileManager().get(study, inputFile[i], QueryOptions.empty(), token)
                                .first();
                        if (opencgaFile.getType() != File.Type.FILE) {
                            throw new ToolException("Clinical pipeline input path '" + inputFile[i] + "' is not a file.");
                        }
                        if (i > 0) {
                            updatedSample.append(SAMPLE_FILE_SEP);
                        }
                        updatedSample.append(Paths.get(opencgaFile.getUri()).toAbsolutePath());
                    }
                    for (int i = 2; i < fields.length; i++) {
                        updatedSample.append(SAMPLE_FIELD_SEP).append(fields[i]);
                    }

                    logger.info("Updated sample {}", updatedSample);
                    updatedSamples.add(updatedSample.toString());
                }
            }
            // Set updated samples
            updatedExecuteParams.setSamples(updatedSamples);

            // Check the index path
            String indexDir = executeParams.getIndexDir();
            if (StringUtils.isEmpty(indexDir)) {
                if (CollectionUtils.isNotEmpty(pipelineConfig.getSteps())) {
                    for (PipelineStep pipelineStep : executeParams.getPipeline().getSteps()) {
                        if (pipelineStep.getTool() != null) {
                            checkToolIndexDir(pipelineStep.getTool());
                        }
                        if (CollectionUtils.isNotEmpty(pipelineStep.getTools())) {
                            for (PipelineTool tool : pipelineStep.getTools()) {
                                checkToolIndexDir(tool);
                            }
                        }
                    }
                }
            } else {
                logger.info("Checking index path {}", executeParams.getIndexDir());
                File opencgaFile = getCatalogManager().getFileManager().get(study, executeParams.getIndexDir(), QueryOptions.empty(),
                        token).first();
                if (opencgaFile.getType() != File.Type.DIRECTORY) {
                    throw new ToolException("Clinical pipeline index path '" + executeParams.getIndexDir() + "' is not a folder.");
                }
                updatedExecuteParams.setIndexDir(Paths.get(opencgaFile.getUri()).toAbsolutePath().toString());
            }

            // Check steps
            if (CollectionUtils.isNotEmpty(executeParams.getSteps())) {
                for (String step : executeParams.getSteps()) {
                    if (!VALID_PIPELINE_STEPS.contains(step)) {
                        throw new ToolException("Clinical pipeline step '" + step + "' is not valid. Supported steps are: "
                                + String.join(", ", VALID_PIPELINE_STEPS));
                    }
                }
                updatedExecuteParams.setSteps(executeParams.getSteps());
            } else {
                updatedExecuteParams.setSteps(Arrays.asList(QUALITY_CONTROL_STEP, ALIGNMENT_STEP, VARIANT_CALLING_STEP));
            }

            // Set pipeline configuration
            updatedExecuteParams.setPipeline(pipelineConfig);

            // Update the execute params
            updatedParams.setExecuteParams(updatedExecuteParams);
        }
    }

    private void checkToolIndexDir(PipelineTool pipelineTool) throws CatalogException {
        logger.info("Checking index path {} for tool {}", pipelineTool.getIndex(), pipelineTool.getId());
        File opencgaFile = getCatalogManager().getFileManager().get(study, pipelineTool.getIndex(), QueryOptions.empty(), token).first();
        pipelineTool.setIndex(Paths.get(opencgaFile.getUri()).toAbsolutePath().toString());
    }

    @Override
    protected List<String> getSteps() {
        if (analysisParams.getPrepareParams() != null) {
            return Collections.singletonList(PREPARE_PIPELINE_STEP);
        } else {
            return Arrays.asList(EXECUTE_PIPELINE_STEP, INDEX_VARIANTS_STEP);
        }
    }

    protected void run() throws ToolException, IOException {
        if (analysisParams.getPrepareParams() != null) {
            // Prepare the pipeline
            step(PREPARE_PIPELINE_STEP, this::runPipelineExecutor);
        } else {
            // Execute the pipeline
            step(EXECUTE_PIPELINE_STEP, this::runPipelineExecutor);

            // Index variants in OpenCGA
            step(INDEX_VARIANTS_STEP, this::indexVariants);
        }
    }

    protected void runPipelineExecutor() throws  ToolException {
        // Get executor
        ClinicalPipelineWrapperAnalysisExecutor executor = getToolExecutor(ClinicalPipelineWrapperAnalysisExecutor.class);

        // Set parameters and execute (depending on the updated params, it will prepare or execute the pipeline)
        executor.setStudy(study)
                .setScriptPath(getOpencgaHome().resolve(ANALYSIS_DIRNAME).resolve(ID))
                .setPipelineParams(updatedParams)
                .execute();

        // TODO: check output?
    }

    protected void indexVariants() throws CatalogException, StorageEngineException, ToolException, IOException {
        // Find the .sorted.gatk.vcf file within the variant-calling folder
        Path vcfPath;
        try (Stream<Path> stream = Files.list(getOutDir().resolve("variant-calling"))) {
            vcfPath = stream
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".sorted.gatk.vcf"))
                    .findFirst()
                    .orElse(null);
        }

        if (vcfPath == null || !Files.exists(vcfPath)) {
            throw new ToolException("Could not find the generated VCF: " + vcfPath);
        }
        File vcfFile = catalogManager.getFileManager().link(study, new FileLinkParams(vcfPath.toAbsolutePath().toString(),
                "", "", "", null, null, null, null, null), false, token).first();

        ObjectMap storageOptions = new ObjectMap()
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);

        getVariantStorageManager().index(study, vcfFile.getId(), getScratchDir().toAbsolutePath().toString(), storageOptions, token);
    }
}
