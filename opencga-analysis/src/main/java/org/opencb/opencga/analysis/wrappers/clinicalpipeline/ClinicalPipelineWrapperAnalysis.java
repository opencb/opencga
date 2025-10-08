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
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.pipeline.ClinicalPipelineExecuteParams;
import org.opencb.opencga.core.models.clinical.pipeline.ClinicalPipelinePrepareParams;
import org.opencb.opencga.core.models.clinical.pipeline.ClinicalPipelineWrapperParams;
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
            ClinicalPipelinePrepareParams updatedPrepareParams = new ClinicalPipelinePrepareParams();
            if (StringUtils.isEmpty(analysisParams.getPrepareParams().getReferenceGenome())) {
                throw new ToolException("Missing reference genome to prepare the clinical pipeline.");
            }

            String referenceGenome = analysisParams.getPrepareParams().getReferenceGenome();
            if (referenceGenome.startsWith("http://") || referenceGenome.startsWith("https://") || referenceGenome.startsWith("ftp://")) {
                updatedPrepareParams.setReferenceGenome(referenceGenome);
            } else {
                logger.info("Checking reference genome file {}", referenceGenome);
                File opencgaFile = getCatalogManager().getFileManager().get(study, referenceGenome, QueryOptions.empty(), token).first();
                updatedPrepareParams.setReferenceGenome(Paths.get(opencgaFile.getUri()).toAbsolutePath().toString());
            }

            // Add the aligner indexes if provided
            if (CollectionUtils.isNotEmpty(analysisParams.getPrepareParams().getAlignerIndexes())) {
                updatedPrepareParams.setAlignerIndexes(analysisParams.getPrepareParams().getAlignerIndexes());
            }

            // Update the prepare params
            updatedParams.setPrepareParams(updatedPrepareParams);
        } else {
            // Check execute pipeline parameters
            ClinicalPipelineExecuteParams updatedExecuteParams = new ClinicalPipelineExecuteParams();
            if (CollectionUtils.isEmpty(analysisParams.getExecuteParams().getInput())) {
                throw new ToolException("Clinical pipeline input files are mandatory for running the pipeline.");
            }
            for (String file : analysisParams.getExecuteParams().getInput()) {
                logger.info("Checking input file {}", file);
                File opencgaFile = getCatalogManager().getFileManager().get(study, file, QueryOptions.empty(), token).first();
                if (opencgaFile.getType() != File.Type.FILE) {
                    throw new ToolException("Clinical pipeline input path '" + file + "' is not a file.");
                }
                updatedExecuteParams.getInput().add(Paths.get(opencgaFile.getUri()).toAbsolutePath().toString());
            }

            // Check the index path
            if (StringUtils.isEmpty(analysisParams.getExecuteParams().getIndexDir())) {
                throw new ToolException("Clinical pipeline index path is mandatory for running the pipeline.");
            }
            logger.info("Checking index path {}", analysisParams.getExecuteParams().getIndexDir());
            File opencgaFile = getCatalogManager().getFileManager().get(study, analysisParams.getExecuteParams().getIndexDir(),
                    QueryOptions.empty(), token).first();
            if (opencgaFile.getType() != File.Type.DIRECTORY) {
                throw new ToolException("Clinical pipeline index path '" + analysisParams.getExecuteParams().getIndexDir()
                        + "' is not a folder.");
            }
            updatedExecuteParams.setIndexDir(Paths.get(opencgaFile.getUri()).toAbsolutePath().toString());

            // Check pipeline parameters
            if (StringUtils.isEmpty(analysisParams.getExecuteParams().getPipelineFile())
                    && MapUtils.isEmpty(analysisParams.getExecuteParams().getPipeline())) {
                throw new ToolException("Missing clinical pipeline definition. You can either provide a pipeline JSON file or a JSON"
                        + " object.");
            }
            if (StringUtils.isNotEmpty(analysisParams.getExecuteParams().getPipelineFile())
                    && MapUtils.isNotEmpty(analysisParams.getExecuteParams().getPipeline())) {
                throw new ToolException("Ambiguous clinical pipeline definition. You can either provide a pipeline JSON file or a JSON"
                        + " object but not both.");
            }
            // If a pipeline file is provided, check the file exists and then read the file content to add it to the params
            if (StringUtils.isNotEmpty(analysisParams.getExecuteParams().getPipelineFile())) {
                String pipelineFile = analysisParams.getExecuteParams().getPipelineFile();
                logger.info("Checking clinical pipeline definition file {}", pipelineFile);
                opencgaFile = getCatalogManager().getFileManager().get(study, pipelineFile, QueryOptions.empty(), token).first();
                if (opencgaFile.getType() != File.Type.FILE) {
                    throw new ToolException("Clinical pipeline definition path '" + pipelineFile + "' is not a file.");
                }
                Path pipelinePath = Paths.get(opencgaFile.getUri()).toAbsolutePath();
                updatedExecuteParams.setPipeline(JacksonUtils.getDefaultObjectMapper().readerFor(ObjectMap.class)
                        .readValue(pipelinePath.toFile()));
            } else {
                updatedExecuteParams.setPipeline(analysisParams.getExecuteParams().getPipeline());
            }

            // Check steps
            if (CollectionUtils.isNotEmpty(analysisParams.getExecuteParams().getSteps())) {
                for (String step : analysisParams.getExecuteParams().getSteps()) {
                    if (!VALID_PIPELINE_STEPS.contains(step)) {
                        throw new ToolException("Clinical pipeline step '" + step + "' is not valid. Supported steps are: "
                                + String.join(", ", VALID_PIPELINE_STEPS));
                    }
                }
                updatedExecuteParams.setSteps(analysisParams.getExecuteParams().getSteps());
            } else {
                updatedExecuteParams.setSteps(Arrays.asList(QUALITY_CONTROL_STEP, ALIGNMENT_STEP, VARIANT_CALLING_STEP));
            }

            // Update the execute params
            updatedParams.setExecuteParams(updatedExecuteParams);
        }
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
