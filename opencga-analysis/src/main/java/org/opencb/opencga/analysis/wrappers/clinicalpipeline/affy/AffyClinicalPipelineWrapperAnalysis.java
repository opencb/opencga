package org.opencb.opencga.analysis.wrappers.clinicalpipeline.affy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.pipeline.affy.AffyClinicalPipelineParams;
import org.opencb.opencga.core.models.clinical.pipeline.affy.AffyClinicalPipelineWrapperParams;
import org.opencb.opencga.core.models.clinical.pipeline.affy.AffyPipelineConfig;
import org.opencb.opencga.core.models.clinical.pipeline.affy.AffyPipelineInput;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.opencb.opencga.analysis.wrappers.clinicalpipeline.ClinicalPipelineUtils.PIPELINE_ANALYSIS_DIRNAME;
import static org.opencb.opencga.analysis.wrappers.clinicalpipeline.ClinicalPipelineUtils.getPhysicalDirPath;
import static org.opencb.opencga.catalog.utils.ResourceManager.ANALYSIS_DIRNAME;

@Tool(id = AffyClinicalPipelineWrapperAnalysis.ID, resource = Enums.Resource.VARIANT,
        description = AffyClinicalPipelineWrapperAnalysis.DESCRIPTION)
public class AffyClinicalPipelineWrapperAnalysis extends OpenCgaTool {

    public static final String ID = "affy-pipeline";
    public static final String DESCRIPTION = "Execute the Affymetrix pipeline that performs QC (apt-geno-qc-axiom),"
            + " genotype (apt-genotype-axiom) and variant indexing in OpenCGA storage.";

    private static final String AFFY_PIPELINE_STEP = "affy-pipeline";
    private static final String VARIANT_INDEX_STEP = "variant-index";

    private AffyPipelineConfig updatedPipelineConfig;

    @ToolParams
    protected final AffyClinicalPipelineWrapperParams analysisParams = new AffyClinicalPipelineWrapperParams();

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        // Check pipeline configuration
        updatedPipelineConfig = checkPipelineConfig();

        // Update from params: samples, data dir and index dir
        updatePipelineConfigFromParams();
    }

    @Override
    protected List<String> getSteps() {
        if (analysisParams.getPipelineParams().getVariantIndexParams() != null) {
            return Arrays.asList(AFFY_PIPELINE_STEP, VARIANT_INDEX_STEP);
        } else {
            return Collections.singletonList(AFFY_PIPELINE_STEP);
        }
    }

    @Override
    protected void run() throws ToolException, IOException {
        // Execute the pipeline
        step(AFFY_PIPELINE_STEP, this::runAffyPipeline);

        // Index variants in OpenCGA storage
        if (analysisParams.getPipelineParams().getVariantIndexParams() != null) {
            step(VARIANT_INDEX_STEP, this::runVariantIndex);
        }
    }

    private void runAffyPipeline() throws  ToolException {
        // Get executor
        AffyClinicalPipelineWrapperAnalysisExecutor executor = getToolExecutor(AffyClinicalPipelineWrapperAnalysisExecutor.class);

        // Set parameters and execute
        executor.setStudy(study)
                .setScriptPath(getOpencgaHome().resolve(ANALYSIS_DIRNAME).resolve(PIPELINE_ANALYSIS_DIRNAME))
                .setPipelineConfig(updatedPipelineConfig)
                .execute();
    }


    private void runVariantIndex() throws CatalogException, StorageEngineException, ToolException, IOException {
        // Find the .sorted.<variant calling tool ID>.vcf.gz file within the genotype/variant-calling folder
        Path vcfPath;
        try (Stream<Path> stream = Files.list(getOutDir())) {
            vcfPath = stream
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".vcf") || path.getFileName().toString().endsWith(".vcf.gz"))
                    .findFirst()
                    .orElse(null);
        }

        if (vcfPath == null || !Files.exists(vcfPath)) {
            throw new ToolException("Could not find the generated VCF: " + vcfPath + " from Affymetrix pipeline execution.");
        }
        File vcfFile = catalogManager.getFileManager().link(study, new FileLinkParams(vcfPath.toAbsolutePath().toString(),
                "", "", "", null, null, null, null, null), false, token).first();

        VariantIndexParams variantIndexParams = analysisParams.getPipelineParams().getVariantIndexParams();

        logger.info("Indexing variants generated by Affymetrix pipeline {} with options {}", vcfFile.getPath(), variantIndexParams);
        getVariantStorageManager().index(study, vcfFile.getId(), getScratchDir().toAbsolutePath().toString(),
                variantIndexParams.toObjectMap(), token);
    }

    private AffyPipelineConfig checkPipelineConfig() throws ToolException, CatalogException, IOException {
        // Check pipeline configuration, if a pipeline file is provided, check the file exists and then read the file content
        // to add it to the params
        AffyClinicalPipelineParams params = analysisParams.getPipelineParams();
        if (StringUtils.isEmpty(params.getPipelineFile()) && params.getPipeline() == null) {
            throw new ToolException("Missing Affymetrix pipeline configuration. You can either provide a pipeline configuration file or"
                    + " directly the pipeline configuration.");
        }

        // Get pipeline config
        if (!StringUtils.isEmpty(params.getPipelineFile())) {
            String pipelineFile = params.getPipelineFile();
            logger.info("Checking Affymetrix pipeline configuration file {}", pipelineFile);
            File opencgaFile = catalogManager.getFileManager().get(study, pipelineFile, QueryOptions.empty(), token).first();
            if (opencgaFile.getType() != File.Type.FILE) {
                throw new ToolException("Affymetrix pipeline configuration file '" + pipelineFile + "' is not a file.");
            }
            Path pipelinePath = Paths.get(opencgaFile.getUri()).toAbsolutePath();
            return JacksonUtils.getDefaultObjectMapper().readerFor(AffyPipelineConfig.class).readValue(pipelinePath.toFile());
        } else {
            logger.info("Getting Affymetrix pipeline configuration provided directly in the parameters");
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(mapper.writeValueAsString(params.getPipeline()), AffyPipelineConfig.class);
        }
    }

    private void updatePipelineConfigFromParams() throws ToolException, CatalogException {
        // Init input if not present
        if (updatedPipelineConfig.getInput() == null) {
            updatedPipelineConfig.setInput(new AffyPipelineInput());
        }

        // If chip is provided in the pipeline params, set it in the pipeline config to be processed later
        if (StringUtils.isNotEmpty(analysisParams.getPipelineParams().getChip())) {
            updatedPipelineConfig.getInput().setChip(analysisParams.getPipelineParams().getChip());
        }

        // If data dir is provided in the pipeline params, set it in the pipeline config to be processed later
        if (StringUtils.isNotEmpty(analysisParams.getPipelineParams().getDataDir())) {
            Path path = getPhysicalDirPath(analysisParams.getPipelineParams().getDataDir(), study, catalogManager, token);
            updatedPipelineConfig.getInput().setDataDir(path.toString());
        }

        // If index dir is provided in the pipeline params, set it in the pipeline config to be processed later
        if (StringUtils.isNotEmpty(analysisParams.getPipelineParams().getIndexDir())) {
            Path path = getPhysicalDirPath(analysisParams.getPipelineParams().getIndexDir(), study, catalogManager, token);
            updatedPipelineConfig.getInput().setIndexDir(path.toString());
        }
    }
}
