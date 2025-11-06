package org.opencb.opencga.analysis.wrappers.clinicalpipeline.affy;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.pipeline.affy.AffyClinicalPipelineWrapperParams;
import org.opencb.opencga.core.models.clinical.pipeline.affy.AffyPipelineConfig;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.opencb.opencga.analysis.wrappers.clinicalpipeline.ClinicalPipelineUtils.*;
import static org.opencb.opencga.catalog.utils.ResourceManager.ANALYSIS_DIRNAME;

@Tool(id = AffyClinicalPipelineWrapperAnalysis.ID, resource = Enums.Resource.VARIANT,
        description = AffyClinicalPipelineWrapperAnalysis.DESCRIPTION)
public class AffyClinicalPipelineWrapperAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "affy-pipeline";
    public static final String DESCRIPTION = "Execute the affy genomics pipeline that performs QC (apt-geno-qc-axiom),"
            + " genotype (apt-genotype-axiom) and variant indexing in OpenCGA storage.";

    private static final String AFFY_PIPELINE_STEP = "affy-pipeline";
    private static final String VARIANT_INDEX_STEP = "variant-index";

    private List<String> pipelineSteps;
    private AffyPipelineConfig updatedPipelineConfig;

    @ToolParams
    protected final AffyClinicalPipelineWrapperParams analysisParams = new AffyClinicalPipelineWrapperParams();

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        // Check pipeline configuration
        updatedPipelineConfig = checkPipelineConfig(analysisParams.getPipelineParams().getPipelineFile(),
                analysisParams.getPipelineParams().getPipeline(), catalogManager, study, token);

        // Update from params: samples, data dir and index dir
        updatePipelineConfigFromParams(updatedPipelineConfig, analysisParams.getPipelineParams().getSamples(),
                analysisParams.getPipelineParams().getDataDir(), analysisParams.getPipelineParams().getIndexDir());

        // Update physical paths
        updatePipelineConfigWithPhysicalPaths(updatedPipelineConfig, study, catalogManager, token);

        // Check pipeline steps
        checkPipelineSteps();
    }

    @Override
    protected List<String> getSteps() {
        if (analysisParams.getPipelineParams().getVariantIndexParams() != null) {
            return Arrays.asList(AFFY_PIPELINE_STEP, VARIANT_INDEX_STEP);
        } else {
            return Collections.singletonList(AFFY_PIPELINE_STEP);
        }
    }

    protected void run() throws ToolException, IOException {
        // Execute the pipeline
        step(AFFY_PIPELINE_STEP, this::runAffyPipeline);

        if (analysisParams.getPipelineParams().getVariantIndexParams() != null) {
            // Index variants in OpenCGA storage
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
                .setPipelineSteps(pipelineSteps)
                .execute();
    }


    private void runVariantIndex() throws CatalogException, StorageEngineException, ToolException, IOException {
        // Find the .sorted.<variant calling tool ID>.vcf.gz file within the genotype/variant-calling folder
        Path vcfPath;
        String genotypeToolId = updatedPipelineConfig.getSteps().getGenotype().getTool().getId();
        try (Stream<Path> stream = Files.list(getOutDir())) {
            vcfPath = stream
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".vcf.gz"))
                    .findFirst()
                    .orElse(null);
        }

        if (vcfPath == null || !Files.exists(vcfPath)) {
            throw new ToolException("Could not find the generated VCF: " + vcfPath + " from genotype tool " + genotypeToolId);
        }
        File vcfFile = catalogManager.getFileManager().link(study, new FileLinkParams(vcfPath.toAbsolutePath().toString(),
                "", "", "", null, null, null, null, null), false, token).first();

        ObjectMap storageOptions = analysisParams.getPipelineParams().getVariantIndexParams() != null
                // ? new ObjectMap(analysisParams.getPipelineParams().getVariantIndexParams())
                ? analysisParams.getPipelineParams().getVariantIndexParams().toObjectMap()
                : new ObjectMap();

        getVariantStorageManager().index(study, vcfFile.getId(), getScratchDir().toAbsolutePath().toString(), storageOptions, token);
    }

    private void checkPipelineSteps() throws ToolException, CatalogException {
        // Ensure pipeline config has steps
        if (updatedPipelineConfig.getSteps() == null || (updatedPipelineConfig.getSteps().getQualityControl() == null
                && updatedPipelineConfig.getSteps().getGenotype() == null)) {
            throw new ToolException("All clinical pipeline configuration steps are missing.");
        }

        pipelineSteps = analysisParams.getPipelineParams().getSteps();
        // If no steps are provided, set all the steps by default
        if (CollectionUtils.isEmpty(pipelineSteps)) {
            pipelineSteps = Arrays.asList(QUALITY_CONTROL_PIPELINE_STEP, GENOTYPE_PIPELINE_STEP);
        }

        // Validate each step exists in config and has required tools
        for (String step : pipelineSteps) {
            if (!VALID_AFFY_PIPELINE_STEPS.contains(step)) {
                throw new ToolException("Affy clinical pipeline step '" + step + "' is not valid. Supported steps are: "
                        + String.join(", ", VALID_AFFY_PIPELINE_STEPS));
            }

            switch (step) {
                case QUALITY_CONTROL_PIPELINE_STEP: {
                    if (updatedPipelineConfig.getSteps().getQualityControl() == null) {
                        throw new ToolException("Affy clinical pipeline step '" + QUALITY_CONTROL_PIPELINE_STEP + "' is not present in the"
                                + " pipeline configuration.");
                    }
                    validateTool(QUALITY_CONTROL_PIPELINE_STEP, updatedPipelineConfig.getSteps().getQualityControl().getTool());
                    break;
                }

                case GENOTYPE_PIPELINE_STEP: {
                    if (updatedPipelineConfig.getSteps().getGenotype() == null) {
                        throw new ToolException("Affy clinical pipeline step '" + GENOTYPE_PIPELINE_STEP + "' is not present in the"
                                + " pipeline configuration.");
                    }
                    validateTool(GENOTYPE_PIPELINE_STEP, updatedPipelineConfig.getSteps().getGenotype().getTool());
                    break;
                }

                default: {
                    throw new ToolException("Clinical pipeline step '" + step + "' is not valid. Supported steps are: "
                            + String.join(", ", VALID_AFFY_PIPELINE_STEPS));
                }
            }
        }
    }
}
