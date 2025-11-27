package org.opencb.opencga.analysis.wrappers.clinicalpipeline.rnaseq;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.analysis.wrappers.clinicalpipeline.affy.AffyClinicalPipelineWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.pipeline.affy.AffyClinicalPipelineWrapperParams;
import org.opencb.opencga.core.models.clinical.pipeline.affy.AffyPipelineConfig;
import org.opencb.opencga.core.models.clinical.pipeline.rnaseq.RnaSeqPipelineConfig;
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

@Tool(id = RnaSeqClinicalPipelineWrapperAnalysis.ID, resource = Enums.Resource.VARIANT,
        description = RnaSeqClinicalPipelineWrapperAnalysis.DESCRIPTION)
public class RnaSeqClinicalPipelineWrapperAnalysis extends OpenCgaTool {

    public static final String ID = "rna-seq-pipeline";
    public static final String DESCRIPTION = "Execute the RNA-Seq pipeline that performs QC, alignment and quantification analyses.";

    // RNA-Seq pipeline
    private static final String GENOTYPE_PIPELINE_STEP = "genotype";
    private static final List<String> VALID_AFFY_PIPELINE_STEPS = Arrays.asList(QUALITY_CONTROL_PIPELINE_STEP, GENOTYPE_PIPELINE_STEP);

    private List<String> pipelineSteps;
    private RnaSeqPipelineConfig updatedPipelineConfig;

    @ToolParams
    protected final AffyClinicalPipelineWrapperParams analysisParams = new AffyClinicalPipelineWrapperParams();

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        // Check commons
        updatedPipelineConfig = checkCommons(analysisParams.getPipelineParams(), catalogManager, study, token);

        // Check pipeline steps
        checkPipelineSteps();
    }


    protected void run() throws ToolException, IOException {
        // Execute the pipeline
        step(ID, this::runRnaSeqPipeline);
    }

    private void runRnaSeqPipeline() throws  ToolException {
        // Get executor
        RnaSeqClinicalPipelineWrapperAnalysisExecutor executor = getToolExecutor(RnaSeqClinicalPipelineWrapperAnalysisExecutor.class);

        // Set parameters and execute
        executor.setStudy(study)
                .setScriptPath(getOpencgaHome().resolve(ANALYSIS_DIRNAME).resolve(PIPELINE_ANALYSIS_DIRNAME))
                .setPipelineConfig(updatedPipelineConfig)
                .setPipelineSteps(pipelineSteps)
                .execute();
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
