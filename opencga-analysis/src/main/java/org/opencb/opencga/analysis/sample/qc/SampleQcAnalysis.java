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

package org.opencb.opencga.analysis.sample.qc;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.qc.SampleQcVariantStats;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.individual.qc.IndividualQcUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.genomePlot.GenomePlotAnalysis;
import org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureAnalysis;
import org.opencb.opencga.analysis.variant.stats.SampleVariantStatsAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.variant.GenomePlotAnalysisParams;
import org.opencb.opencga.core.models.variant.MutationalSignatureAnalysisParams;
import org.opencb.opencga.core.models.variant.SampleQcAnalysisParams;
import org.opencb.opencga.core.models.variant.SampleVariantStatsAnalysisParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.core.tools.result.Status;
import org.opencb.opencga.storage.core.StorageEngineFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.core.models.study.StudyPermissions.Permissions.WRITE_SAMPLES;

@Tool(id = SampleQcAnalysis.ID, resource = Enums.Resource.SAMPLE, description = SampleQcAnalysis.DESCRIPTION)
public class SampleQcAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "sample-qc";
    public static final String DESCRIPTION = "Run quality control (QC) for a given sample. It includes variant stats, and if the sample " +
            "is somatic, mutational signature and genome plot are calculated.";

    private static final String SAMPLE_VARIANT_STATS_STEP = "sample-variant-stats";
    private static final String MUTATIONAL_SIGNATURE_STEP = "mutational-signature";
    private static final String GENOME_PLOT_STEP = "genome-plot";

    @ToolParams
    protected final SampleQcAnalysisParams sampleQcParams = new SampleQcAnalysisParams();

    private ToolRunner toolRunner;

    private boolean runVariantStats = true;
    private boolean runSignatureCatalogue = true;
    private boolean runSignatureFitting = true;
    private boolean runGenomePlot = true;

    @Override
    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study");
        }

        // Check permissions
        try {
            JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
            CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(study, jwtPayload);
            String organizationId = studyFqn.getOrganizationId();
            String userId = jwtPayload.getUserId(organizationId);

            long studyUid = catalogManager.getStudyManager().get(getStudy(), QueryOptions.empty(), token).first().getUid();
            catalogManager.getAuthorizationManager().checkStudyPermission(organizationId, studyUid, userId, WRITE_SAMPLES);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        // Sanity check
        if (StringUtils.isEmpty(sampleQcParams.getSample())) {
            throw new ToolException("Missing sample ID.");
        }

        Sample sample = IndividualQcUtils.getValidSampleById(getStudy(), sampleQcParams.getSample(), catalogManager, token);
        if (sample == null) {
            throw new ToolException("Sample '" + sampleQcParams.getSample() + "' not found.");
        }

        // Prepare flags
        String skip = null;
        if (StringUtils.isNotEmpty(sampleQcParams.getSkip())) {
            skip = sampleQcParams.getSkip().toLowerCase().replace(" ", "");
        }
        if (StringUtils.isNotEmpty(skip)) {
            Set<String> skipValues = new HashSet<>(Arrays.asList(skip.split(",")));
            if (skipValues.contains(SampleQcAnalysisParams.VARIANT_STATS_SKIP_VALUE)) {
                runVariantStats = false;
            }
            if (skipValues.contains(SampleQcAnalysisParams.SIGNATURE_SKIP_VALUE)
                    || skipValues.contains(SampleQcAnalysisParams.SIGNATURE_CATALOGUE_SKIP_VALUE)) {
                runSignatureCatalogue = false;
            }
            if (skipValues.contains(SampleQcAnalysisParams.SIGNATURE_SKIP_VALUE)
                    || skipValues.contains(SampleQcAnalysisParams.SIGNATURE_FITTING_SKIP_VALUE)) {
                runSignatureFitting = false;
            }
            if (skipValues.contains(SampleQcAnalysisParams.GENOME_PLOT_SKIP_VALUE)) {
                runGenomePlot = false;
            }
        }

        // Check variant stats
        if (runVariantStats) {
            final String OPENCGA_ALL = "ALL";
            if (OPENCGA_ALL.equals(sampleQcParams.getVsId())) {
                throw new ToolException("Invalid parameters: " + OPENCGA_ALL + " is a reserved word, you can not use as a variant stats ID");
            }

            if (StringUtils.isEmpty(sampleQcParams.getVsId()) && sampleQcParams.getVsQuery() != null
                    && !sampleQcParams.getVsQuery().toParams().isEmpty()) {
                throw new ToolException("Invalid parameters: if variant stats ID is empty, variant stats query must be empty");
            }
            if (StringUtils.isNotEmpty(sampleQcParams.getVsId())
                    && (sampleQcParams.getVsQuery() == null || sampleQcParams.getVsQuery().toParams().isEmpty())) {
                throw new ToolException("Invalid parameters: if you provide a variant stats ID, variant stats query can not be empty");
            }
            if (StringUtils.isEmpty(sampleQcParams.getVsId())) {
                sampleQcParams.setVsId(OPENCGA_ALL);
            }

            if (sampleQcParams.getVsQuery() == null) {
                throw new ToolException("Invalid parameters: variant stats query is empty");
            }
            if (sample.getQualityControl() != null && sample.getQualityControl().getVariant() != null) {
                if (CollectionUtils.isNotEmpty(sample.getQualityControl().getVariant().getVariantStats())
                        && OPENCGA_ALL.equals(sampleQcParams.getVsId())) {
                    runVariantStats = false;
                } else {
                    for (SampleQcVariantStats variantStats : sample.getQualityControl().getVariant().getVariantStats()) {
                        if (variantStats.getId().equals(sampleQcParams.getVsId())) {
                            throw new ToolException("Invalid parameters: variant stats ID '" + sampleQcParams.getVsId()
                                    + "' is already used");
                        }
                    }
                }
            }
        } else {
            String msg = "Skipping sample variant stats analysis by user";
            addWarning(msg);
            logger.warn(msg);
        }

        // Check mutational signature
        if (runSignatureCatalogue) {
            if (!sample.isSomatic()) {
                String msg = "Skipping mutational signature catalog analysis:" + getSampleIsNotSomaticMsg(sample.getId());
                addWarning(msg);
                logger.warn(msg);
                runSignatureCatalogue = false;
            } else if (StringUtils.isEmpty(sampleQcParams.getMsQuery())) {
                    throw new ToolException("Invalid parameters: mutational signature query is empty");
            }
        } else {
            String msg = "Skipping mutational signature catalogue analysis by user";
            addWarning(msg);
            logger.warn(msg);
        }

        if (runSignatureFitting) {
            if (!sample.isSomatic()) {
                String msg = "Skipping mutational signature fitting analysis:" + getSampleIsNotSomaticMsg(sample.getId());
                addWarning(msg);
                logger.warn(msg);
                runSignatureFitting = false;
            }
        } else {
            String msg = "Skipping mutational signature fitting analysis by user";
            addWarning(msg);
            logger.warn(msg);
        }

        // Check genome plot
        if (runGenomePlot) {
            if (!sample.isSomatic()) {
                String msg = "Skipping genome plot: " + getSampleIsNotSomaticMsg(sample.getId());
                addWarning(msg);
                logger.warn(msg);
                runGenomePlot = false;
            } else {
                if (StringUtils.isEmpty(sampleQcParams.getGpConfigFile())) {
                    throw new ToolException("Invalid parameters: genome plot configuration file is empty");
                }

                File genomePlotConfFile = AnalysisUtils.getCatalogFile(sampleQcParams.getGpConfigFile(), getStudy(),
                        catalogManager.getFileManager(), getToken());
                Path genomePlotConfigPath = Paths.get(genomePlotConfFile.getUri().getPath());
                if (!genomePlotConfigPath.toFile().exists()) {
                    throw new ToolException("Invalid parameters: genome plot configuration file does not exist (" + genomePlotConfigPath
                            + ")");
                }
            }
        } else {
            String msg = "Skipping genome plot analysis by user";
            addWarning(msg);
            logger.warn(msg);
        }
    }

    @Override
    protected List<String> getSteps() {
        List<String> steps = new ArrayList<>();
        if (runVariantStats) {
            steps.add(SAMPLE_VARIANT_STATS_STEP);
        }
        if (runSignatureCatalogue || runSignatureFitting) {
            steps.add(MUTATIONAL_SIGNATURE_STEP);
        }
        if (runGenomePlot) {
            steps.add(GENOME_PLOT_STEP);
        }
        return steps;
    }

    @Override
    protected void run() throws ToolException {
        // Create the tool runner
        toolRunner = new ToolRunner(getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(variantStorageManager.getStorageConfiguration()));

        // Sample variant stats
        if (runVariantStats) {
            step(SAMPLE_VARIANT_STATS_STEP, this::runSampleVariantStats);
        }

        // Mutational signature
        if (runSignatureCatalogue || runSignatureFitting) {
            step(MUTATIONAL_SIGNATURE_STEP, this::runMutationalSignature);
        }

        // Genome plot
        if (runGenomePlot) {
            step(GENOME_PLOT_STEP, this::runGenomePlot);
        }
    }

    private void runSampleVariantStats() throws ToolException {
        // Create out folder
        Path outPath = getOutDir().resolve(SAMPLE_VARIANT_STATS_STEP);
        try {
            FileUtils.forceMkdir(outPath.toFile());
        } catch (IOException e) {
            throw new ToolException("Error creating sample variant stats folder: " + outPath, e);
        }

        // Prepare parameters
        SampleVariantStatsAnalysisParams sampleVariantStatsParams = new SampleVariantStatsAnalysisParams(
                Collections.singletonList(sampleQcParams.getSample()), null, null, true, false, sampleQcParams.getVsId(),
                sampleQcParams.getVsDescription(), null, sampleQcParams.getVsQuery());

        // Execute the sample variant stats analysis and add its step attributes if exist
        ExecutionResult executionResult = toolRunner.execute(SampleVariantStatsAnalysis.class, study, sampleVariantStatsParams, outPath,
                null, token);
        addStepAttribute(STEP_EXECUTION_RESULT_ATTRIBUTE_KEY, executionResult);

        // Check execution status
        if (executionResult.getStatus().getName() != Status.Type.DONE) {
            throw new ToolException("Something wrong happened running the sample variant stats analysis. Execution status = "
                    + executionResult.getStatus().getName());
        }
    }

    private void runMutationalSignature() throws ToolException {
        // Create the output folder
        Path outPath = getOutDir().resolve(MUTATIONAL_SIGNATURE_STEP);
        try {
            FileUtils.forceMkdir(outPath.toFile());
        } catch (IOException e) {
            throw new ToolException("Error creating mutational signature folder: " + outPath, e);
        }

        // Prepare parameters
        String skip = null;
        if (!runSignatureCatalogue) {
            skip = MutationalSignatureAnalysisParams.SIGNATURE_CATALOGUE_SKIP_VALUE;
        } else if (!runSignatureFitting) {
            skip = MutationalSignatureAnalysisParams.SIGNATURE_FITTING_SKIP_VALUE;
        }

        MutationalSignatureAnalysisParams mutationalSignatureParams = new MutationalSignatureAnalysisParams()
                .setId(sampleQcParams.getMsId())
                .setDescription(sampleQcParams.getMsDescription())
                .setSample(sampleQcParams.getSample())
                .setQuery(sampleQcParams.getMsQuery())
                .setFitId(sampleQcParams.getMsFitId())
                .setFitMethod(sampleQcParams.getMsFitMethod())
                .setFitSigVersion(sampleQcParams.getMsFitSigVersion())
                .setFitOrgan(sampleQcParams.getMsFitOrgan())
                .setFitNBoot(sampleQcParams.getMsFitNBoot())
                .setFitThresholdPerc(sampleQcParams.getMsFitThresholdPerc())
                .setFitThresholdPval(sampleQcParams.getMsFitThresholdPval())
                .setFitMaxRareSigs(sampleQcParams.getMsFitMaxRareSigs())
                .setFitSignaturesFile(sampleQcParams.getMsFitSignaturesFile())
                .setFitRareSignaturesFile(sampleQcParams.getMsFitRareSignaturesFile())
                .setSkip(skip);

        // Execute the mutational signature analysis and add its step attributes if exist
        ExecutionResult executionResult = toolRunner.execute(MutationalSignatureAnalysis.class, study, mutationalSignatureParams, outPath,
                null, token);
        addStepAttribute(STEP_EXECUTION_RESULT_ATTRIBUTE_KEY, executionResult);

        // Check execution status
        if (executionResult.getStatus().getName() != Status.Type.DONE) {
            throw new ToolException("Something wrong happened running the mutational signature analysis. Execution status = "
                    + executionResult.getStatus().getName());
        }
    }

    private void runGenomePlot() throws ToolException {
        Path outPath = getOutDir().resolve(GENOME_PLOT_STEP);
        try {
            FileUtils.forceMkdir(outPath.toFile());
        } catch (IOException e) {
            throw new ToolException("Error creating genome plot folder: " + outPath, e);
        }

        // Prepare parameters
        GenomePlotAnalysisParams genomePlotParams = new GenomePlotAnalysisParams()
                .setSample(sampleQcParams.getSample())
                .setId(sampleQcParams.getGpId())
                .setDescription(sampleQcParams.getGpDescription())
                .setConfigFile(sampleQcParams.getGpConfigFile());

        // Execute the genome plot analysis and add its step attributes if exist
        ExecutionResult executionResult = toolRunner.execute(GenomePlotAnalysis.class, study, genomePlotParams, outPath, null, token);
        addStepAttribute(STEP_EXECUTION_RESULT_ATTRIBUTE_KEY, executionResult);

        // Check execution status
        if (executionResult.getStatus().getName() != Status.Type.DONE) {
            throw new ToolException("Something wrong happened running the mutational signature analysis. Execution status = "
                    + executionResult.getStatus().getName());
        }
    }

    public static String getSampleIsNotSomaticMsg(String id) {
        return "sample '" + id + "' is not somatic.";
    }
}
