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
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.alignment.picard.HsMetrics;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsFlagstats;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsStats;
import org.opencb.biodata.formats.sequence.fastqc.FastQcMetrics;
import org.opencb.biodata.models.clinical.qc.*;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.alignment.qc.AlignmentFastQcMetricsAnalysis;
import org.opencb.opencga.analysis.alignment.qc.AlignmentFlagStatsAnalysis;
import org.opencb.opencga.analysis.alignment.qc.AlignmentHsMetricsAnalysis;
import org.opencb.opencga.analysis.alignment.qc.AlignmentStatsAnalysis;
import org.opencb.opencga.analysis.individual.qc.IndividualQcUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.analysis.variant.genomePlot.GenomePlotAnalysis;
import org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureAnalysis;
import org.opencb.opencga.analysis.variant.stats.SampleVariantStatsAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.alignment.AlignmentFileQualityControl;
import org.opencb.opencga.core.models.alignment.AlignmentQcParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileQualityControl;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleQualityControl;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.models.sample.SampleVariantQualityControlMetrics;
import org.opencb.opencga.core.models.variant.GenomePlotAnalysisParams;
import org.opencb.opencga.core.models.variant.MutationalSignatureAnalysisParams;
import org.opencb.opencga.core.models.variant.SampleQcAnalysisParams;
import org.opencb.opencga.core.models.variant.SampleVariantStatsAnalysisParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.core.models.study.StudyPermissions.Permissions.WRITE_SAMPLES;

@Tool(id = SampleQcAnalysis.ID, resource = Enums.Resource.SAMPLE, description = SampleQcAnalysis.DESCRIPTION)
public class SampleQcAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "sample-qc";
    public static final String DESCRIPTION = "Run quality control (QC) for a given sample. It includes variant stats, and if the sample " +
            "is somatic, mutational signature and genome plot are calculated.";

    @ToolParams
    protected final SampleQcAnalysisParams analysisParams = new SampleQcAnalysisParams();

    private ObjectMap signatureQuery;
    private Path genomePlotConfigPath;

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
            long studyUid = catalogManager.getStudyManager().get(getStudy(), QueryOptions.empty(), token).first().getUid();
            String userId = catalogManager.getUserManager().getUserId(token);
            catalogManager.getAuthorizationManager().checkStudyPermission(studyUid, userId, WRITE_SAMPLES);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        // Sanity check
        if (StringUtils.isEmpty(analysisParams.getSample())) {
            throw new ToolException("Missing sample ID.");
        }

        Sample sample = IndividualQcUtils.getValidSampleById(getStudy(), analysisParams.getSample(), catalogManager, token);
        if (sample == null) {
            throw new ToolException("Sample '" + analysisParams.getSample() + "' not found.");
        }

        // Prepare flags
        String skip = null;
        if (StringUtils.isNotEmpty(analysisParams.getSkip())) {
            skip = analysisParams.getSkip().toLowerCase().replace(" ", "");
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
            if (OPENCGA_ALL.equals(analysisParams.getVsId())) {
                new ToolException("Invalid parameters: " + OPENCGA_ALL + " is a reserved word, you can not use as a variant stats ID");
            }

            if (StringUtils.isEmpty(analysisParams.getVsId()) && analysisParams.getVsQuery() != null
                    && !analysisParams.getVsQuery().toParams().isEmpty()) {
                new ToolException("Invalid parameters: if variant stats ID is empty, variant stats query must be empty");
            }
            if (StringUtils.isNotEmpty(analysisParams.getVsId())
                    && (analysisParams.getVsQuery() == null || analysisParams.getVsQuery().toParams().isEmpty())) {
                new ToolException("Invalid parameters: if you provide a variant stats ID, variant stats query can not be empty");
            }
            if (StringUtils.isEmpty(analysisParams.getVsId())) {
                analysisParams.setVsId(OPENCGA_ALL);
            }

            if (analysisParams.getVsQuery() == null) {
                new ToolException("Invalid parameters: variant stats query is empty");
            }
            if (sample.getQualityControl() != null && sample.getQualityControl().getVariant() != null) {
                if (CollectionUtils.isNotEmpty(sample.getQualityControl().getVariant().getVariantStats())
                        && OPENCGA_ALL.equals(analysisParams.getVsId())) {
                    runVariantStats = false;
                } else {
                    for (SampleQcVariantStats variantStats : sample.getQualityControl().getVariant().getVariantStats()) {
                        if (variantStats.getId().equals(analysisParams.getVsId())) {
                            throw new ToolException("Invalid parameters: variant stats ID '" + analysisParams.getVsId()
                                    + "' is already used");
                        }
                    }
                }
            }
        }

        // Check mutational signature
        if (runSignatureCatalogue) {
            if (StringUtils.isEmpty(analysisParams.getMsQuery())) {
                new ToolException("Invalid parameters: mutational signature query is empty");
            }
        }

        if (runSignatureCatalogue && !sample.isSomatic()) {
            String msg = "Skipping mutational signature catalog analysis: sample '" + sample.getId() + "' is not somatic.";
            addWarning(msg);
            logger.warn(msg);
            runSignatureCatalogue = false;
        }

        if (runSignatureFitting && !sample.isSomatic()) {
            String msg = "Skipping mutational signature fitting analysis: sample '" + sample.getId() + "' is not somatic.";
            addWarning(msg);
            logger.warn(msg);
            runSignatureFitting = false;
        }

        // Check genome plot
        if (runGenomePlot) {
            if (StringUtils.isEmpty(analysisParams.getGpConfigFile())) {
                new ToolException("Invalid parameters: genome plot configuration file is empty");
            }
            if (runGenomePlot && !sample.isSomatic()) {
                String msg = "Skipping genome plot: sample '" + sample.getId() + "' is not somatic.";
                addWarning(msg);
                logger.warn(msg);
                runGenomePlot = false;
            } else {
                File genomePlotConfFile = AnalysisUtils.getCatalogFile(analysisParams.getGpConfigFile(), getStudy(),
                        catalogManager.getFileManager(), getToken());
                genomePlotConfigPath = Paths.get(genomePlotConfFile.getUri().getPath());
                if (!genomePlotConfigPath.toFile().exists()) {
                    new ToolException("Invalid parameters: genome plot configuration file does not exist (" + genomePlotConfigPath + ")");
                }
            }
        }
    }

    @Override
    protected void run() throws ToolException {
        step(() -> {
            Map<String, Object> params;
            String variantStatsJobId = null;
            String signatureJobId = null;
            String genomePlotJobId = null;

            try {
                if (runVariantStats) {
                    // Run variant stats
                    params = new SampleVariantStatsAnalysisParams(Collections.singletonList(analysisParams.getSample()), null, null, true,
                            false, analysisParams.getVsId(), analysisParams.getVsDescription(), null,
                            analysisParams.getVsQuery())
                            .toParams(new ObjectMap(ParamConstants.STUDY_PARAM, getStudy()));

                    OpenCGAResult<Job> variantStatsJobResult = catalogManager.getJobManager()
                            .submit(study, SampleVariantStatsAnalysis.ID, Enums.Priority.MEDIUM, params, null, "Job generated by "
                                    + getId() + " - " + getJobId(), Collections.emptyList(), Collections.emptyList(), token);
                    variantStatsJobId = variantStatsJobResult.first().getId();
                    addEvent(Event.Type.INFO, "Submit job " + variantStatsJobId + " to compute stats (" + SampleVariantStatsAnalysis.ID
                            + ")");
                }
            } catch (CatalogException e) {
                addWarning("Error launching job for sample variant stats analysis: " + e.getMessage());
                variantStatsJobId = null;
            }

            try {
                if (runSignatureCatalogue || runSignatureFitting) {
                    // Run mutational signature

                    // Be sure to update sample quality control
                    signatureQuery = JacksonUtils.getDefaultObjectMapper().readValue(analysisParams.getMsQuery(), ObjectMap.class);
                    signatureQuery.append(MutationalSignatureAnalysis.QC_UPDATE_KEYNAME, true);
                    String queryString = signatureQuery.toJson();

                    params = new MutationalSignatureAnalysisParams()
                            .setId(analysisParams.getMsId())
                            .setDescription(analysisParams.getMsDescription())
                            .setQuery(queryString)
                            .setFitId(analysisParams.getMsFitId())
                            .setFitMethod(analysisParams.getMsFitMethod())
                            .setFitSigVersion(analysisParams.getMsFitSigVersion())
                            .setFitOrgan(analysisParams.getMsFitOrgan())
                            .setFitNBoot(analysisParams.getMsFitNBoot())
                            .setFitThresholdPerc(analysisParams.getMsFitThresholdPerc())
                            .setFitThresholdPval(analysisParams.getMsFitThresholdPval())
                            .setFitMaxRareSigs(analysisParams.getMsFitMaxRareSigs())
                            .setFitSignaturesFile(analysisParams.getMsFitSignaturesFile())
                            .setFitRareSignaturesFile(analysisParams.getMsFitRareSignaturesFile())
                            .toParams(new ObjectMap(ParamConstants.STUDY_PARAM, getStudy()));

                    OpenCGAResult<Job> signatureJobResult = catalogManager.getJobManager()
                            .submit(getStudy(), MutationalSignatureAnalysis.ID, Enums.Priority.MEDIUM, params, null, "Job generated by "
                                    + getId() + " - " + getJobId(), Collections.emptyList(), Collections.emptyList(), token);
                    addEvent(Event.Type.INFO, "Submit job " + signatureJobResult.first().getId() + " to compute the mutational signature ("
                            + MutationalSignatureAnalysis.ID + ")");
                }
            } catch (CatalogException e) {
                throw new ToolException(e);
            }


            try {
                if (runGenomePlot) {
                    // Run genome plot
                    params = new GenomePlotAnalysisParams(analysisParams.getSample(), analysisParams.getGpId(),
                            analysisParams.getGpDescription(), analysisParams.getGpConfigFile(), null)
                            .toParams(new ObjectMap(ParamConstants.STUDY_PARAM, getStudy()));

                    OpenCGAResult<Job> genomePlotJobResult = catalogManager.getJobManager()
                            .submit(getStudy(), GenomePlotAnalysis.ID, Enums.Priority.MEDIUM, params, null,
                                    "Job generated by " + getId() + " - " + getJobId(), Collections.emptyList(), Collections.emptyList(),
                                    token);
                    genomePlotJobId = genomePlotJobResult.first().getId();
                    addEvent(Event.Type.INFO, "Submit job " + genomePlotJobId + " to compute genome plot (" + GenomePlotAnalysis.ID
                            + ")");
                }
            } catch (CatalogException e) {
                addWarning("Error launching job for sample genome plot analysis: " + e.getMessage());
                genomePlotJobId = null;
            }


            // Wait for those jobs before saving QC
            Signature signature = null;
            SignatureFitting signatureFitting = null;
            GenomePlot genomePlot = null;

            if (variantStatsJobId != null) {
                try {
                    AnalysisUtils.waitFor(variantStatsJobId, getStudy(), catalogManager.getJobManager(), getToken());
                } catch (Exception e) {
                    addWarning("Error waiting for job '" + variantStatsJobId + "' (sample variant stats): " + e.getMessage());
                }
            }

            if (signatureJobId != null) {
                try {
                    if (AnalysisUtils.waitFor(signatureJobId, getStudy(), catalogManager.getJobManager(), getToken())) {
                        Job job = AnalysisUtils.getJob(signatureJobId, getStudy(), catalogManager.getJobManager(), getToken());
                        Path outPath = Paths.get(job.getOutDir().getUri().getPath());
                        if (runSignatureCatalogue) {
                            // Parse mutational signature catalogue results
                            List<Signature.GenomeContextCount> counts = MutationalSignatureAnalysis.parseCatalogueResults(outPath);
                            signature = new Signature()
                                    .setId(analysisParams.getMsId())
                                    .setDescription(analysisParams.getMsDescription())
                                    .setQuery(signatureQuery)
                                    .setType("SNV")
                                    .setCounts(counts);
                        }
                        if (runSignatureFitting) {
                            // Parse mutational signature fitting results
                            signatureFitting = MutationalSignatureAnalysis.parseFittingResults(outPath, analysisParams.getMsFitId(),
                                    analysisParams.getMsFitMethod(), analysisParams.getMsFitSigVersion(), analysisParams.getMsFitNBoot(),
                                    analysisParams.getMsFitOrgan(), analysisParams.getMsFitThresholdPerc(),
                                    analysisParams.getMsFitThresholdPval(), analysisParams.getMsFitMaxRareSigs());
                        }
                    }
                } catch (Exception e) {
                    addWarning("Error waiting for job '" + signatureJobId + "' (mutational signature analysis): " + e.getMessage());
                }
            }

            if (genomePlotJobId != null) {
                try {
                    if (AnalysisUtils.waitFor(genomePlotJobId, getStudy(), catalogManager.getJobManager(), getToken())) {
                        Job job = AnalysisUtils.getJob(genomePlotJobId, getStudy(), catalogManager.getJobManager(), getToken());

                        // Parse configuration file
                        GenomePlotConfig plotConfig = JacksonUtils.getDefaultObjectMapper().readerFor(GenomePlotConfig.class)
                                .readValue(genomePlotConfigPath.toFile());

                        // Parse genome plot results
                        genomePlot = GenomePlotAnalysis.parseResults(Paths.get(job.getOutDir().getUri().getPath()),
                                analysisParams.getGpDescription(), plotConfig);
                    }
                } catch (Exception e) {
                    addWarning("Error waiting for job '" + genomePlotJobId + "' (genome plot analysis): " + e.getMessage());
                }
            }

            // Update quality control for the sample
            Sample sample = IndividualQcUtils.getValidSampleById(getStudy(), analysisParams.getSample(), catalogManager, token);
            SampleQualityControl qc = sample.getQualityControl();

            // Sanity check
            if (qc == null) {
                qc = new SampleQualityControl();
            } else if (qc.getVariant() == null) {
                qc.setVariant(new SampleVariantQualityControlMetrics());
            }

            boolean saveQc = false;
            // Variant stats of the quality control are updated in the variant stats analysis itself !!!
            if (signature != null) {
                if (qc.getVariant().getSignatures() == null) {
                    qc.getVariant().setSignatures(new ArrayList<>());
                }
                qc.getVariant().getSignatures().add(signature);
                saveQc = true;
            }
            if (signatureFitting != null) {
                if (qc.getVariant().getSignatures() == null) {
                    // Never have to be here
                } else {
                    for (Signature sig : qc.getVariant().getSignatures()) {
                        if (sig.getId().equals(analysisParams.getMsId())) {
                            if (CollectionUtils.isEmpty(sig.getFittings())) {
                                sig.setFittings(new ArrayList<>());
                            }
                            sig.getFittings().add(signatureFitting);
                            saveQc = true;
                            break;
                        }
                    }
                }
            }
            if (genomePlot != null) {
                qc.getVariant().setGenomePlot(genomePlot);
                saveQc = true;
            }

            if (saveQc) {
                catalogManager.getSampleManager().update(getStudy(), sample.getId(), new SampleUpdateParams().setQualityControl(qc),
                        QueryOptions.empty(), getToken());
            }
        });
    }
}
