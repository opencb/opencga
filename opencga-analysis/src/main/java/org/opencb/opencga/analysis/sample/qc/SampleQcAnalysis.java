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
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.qc.SampleQcVariantStats;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.individual.qc.IndividualQcUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.analysis.variant.genomePlot.GenomePlotAnalysis;
import org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureAnalysis;
import org.opencb.opencga.analysis.variant.stats.SampleVariantStatsAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.variant.GenomePlotAnalysisParams;
import org.opencb.opencga.core.models.variant.MutationalSignatureAnalysisParams;
import org.opencb.opencga.core.models.variant.SampleQcAnalysisParams;
import org.opencb.opencga.core.models.variant.SampleVariantStatsAnalysisParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

import static org.opencb.opencga.core.models.study.StudyAclEntry.StudyPermissions.WRITE_SAMPLES;

@Tool(id = SampleQcAnalysis.ID, resource = Enums.Resource.SAMPLE, description = SampleQcAnalysis.DESCRIPTION)
public class SampleQcAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "sample-qc";
    public static final String DESCRIPTION = "Run quality control (QC) for a given sample. It includes variant stats and genome plot; and "
            + " for somatic samples, mutational signature.";

    @ToolParams
    protected final SampleQcAnalysisParams analysisParams = new SampleQcAnalysisParams();

    private Path genomePlotConfigPath;

    private boolean runVariantStats = true;
    private boolean runSignature = true;
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

        // Check variant stats
        final String OPENCGA_ALL = "ALL";
        if (OPENCGA_ALL.equals(analysisParams.getVariantStatsId())) {
            throw new ToolException("Invalid parameters: " + OPENCGA_ALL + " is a reserved word, you can not use as a"
                    + " variant stats ID");
        }

        if (StringUtils.isEmpty(analysisParams.getVariantStatsId()) && analysisParams.getVariantStatsQuery() != null
                && !analysisParams.getVariantStatsQuery().toParams().isEmpty()) {
            throw new ToolException("Invalid parameters: if variant stats ID is empty, variant stats query must be empty");
        }
        if (StringUtils.isNotEmpty(analysisParams.getVariantStatsId())
                && (analysisParams.getVariantStatsQuery() == null || analysisParams.getVariantStatsQuery().toParams().isEmpty())) {
            throw new ToolException("Invalid parameters: if you provide a variant stats ID, variant stats query can not be empty");
        }
        if (StringUtils.isEmpty(analysisParams.getVariantStatsId())) {
            analysisParams.setVariantStatsId(OPENCGA_ALL);
        }

        if (sample.getQualityControl() != null && sample.getQualityControl().getVariantMetrics() != null) {
            if (CollectionUtils.isNotEmpty(sample.getQualityControl().getVariantMetrics().getVariantStats())
                    && OPENCGA_ALL.equals(analysisParams.getVariantStatsId())) {
                runVariantStats = false;
            } else {
                for (SampleQcVariantStats variantStats : sample.getQualityControl().getVariantMetrics().getVariantStats()) {
                    if (variantStats.getId().equals(analysisParams.getVariantStatsId())) {
                        throw new ToolException("Invalid parameters: variant stats ID '" + analysisParams.getVariantStatsId()
                                + "' is already used");
                    }
                }
            }
        }

        // Check mutational signature
        if (MapUtils.isEmpty(analysisParams.getSignatureQuery())) {
            runSignature = false;
        }

        // Check genome plot
        if (StringUtils.isEmpty(analysisParams.getGenomePlotConfigFile())) {
            runGenomePlot = false;
        } else {
            File genomePlotConfFile = AnalysisUtils.getCatalogFile(analysisParams.getGenomePlotConfigFile(), getStudy(),
                    catalogManager.getFileManager(), getToken());
            genomePlotConfigPath = Paths.get(genomePlotConfFile.getUri().getPath());
            if (!genomePlotConfigPath.toFile().exists()) {
                throw new ToolException("Invalid parameters: genome plot configuration file does not exist (" + genomePlotConfigPath + ")");
            }
        }
    }

    @Override
    protected void run() throws ToolException {
        step(() -> {
            try {
                Map<String, Object> params;
                OpenCGAResult<Job> variantStatsJobResult;
                OpenCGAResult<Job> signatureJobResult;
                OpenCGAResult<Job> genomePlotJobResult;

                if (runVariantStats) {
                    // Run variant stats
                    params = new SampleVariantStatsAnalysisParams(Collections.singletonList(analysisParams.getSample()), null, null, true,
                            false, analysisParams.getVariantStatsId(), analysisParams.getVariantStatsDescription(), null,
                            analysisParams.getVariantStatsQuery())
                            .toParams(new ObjectMap(ParamConstants.STUDY_PARAM, getStudy()));

                    variantStatsJobResult = catalogManager.getJobManager()
                            .submit(getStudy(), SampleVariantStatsAnalysis.ID, Enums.Priority.MEDIUM, params, null, "Job generated by "
                                    + getId() + " - " + getJobId(), Collections.emptyList(), Collections.emptyList(), token);
                    addEvent(Event.Type.INFO, "Submit job " + variantStatsJobResult.first().getId() + " to compute sample variant stats ("
                            + SampleVariantStatsAnalysis.ID + ")");
                }

                if (runSignature) {
                    // Run mutational signature
                    // Be sure to update sample quality control
                    analysisParams.getSignatureQuery().put(MutationalSignatureAnalysis.QC_UPDATE_KEYNAME, "true");
                    params = new MutationalSignatureAnalysisParams(analysisParams.getSample(), analysisParams.getSignatureId(),
                            analysisParams.getSignatureDescription(), analysisParams.getSignatureQuery(),
                            analysisParams.getSignatureRelease(), true, null)
                            .toParams(new ObjectMap(ParamConstants.STUDY_PARAM, getStudy()));

                    signatureJobResult = catalogManager.getJobManager()
                            .submit(getStudy(), MutationalSignatureAnalysis.ID, Enums.Priority.MEDIUM, params, null, "Job generated by "
                                    + getId() + " - " + getJobId(), Collections.emptyList(), Collections.emptyList(), token);
                    addEvent(Event.Type.INFO, "Submit job " + signatureJobResult.first().getId() + " to compute the mutational signature ("
                            + MutationalSignatureAnalysis.ID + ")");
                }

                if (runGenomePlot) {
                    // Run genome plot
                    params = new GenomePlotAnalysisParams(analysisParams.getSample(), analysisParams.getGenomePlotId(),
                            analysisParams.getGenomePlotDescription(), analysisParams.getGenomePlotConfigFile(), null)
                            .toParams(new ObjectMap(ParamConstants.STUDY_PARAM, getStudy()));

                    genomePlotJobResult = catalogManager.getJobManager()
                            .submit(getStudy(), GenomePlotAnalysis.ID, Enums.Priority.MEDIUM, params, null,
                                    "Job generated by " + getId() + " - " + getJobId(), Collections.emptyList(), Collections.emptyList(),
                                    token);
                    addEvent(Event.Type.INFO, "Submit job " + genomePlotJobResult.first().getId() + " to compute genome plot ("
                            + GenomePlotAnalysis.ID + ")");
                }


                // Wait for those jobs ???
//                waitFor(variantStatsJobResult.first().getId());
//                waitFor(signatureJobResult.first().getId());
//                waitFor(genomePlotJobResult.first().getId());
            } catch (CatalogException e) {
                throw new ToolException(e);
            }
        });
    }
}
