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

package org.opencb.opencga.analysis.alignment.qc;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.alignment.*;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.job.Execution;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.util.*;

import static org.opencb.opencga.core.api.ParamConstants.ALIGNMENT_QC_DESCRIPTION;

@Tool(id = AlignmentQcAnalysis.ID, resource = Enums.Resource.ALIGNMENT)
public class AlignmentQcAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "alignment-qc";
    public static final String DESCRIPTION = ALIGNMENT_QC_DESCRIPTION;

    @ToolParams
    protected final AlignmentQcParams analysisParams = new AlignmentQcParams();

    private boolean runStats = true;
    private boolean runFlagStats = true;
    private boolean runFastqc = true;
    private boolean runHsmetrics = true;

    @Override
    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study");
        }

        try {
            File catalogFile = AnalysisUtils.getCatalogFile(analysisParams.getBamFile(), study, catalogManager.getFileManager(), token);
            AlignmentFileQualityControl alignmentQc = null;
            if (catalogFile.getQualityControl() != null) {
                alignmentQc = catalogFile.getQualityControl().getAlignment();
            }

            // Prepare flags
            String skip = null;
            if (StringUtils.isNotEmpty(analysisParams.getSkip())) {
                skip = analysisParams.getSkip().toLowerCase().replace(" ", "");
            }
            if (StringUtils.isNotEmpty(skip)) {
                Set<String> skipValues = new HashSet<>(Arrays.asList(skip.split(",")));
                if (skipValues.contains(AlignmentQcParams.STATS_SKIP_VALUE)
                        ||
                        (!analysisParams.isOverwrite() && alignmentQc != null && alignmentQc.getSamtoolsStats() != null)) {
                    runStats = false;
                }
                if (skipValues.contains(AlignmentQcParams.FLAGSTATS_SKIP_VALUE)
                        ||
                        (!analysisParams.isOverwrite() && alignmentQc != null && alignmentQc.getSamtoolsFlagStats() != null)) {
                    runFlagStats = false;
                }
                if (skipValues.contains(AlignmentQcParams.FASTQC_METRICS_SKIP_VALUE)
                        ||
                        (!analysisParams.isOverwrite() && alignmentQc != null && alignmentQc.getFastQcMetrics() != null)) {
                    runFastqc = false;
                }
                if (skipValues.contains(AlignmentQcParams.HS_METRICS_SKIP_VALUE)
                        ||
                        (!analysisParams.isOverwrite() && alignmentQc != null && alignmentQc.getHsMetrics() != null)
                        ||
                        StringUtils.isEmpty(analysisParams.getBedFile())
                        ||
                        StringUtils.isEmpty(analysisParams.getDictFile())) {
                    runHsmetrics = false;
                }
            }
        } catch (CatalogException e) {
            throw new ToolException("Error accessing to the BAM file '" + analysisParams.getBamFile() + "'", e);
        }

        if (runHsmetrics) {
            try {
                AnalysisUtils.getCatalogFile(analysisParams.getBedFile(), study, catalogManager.getFileManager(), token);
            } catch (CatalogException e) {
                throw new ToolException("Error accessing to the BED file '" + analysisParams.getBedFile() + "'", e);
            }

            try {
                AnalysisUtils.getCatalogFile(analysisParams.getDictFile(), study, catalogManager.getFileManager(), token);
            } catch (CatalogException e) {
                throw new ToolException("Error accessing to the dictionary file '" + analysisParams.getDictFile() + "'", e);
            }
        }
    }

    @Override
    protected void run() throws ToolException {

        step(() -> {
            try {
                Map<String, Object> params;
                OpenCGAResult<Execution> statsJobResult;
                OpenCGAResult<Execution> flagStatsJobResult;
                OpenCGAResult<Execution> fastQcMetricsJobResult;
                OpenCGAResult<Execution> hsMetricsJobResult;

                if (runStats) {
                    // Stats
                    params = new AlignmentStatsParams(analysisParams.getBamFile(), null)
                            .toParams(new ObjectMap(ParamConstants.STUDY_PARAM, study));

                    statsJobResult = catalogManager.getExecutionManager()
                            .submit(study, AlignmentStatsAnalysis.ID, Enums.Priority.MEDIUM, params, null, "Job generated by "
                                    + getId() + " - " + getJobId(), Collections.emptyList(), Collections.emptyList(), token);
                    addEvent(Event.Type.INFO, "Submit job " + statsJobResult.first().getId() + " to compute stats ("
                            + AlignmentStatsAnalysis.ID + ")");
                }

                if (runFlagStats) {
                    // Flag stats
                    params = new AlignmentFlagStatsParams(analysisParams.getBamFile(), null)
                            .toParams(new ObjectMap(ParamConstants.STUDY_PARAM, study));

                    flagStatsJobResult = catalogManager.getExecutionManager()
                            .submit(study, AlignmentFlagStatsAnalysis.ID, Enums.Priority.MEDIUM, params, null, "Job generated by "
                                    + getId() + " - " + getJobId(), Collections.emptyList(), Collections.emptyList(), token);
                    addEvent(Event.Type.INFO, "Submit job " + flagStatsJobResult.first().getId() + " to compute stats ("
                            + AlignmentFlagStatsAnalysis.ID + ")");
                }

                if (runFastqc) {
                    // FastQC metrics
                    params = new AlignmentFastQcMetricsParams(analysisParams.getBamFile(), null)
                            .toParams(new ObjectMap(ParamConstants.STUDY_PARAM, study));

                    fastQcMetricsJobResult = catalogManager.getExecutionManager()
                            .submit(study, AlignmentFastQcMetricsAnalysis.ID, Enums.Priority.MEDIUM, params, null,
                                    "Job generated by " + getId() + " - " + getJobId(), Collections.emptyList(), Collections.emptyList(),
                                    token);
                    addEvent(Event.Type.INFO, "Submit job " + fastQcMetricsJobResult.first().getId() + " to compute FastQC metrics ("
                            + AlignmentFastQcMetricsAnalysis.ID + ")");
                }

                if (runHsmetrics) {
                    // HS metrics
                    params = new AlignmentHsMetricsParams(analysisParams.getBamFile(), analysisParams.getBedFile(),
                            analysisParams.getDictFile(), null).toParams(new ObjectMap(ParamConstants.STUDY_PARAM, study));

                    hsMetricsJobResult = catalogManager.getExecutionManager()
                            .submit(study, AlignmentHsMetricsAnalysis.ID, Enums.Priority.MEDIUM, params, null,
                                    "Job generated by " + getId() + " - " + getJobId(), Collections.emptyList(), Collections.emptyList(),
                                    token);
                    addEvent(Event.Type.INFO, "Submit job " + hsMetricsJobResult.first().getId() + " to compute HS metrics ("
                            + AlignmentHsMetricsAnalysis.ID + ")");
                }

                // Wait for those jobs ???
//                waitFor(statsJobResult.first().getId());
//                waitFor(flagStatsJobResult.first().getId());
//                waitFor(fastQcMetricsJobResult.first().getId());
//                waitFor(hsMetricsJobResult.first().getId());
            } catch (CatalogException e) {
                throw new ToolException(e);
            }
        });
    }

    private void waitFor(String jobId) throws InterruptedException, CatalogException, ToolException {
        Job job;
        String status;
        Query query = new Query("id", jobId);
        do {
            Thread.sleep(3000);
            OpenCGAResult<Job> result = catalogManager.getJobManager().search(study, query, QueryOptions.empty(), token);
            job = result.first();
            status = job.getInternal().getStatus().getName();
        } while (status.equals(Enums.ExecutionStatus.PENDING) || status.equals(Enums.ExecutionStatus.RUNNING)
                || status.equals(Enums.ExecutionStatus.QUEUED) || status.equals(Enums.ExecutionStatus.READY)
                || status.equals(Enums.ExecutionStatus.REGISTERING));

        addEvent(Event.Type.INFO, "Submitted job " + job.getId() + " to compute flag stats (" + job.getTool().getId() + "), execution"
                + " status: " + job.getInternal().getStatus());
    }
}
