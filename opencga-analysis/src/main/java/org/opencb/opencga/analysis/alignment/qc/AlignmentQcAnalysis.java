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
import org.opencb.biodata.formats.alignment.picard.HsMetrics;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsFlagstats;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsStats;
import org.opencb.biodata.formats.sequence.fastqc.FastQcMetrics;
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
import org.opencb.opencga.core.models.file.FileQualityControl;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.core.api.ParamConstants.ALIGNMENT_QC_DESCRIPTION;

@Tool(id = AlignmentQcAnalysis.ID, resource = Enums.Resource.ALIGNMENT)
public class AlignmentQcAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "alignment-qc";
    public static final String DESCRIPTION = ALIGNMENT_QC_DESCRIPTION;

    @ToolParams
    protected final AlignmentQcParams analysisParams = new AlignmentQcParams();

    private File catalogBamFile;
    private AlignmentFileQualityControl alignmentQc = null;

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
            catalogBamFile = AnalysisUtils.getCatalogFile(analysisParams.getBamFile(), study, catalogManager.getFileManager(), token);
            if (catalogBamFile.getQualityControl() != null) {
                alignmentQc = catalogBamFile.getQualityControl().getAlignment();
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
            Map<String, Object> params;
            String statsJobId = null;
            String flagStatsJobId = null;
            String fastQcMetricsJobId = null;
            String hsMetricsJobId = null;

            try {
                if (runFlagStats) {
                    // Flag stats
                    params = new AlignmentFlagStatsParams(analysisParams.getBamFile(), null)
                            .toParams(new ObjectMap(ParamConstants.STUDY_PARAM, study));

                    OpenCGAResult<Job> flagStatsJobResult = catalogManager.getJobManager()
                            .submit(study, AlignmentFlagStatsAnalysis.ID, Enums.Priority.MEDIUM, params, null, "Job generated by "
                                    + getId() + " - " + getJobId(), Collections.emptyList(), Collections.emptyList(), token);
                    flagStatsJobId = flagStatsJobResult.first().getId();
                    addEvent(Event.Type.INFO, "Submit job " +  flagStatsJobId + " to compute stats (" + AlignmentFlagStatsAnalysis.ID
                            + ")");
                }
            } catch (CatalogException e) {
                addWarning("Error launching job for Alignment Flag Stats Analysis: " + e.getMessage());
            }

            try {
                if (runStats) {
                    // Stats
                    params = new AlignmentStatsParams(analysisParams.getBamFile(), null)
                            .toParams(new ObjectMap(ParamConstants.STUDY_PARAM, study));

                    OpenCGAResult<Job> statsJobResult = catalogManager.getJobManager()
                            .submit(study, AlignmentStatsAnalysis.ID, Enums.Priority.MEDIUM, params, null, "Job generated by "
                                    + getId() + " - " + getJobId(), Collections.emptyList(), Collections.emptyList(), token);
                    statsJobId = statsJobResult.first().getId();
                    addEvent(Event.Type.INFO, "Submit job " + statsJobId + " to compute stats (" + AlignmentStatsAnalysis.ID + ")");
                }
            } catch (CatalogException e) {
                addWarning("Error launching job for Alignment Stats Analysis: " + e.getMessage());
            }

            try {
                if (runFastqc) {
                    // FastQC metrics
                    params = new AlignmentFastQcMetricsParams(analysisParams.getBamFile(), null)
                            .toParams(new ObjectMap(ParamConstants.STUDY_PARAM, study));

                    OpenCGAResult<Job> fastQcMetricsJobResult = catalogManager.getJobManager()
                            .submit(study, AlignmentFastQcMetricsAnalysis.ID, Enums.Priority.MEDIUM, params, null,
                                    "Job generated by " + getId() + " - " + getJobId(), Collections.emptyList(), Collections.emptyList(),
                                    token);
                    fastQcMetricsJobId = fastQcMetricsJobResult.first().getId();
                    addEvent(Event.Type.INFO, "Submit job " + fastQcMetricsJobId + " to compute FastQC metrics ("
                            + AlignmentFastQcMetricsAnalysis.ID + ")");
                }
            } catch (CatalogException e) {
                addWarning("Error launching job for Alignment FastQC Metrics Analysis: " + e.getMessage());
            }

            try {
                if (runHsmetrics) {
                    // HS metrics
                    params = new AlignmentHsMetricsParams(analysisParams.getBamFile(), analysisParams.getBedFile(),
                            analysisParams.getDictFile(), null).toParams(new ObjectMap(ParamConstants.STUDY_PARAM, study));

                    OpenCGAResult<Job> hsMetricsJobResult = catalogManager.getJobManager()
                            .submit(study, AlignmentHsMetricsAnalysis.ID, Enums.Priority.MEDIUM, params, null,
                                    "Job generated by " + getId() + " - " + getJobId(), Collections.emptyList(), Collections.emptyList(),
                                    token);
                    hsMetricsJobId = hsMetricsJobResult.first().getId();
                    addEvent(Event.Type.INFO, "Submit job " + hsMetricsJobId + " to compute HS metrics (" + AlignmentHsMetricsAnalysis.ID
                            + ")");
                }
            } catch (CatalogException e) {
                addWarning("Error launching job for Alignment HS Metrics Analysis: " + e.getMessage());
            }

            // Wait for those jobs before saving QC
            SamtoolsFlagstats samtoolsFlagstats = null;
            SamtoolsStats samtoolsStats = null;
            FastQcMetrics fastQcMetrics = null;
            HsMetrics hsMetrics = null;

            if (flagStatsJobId != null) {
                try {
                    if (waitFor(flagStatsJobId)) {
                        Job job = getJob(flagStatsJobId);
                        Path resultPath = AlignmentFlagStatsAnalysis.getResultPath(job.getOutDir().getUri().getPath(),
                                catalogBamFile.getName());
                        samtoolsFlagstats = AlignmentFlagStatsAnalysis.parseResults(resultPath);
                    }
                } catch (Exception e) {
                    addWarning("Error waiting for job '" + flagStatsJobId + "' (Alignment Flag Stats Analysis): " + e.getMessage());
                }
            }

            if (statsJobId != null) {
                try {
                    if (waitFor(statsJobId)) {
                        Job job = getJob(statsJobId);
                        Path resultPath = AlignmentStatsAnalysis.getResultPath(job.getOutDir().getUri().getPath(),
                                catalogBamFile.getName());
                        samtoolsStats = AlignmentStatsAnalysis.parseResults(resultPath, Paths.get(job.getOutDir().getUri().getPath()));
                    }
                } catch (Exception e) {
                    addWarning("Error waiting for job '" + statsJobId + "' (Alignment Stats Analysis): " + e.getMessage());
                }
            }

            if (fastQcMetricsJobId != null) {
                try {
                    if (waitFor(fastQcMetricsJobId)) {
                        Job job = getJob(fastQcMetricsJobId);
                        fastQcMetrics = AlignmentFastQcMetricsAnalysis.parseResults(Paths.get(job.getOutDir().getUri().getPath()));
                    }
                } catch (Exception e) {
                    addWarning("Error waiting for job '" + fastQcMetricsJobId + "' (Alignment FastQC Metrics Analysis): " + e.getMessage());
                }
            }
            if (hsMetricsJobId != null) {
                try {
                    if (waitFor(hsMetricsJobId)) {
                        Job job = getJob(hsMetricsJobId);
                        logger.info("Alignment HS Metrics Analysis, job.outDir = " + job.getOutDir());
                        hsMetrics = AlignmentHsMetricsAnalysis.parseResults(Paths.get(job.getOutDir().getUri().getPath()));
                    }
                } catch (Exception e) {
                    addWarning("Error waiting for job '" + hsMetricsJobId + "' (Alignment FastQC Metrics Analysis): " + e.getMessage());
                }
            }

            // Update quality control for the catalog file
            catalogBamFile = AnalysisUtils.getCatalogFile(analysisParams.getBamFile(), study, catalogManager.getFileManager(), token);
            FileQualityControl qc = catalogBamFile.getQualityControl();
            // Sanity check
            if (qc == null) {
                qc = new FileQualityControl();
            } else if (qc.getAlignment() == null) {
                qc.setAlignment(new AlignmentFileQualityControl());
            }

            boolean saveQc = false;
            if (samtoolsFlagstats != null) {
                qc.getAlignment().setSamtoolsFlagStats(samtoolsFlagstats);
                saveQc = true;
            }
            if (samtoolsStats != null) {
                qc.getAlignment().setSamtoolsStats(samtoolsStats);
                saveQc = true;
            }
            if (fastQcMetrics != null) {
                qc.getAlignment().setFastQcMetrics(fastQcMetrics);
                saveQc = true;
            }
            if (hsMetrics != null) {
                qc.getAlignment().setHsMetrics(hsMetrics);
                saveQc = true;
            }

            if (saveQc) {
                catalogManager.getFileManager().update(getStudy(), catalogBamFile.getId(), new FileUpdateParams().setQualityControl(qc),
                        QueryOptions.empty(), getToken());
            }
        });
    }

    private boolean waitFor(String jobId) throws ToolException {
        Query query = new Query("id", jobId);
        OpenCGAResult<Job> result = null;
        try {
            result = catalogManager.getJobManager().search(study, query, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            new ToolException("Error waiting for job '" + jobId + "': " + e.getMessage());
        }
        Job job = result.first();
        String status = job.getInternal().getStatus().getId();

        while (status.equals(Enums.ExecutionStatus.PENDING) || status.equals(Enums.ExecutionStatus.RUNNING)
                || status.equals(Enums.ExecutionStatus.QUEUED) || status.equals(Enums.ExecutionStatus.READY)
                || status.equals(Enums.ExecutionStatus.REGISTERING)) {
            // Sleep for 1 minute
            try {
                Thread.sleep(60000);
                result = catalogManager.getJobManager().search(study, query, QueryOptions.empty(), token);
                job = result.first();
            } catch (CatalogException | InterruptedException e) {
                new ToolException("Error waiting for job '" + jobId + "': " + e.getMessage());
            }
            status = job.getInternal().getStatus().getId();
        }

        return status.equals(Enums.ExecutionStatus.DONE) ? true : false;
    }

    private Job getJob(String jobId) throws ToolException {
        Job job = null;
        try {
            Query query = new Query("id", jobId);
            OpenCGAResult<Job> result = catalogManager.getJobManager().search(study, query, QueryOptions.empty(), token);
            job = result.first();
        } catch (CatalogException e) {
            new ToolException("Error getting job '" + jobId + "' from catalog: " + e.getMessage());
        }
        if (job == null) {
            new ToolException("Error getting job '" + jobId + "' from catalog.");
        }
        return job;
    }
}
