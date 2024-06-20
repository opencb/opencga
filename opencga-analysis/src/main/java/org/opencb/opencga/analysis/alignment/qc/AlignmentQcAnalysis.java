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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsFlagstats;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsStats;
import org.opencb.biodata.formats.sequence.fastqc.FastQcMetrics;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.fastqc.FastqcWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.samtools.SamtoolsWrapperAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.alignment.AlignmentQcParams;
import org.opencb.opencga.core.models.alignment.FastqcWrapperParams;
import org.opencb.opencga.core.models.alignment.SamtoolsWrapperParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.file.FileQualityControl;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.core.tools.result.Status;
import org.opencb.opencga.storage.core.StorageEngineFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.*;

import static org.apache.commons.io.FileUtils.readLines;
import static org.opencb.opencga.core.api.ParamConstants.ALIGNMENT_QC_DESCRIPTION;

@Tool(id = AlignmentQcAnalysis.ID, resource = Enums.Resource.ALIGNMENT)
public class AlignmentQcAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "alignment-qc";
    public static final String DESCRIPTION = ALIGNMENT_QC_DESCRIPTION;

    public static final String SAMTOOLS_STATS_STEP = "samtools-stats";
    public static final String SAMTOOLS_FLAGSTATS_STEP = "samtools-flagstats";
    public static final String PLOT_BAMSTATS_STEP = "plot-bamstats";
    public static final String FASTQC_METRICS_STEP = "fastqc-metrics";
    public static final String UPDATE_FILE_ALIGNMENT_QC_STEP = "update-file-alignment-qc";

    @ToolParams
    protected final AlignmentQcParams alignmentQcParams = new AlignmentQcParams();

    private ToolRunner toolRunner;

    private boolean runSamtoolsStatsStep = true;
    private boolean runSamptoolsFlagstatsStep = true;
    private boolean runFastqcMetricsStep = true;
    private boolean updateQcStep = true;

    private File catalogBamFile;
    private File catalogStatsFile;
    private FileQualityControl fileQc = null;

    @Override
    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study");
        }

        try {
            catalogBamFile = AnalysisUtils.getCatalogFile(alignmentQcParams.getBamFile(), study, catalogManager.getFileManager(), token);
            fileQc = catalogBamFile.getQualityControl();
        } catch (CatalogException e) {
            throw new ToolException("Error accessing to the BAM file '" + alignmentQcParams.getBamFile() + "'", e);
        }

        // Prepare flags from skip and overwrite
        String skip = null;
        if (StringUtils.isNotEmpty(alignmentQcParams.getSkip())) {
            skip = alignmentQcParams.getSkip().toLowerCase().replace(" ", "");
        }
        if (StringUtils.isNotEmpty(skip)) {
            Set<String> skipValues = new HashSet<>(Arrays.asList(skip.split(",")));
            if (skipValues.contains(AlignmentQcParams.STATS_SKIP_VALUE)) {
                runSamtoolsStatsStep = false;
                String msg = "Skipping Samtools stats (and plot) by user";
                addWarning(msg);
                logger.warn(msg);
            }
            if (skipValues.contains(AlignmentQcParams.FLAGSTATS_SKIP_VALUE)) {
                runSamptoolsFlagstatsStep = false;
                String msg = "Skipping Samtools flagstats by user";
                addWarning(msg);
                logger.warn(msg);
            }
            if (skipValues.contains(AlignmentQcParams.FASTQC_METRICS_SKIP_VALUE)) {
                runFastqcMetricsStep = false;
                String msg = "Skipping FastQC metrics by user";
                addWarning(msg);
                logger.warn(msg);
            }
        }
        if (!alignmentQcParams.isOverwrite() && fileQc != null && fileQc.getAlignment() != null) {
            if (runSamtoolsStatsStep && fileQc.getAlignment().getSamtoolsStats() != null) {
                runSamtoolsStatsStep = false;
                String msg = "Skipping Samtools stats (and plots) because they already exist and the overwrite flag is not set";
                addWarning(msg);
                logger.warn(msg);
            }
            if (runSamptoolsFlagstatsStep && fileQc.getAlignment().getSamtoolsFlagStats() != null) {
                runSamptoolsFlagstatsStep = false;
                String msg = "Skipping Samtools flag stats because they already exist and the overwrite flag is not set";
                addWarning(msg);
                logger.warn(msg);
            }
            if (runFastqcMetricsStep && fileQc.getAlignment().getFastQcMetrics() != null) {
                runFastqcMetricsStep = false;
                String msg = "Skipping FastQC metrics because they already exist and the overwrite flag is not set";
                addWarning(msg);
                logger.warn(msg);
            }
        }

        updateQcStep = (runSamptoolsFlagstatsStep || runSamtoolsStatsStep || runFastqcMetricsStep) ? true : false;
    }

    @Override
    protected List<String> getSteps() {
        List<String> steps = new ArrayList<>();
        if (runSamtoolsStatsStep) {
            steps.add(SAMTOOLS_STATS_STEP);
            steps.add(PLOT_BAMSTATS_STEP);
        }
        if (runSamptoolsFlagstatsStep) {
            steps.add(SAMTOOLS_FLAGSTATS_STEP);
        }
        if (runFastqcMetricsStep) {
            steps.add(FASTQC_METRICS_STEP);
        }
        if (updateQcStep) {
            steps.add(UPDATE_FILE_ALIGNMENT_QC_STEP);
        }
        return steps;
    }

    @Override
    protected void run() throws ToolException {
        // Create the tool runner
        toolRunner = new ToolRunner(getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(variantStorageManager.getStorageConfiguration()));

        // Get alignment QC metrics to update
        if (catalogBamFile.getQualityControl() != null) {
            fileQc = catalogBamFile.getQualityControl();
        }
        if (fileQc == null) {
            fileQc = new FileQualityControl();
        }

        if (runSamtoolsStatsStep) {
            step(SAMTOOLS_STATS_STEP, this::runSamtoolsStats);
            step(PLOT_BAMSTATS_STEP, this::runPlotBamstats);
        }
        if (runSamptoolsFlagstatsStep) {
            step(SAMTOOLS_FLAGSTATS_STEP, this::runSamtoolsFlagstats);
        }
        if (runFastqcMetricsStep) {
            step(FASTQC_METRICS_STEP, this::runFastqcMetrics);
        }
        if (updateQcStep) {
            step(UPDATE_FILE_ALIGNMENT_QC_STEP, this::updateAlignmentQc);
        }
    }

    private void runSamtoolsFlagstats() throws ToolException {
        Path outPath = getOutDir().resolve(SAMTOOLS_FLAGSTATS_STEP);
        try {
            FileUtils.forceMkdir(outPath.toFile());
        } catch (IOException e) {
            throw new ToolException("Error creating SAMtools flagstat output folder: " + outPath, e);
        }

        // Prepare parameters
        SamtoolsWrapperParams samtoolsWrapperParams = new SamtoolsWrapperParams("flagstat", catalogBamFile.getId(), null,
                new HashMap<>());

        // Execute the Samtools flag stats analysis and add its step attributes if exist
        ExecutionResult executionResult = toolRunner.execute(SamtoolsWrapperAnalysis.class, study, samtoolsWrapperParams, outPath,
                null, token);
        addStepAttributes(executionResult);

        // Check execution status
        if (executionResult.getStatus().getName() != Status.Type.DONE) {
            throw new ToolException("Something wrong happened running the Samtools flagstat analysis. Execution status = "
                    + executionResult.getStatus().getName());
        }

        // Check results and update QC file
        Path flagStatsFile = AlignmentFlagStatsAnalysis.getResultPath(outPath.toAbsolutePath().toString(), catalogBamFile.getName());
        java.io.File stdoutFile = outPath.resolve(DockerWrapperAnalysisExecutor.STDOUT_FILENAME).toFile();
        List<String> lines ;
        try {
            lines = readLines(stdoutFile, Charset.defaultCharset());
        } catch (IOException e) {
            throw new ToolException("Error reading running Samtools flagstat results", e);
        }
        if (CollectionUtils.isNotEmpty(lines) && lines.get(0).contains("QC-passed")) {
            try {
                FileUtils.copyFile(stdoutFile, flagStatsFile.toFile());
            } catch (IOException e) {
                throw new ToolException("Error copying Samtools flagstat results", e);
            }
        } else {
            String msg = DockerWrapperAnalysisExecutor.getStdErrMessage("Something wrong happened running Samtools flagstat analysis.",
                    outPath);
            throw new ToolException(msg);
        }

        // Check results and update QC file
        SamtoolsFlagstats samtoolsFlagstats = AlignmentFlagStatsAnalysis.parseResults(flagStatsFile);
        fileQc.getAlignment().setSamtoolsFlagStats(samtoolsFlagstats);
    }

    private void runSamtoolsStats() throws ToolException {
        Path outPath = getOutDir().resolve(SAMTOOLS_STATS_STEP);
        try {
            FileUtils.forceMkdir(outPath.toFile());
        } catch (IOException e) {
            throw new ToolException("Error creating SAMtools stats output folder: " + outPath, e);
        }

        // Prepare parameters
        Map<String, String> statsParams = new HashMap<>();
        // Filter flag:
        //   - not primary alignment (0x100)
        //   - read fails platform/vendor quality checks (0x200)
        //   - supplementary alignment (0x800)
        statsParams.put("F", "0xB00");
        SamtoolsWrapperParams samtoolsWrapperParams = new SamtoolsWrapperParams("stats", catalogBamFile.getId(), null, statsParams);

        // Execute the Samtools stats analysis and add its step attributes if exist
        ExecutionResult executionResult = toolRunner.execute(SamtoolsWrapperAnalysis.class, study, samtoolsWrapperParams, outPath,
                null, token);
        addStepAttributes(executionResult);

        // Check execution status
        if (executionResult.getStatus().getName() != Status.Type.DONE) {
            throw new ToolException("Something wrong happened running the Samtools stats analysis. Execution status = "
                    + executionResult.getStatus().getName());
        }

        // Check results
        Path statsFile = AlignmentStatsAnalysis.getResultPath(outPath.toAbsolutePath().toString(), catalogBamFile.getName());
        java.io.File stdoutFile = outPath.resolve(DockerWrapperAnalysisExecutor.STDOUT_FILENAME).toFile();
        List<String> lines ;
        try {
            lines = readLines(stdoutFile, Charset.defaultCharset());
        } catch (IOException e) {
            throw new ToolException("Error reading running samtools-stats results", e);
        }
        if (CollectionUtils.isNotEmpty(lines) && lines.get(0).startsWith("# This file was produced by samtools stats")) {
            try {
            FileUtils.copyFile(stdoutFile, statsFile.toFile());
            } catch (IOException e) {
                throw new ToolException("Error copying Samtools stats results", e);
            }
        } else {
            String msg = DockerWrapperAnalysisExecutor.getStdErrMessage("Something wrong happened running Samtools stats analysis.",
                    outPath);
            throw new ToolException(msg);
        }

        // Check results and update QC file
        SamtoolsStats samtoolsStats;
        try {
            samtoolsStats = SamtoolsWrapperAnalysis.parseSamtoolsStats(statsFile.toFile());
        } catch (IOException e) {
            throw new ToolException("Error parsing Samtools stats results", e);
        }

        // Link the stats file to the OpenCGA catalog to be used by the plot-batmstats later
        try {
            String path;
            if (outPath.startsWith(configuration.getJobDir())) {
                path = outPath.toString().substring(configuration.getJobDir().length() + 1);
            } else {
                path = outPath.toString();
                logger.warn("Using path {} to link {} to OpenCGA catalog", outPath, catalogStatsFile.getName());
            }
            catalogStatsFile = catalogManager.getFileManager().link(study, new FileLinkParams(statsFile.toUri().toString(), path, "", "",
                    null, null, null, null, null), true, token).first();
        } catch (CatalogException e) {
            throw new ToolException("Error linking the Samtools stats results to OpenCGA catalog", e);
        }

        fileQc.getAlignment().setSamtoolsStats(samtoolsStats);
    }

    private void runPlotBamstats() throws ToolException {
        Path outPath = getOutDir().resolve(PLOT_BAMSTATS_STEP);
        try {
            FileUtils.forceMkdir(outPath.toFile());
        } catch (IOException e) {
            throw new ToolException("Error creating plot-bamstats output folder: " + outPath, e);
        }

        // Prepare parameters
        SamtoolsWrapperParams samtoolsWrapperParams = new SamtoolsWrapperParams("plot-bamstats", catalogStatsFile.getId(), null,
                new HashMap<>());

        // Execute the plot-bamstats analysis and add its step attributes if exist
        ExecutionResult executionResult = toolRunner.execute(SamtoolsWrapperAnalysis.class, study, samtoolsWrapperParams, outPath,
                null, token);
        addStepAttributes(executionResult);

        // Check execution status
        if (executionResult.getStatus().getName() != Status.Type.DONE) {
            throw new ToolException("Something wrong happened running the plot-bamstats analysis. Execution status = "
                    + executionResult.getStatus().getName());
        }

        // Add images from plot-bamstats to the QC alignment
        List<String> images = new ArrayList<>();
        for (java.io.File file : outPath.toFile().listFiles()) {
            if (file.getName().endsWith("png")) {
                // Sanity check
                if (!file.getAbsolutePath().startsWith(configuration.getJobDir())) {
                    throw new ToolException("plot-bamstats image is not in the configuration job folder "+ configuration.getJobDir());
                }
                images.add(file.getAbsolutePath().substring(configuration.getJobDir().length() + 1));
            }
        }
        fileQc.getAlignment().getSamtoolsStats().setFiles(images);
    }

    private void runFastqcMetrics() throws ToolException {
        Path outPath = getOutDir().resolve(FASTQC_METRICS_STEP);
        try {
            FileUtils.forceMkdir(outPath.toFile());
        } catch (IOException e) {
            throw new ToolException("Error creating FastQC output folder: " + outPath, e);
        }

        // Prepare parameters
        Map<String, String> fastQcParams = new HashMap<>();
        fastQcParams.put("extract", "true");
        FastqcWrapperParams fastqcWrapperParams = new FastqcWrapperParams(catalogBamFile.getId(), null, fastQcParams);

        // Execute the FastQC analysis and add its step attributes if exist
        ExecutionResult executionResult = toolRunner.execute(FastqcWrapperAnalysis.class, study, fastqcWrapperParams, outPath, null, token);
        addStepAttributes(executionResult);

        // Check execution status
        if (executionResult.getStatus().getName() != Status.Type.DONE) {
            throw new ToolException("Something wrong happened running the FastQC analysis. Execution status = "
                    + executionResult.getStatus().getName());
        }

        // Check results and update QC file
        FastQcMetrics fastQcMetrics = AlignmentFastQcMetricsAnalysis.parseResults(outPath, configuration.getJobDir());
        fileQc.getAlignment().setFastQcMetrics(fastQcMetrics);
    }

    private void updateAlignmentQc() throws ToolException {
        // Finally, update file quality control
        try {
            FileUpdateParams fileUpdateParams = new FileUpdateParams().setQualityControl(fileQc);
            catalogManager.getFileManager().update(study, catalogBamFile.getId(), fileUpdateParams, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new ToolException("Error updating alignment quality control", e);
        }
    }
}
