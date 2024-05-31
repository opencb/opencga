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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsFlagstats;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsStats;
import org.opencb.biodata.formats.sequence.fastqc.FastQcMetrics;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.fastqc.FastqcWrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.samtools.SamtoolsWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.samtools.SamtoolsWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.alignment.AlignmentQcParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileQualityControl;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.core.tools.result.ExecutionResultManager;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.commons.io.FileUtils.readLines;
import static org.opencb.opencga.core.api.ParamConstants.ALIGNMENT_QC_DESCRIPTION;
import static org.opencb.opencga.core.tools.OpenCgaToolExecutor.EXECUTOR_ID;

@Tool(id = AlignmentQcAnalysis.ID, resource = Enums.Resource.ALIGNMENT)
public class AlignmentQcAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "alignment-qc";
    public static final String DESCRIPTION = ALIGNMENT_QC_DESCRIPTION;

    public static final String SAMTOOLS_STATS_STEP = "samtools-stats";
    public static final String SAMTOOLS_FLAGSTATS_STEP = "samtools-flagstats";
    private static final String PLOT_BAMSTATS_STEP = "plot-bamstats";
    public static final String FASTQC_METRICS_STEP = "fastqc-metrics";

    @ToolParams
    protected final AlignmentQcParams analysisParams = new AlignmentQcParams();

    private File catalogBamFile;
    private FileQualityControl fileQc = null;

    @Override
    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study");
        }

        try {
            catalogBamFile = AnalysisUtils.getCatalogFile(analysisParams.getBamFile(), study, catalogManager.getFileManager(), token);
        } catch (CatalogException e) {
            throw new ToolException("Error accessing to the BAM file '" + analysisParams.getBamFile() + "'", e);
        }
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(SAMTOOLS_STATS_STEP, SAMTOOLS_FLAGSTATS_STEP, PLOT_BAMSTATS_STEP, FASTQC_METRICS_STEP);
    }

    @Override
    protected void run() throws ToolException {
        // Get alignment QC metrics to update
        if (catalogBamFile.getQualityControl() != null) {
            fileQc = catalogBamFile.getQualityControl();
        }
        if (fileQc == null) {
            fileQc = new FileQualityControl();
        }

        step(SAMTOOLS_FLAGSTATS_STEP, this::runSamtoolsFlagStats);
        step(SAMTOOLS_STATS_STEP, this::runSamtoolsStats);
        step(PLOT_BAMSTATS_STEP, this::runPlotBamStats);
        step(FASTQC_METRICS_STEP, this::runFastQcMetrics);

        // Finally, update file quality control
        try {
            FileUpdateParams fileUpdateParams = new FileUpdateParams().setQualityControl(fileQc);
            catalogManager.getFileManager().update(study, catalogBamFile.getId(), fileUpdateParams, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        // Unset the executor info since it is executed by different executors, it will be indicated in the
        // tool step attributes
        getErm().setExecutorInfo(null);
    }

    private void runSamtoolsFlagStats() throws ToolException {
        Path outPath = getOutDir().resolve(SAMTOOLS_FLAGSTATS_STEP);
        try {
            FileUtils.forceMkdir(outPath.toFile());
        } catch (IOException e) {
            throw new ToolException("Error creating SAMtools flagstat output folder: " + outPath, e);
        }

        SamtoolsWrapperAnalysisExecutor executor = getToolExecutor(SamtoolsWrapperAnalysisExecutor.class,
                SamtoolsWrapperAnalysisExecutor.ID)
                .setCommand("flagstat")
                .setInputFile(catalogBamFile.getUri().getPath());

        ExecutionResultManager erm = new ExecutionResultManager(SamtoolsWrapperAnalysisExecutor.ID, outPath);
        ObjectMap params = new ObjectMap();
        executor.setUp(erm, params, outPath);

        // Execute
        executor.execute();
        getErm().addStepAttribute("CLI", executor.getCommandLine());

        // Check results and update QC file
        Path flagStatsFile = AlignmentFlagStatsAnalysis.getResultPath(outPath.toAbsolutePath().toString(), catalogBamFile.getName());
        java.io.File stdoutFile = outPath.resolve(DockerWrapperAnalysisExecutor.STDOUT_FILENAME).toFile();
        List<String> lines ;
        try {
            lines = readLines(stdoutFile, Charset.defaultCharset());
        } catch (IOException e) {
            throw new ToolException("Error reading running samtools-flagstat results.", e);
        }
        if (lines.size() > 0 && lines.get(0).contains("QC-passed")) {
            try {
                FileUtils.copyFile(stdoutFile, flagStatsFile.toFile());
            } catch (IOException e) {
                throw new ToolException("Error copying samtools-flagstat results.", e);
            }
        } else {
            throw new ToolException("Something wrong happened running samtools-flagstat.");
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

        SamtoolsWrapperAnalysisExecutor executor = getToolExecutor(SamtoolsWrapperAnalysisExecutor.class,
                SamtoolsWrapperAnalysisExecutor.ID)
                .setCommand("stats")
                .setInputFile(catalogBamFile.getUri().getPath());

        ExecutionResultManager erm = new ExecutionResultManager(SamtoolsWrapperAnalysisExecutor.ID, outPath);
        ObjectMap params = new ObjectMap();
        params.put(EXECUTOR_ID, SamtoolsWrapperAnalysisExecutor.ID);
        // Filter flag:
        //   - not primary alignment (0x100)
        //   - read fails platform/vendor quality checks (0x200)
        //   - supplementary alignment (0x800)
        params.put("F", "0xB00");
        executor.setUp(erm, params, outPath);

        // Execute
        executor.execute();
        getErm().addStepAttribute("CLI", executor.getCommandLine());

        // Check results
        Path statsFile = AlignmentStatsAnalysis.getResultPath(outPath.toAbsolutePath().toString(), catalogBamFile.getName());
        java.io.File stdoutFile = outPath.resolve(DockerWrapperAnalysisExecutor.STDOUT_FILENAME).toFile();
        List<String> lines ;
        try {
            lines = readLines(stdoutFile, Charset.defaultCharset());
        } catch (IOException e) {
            throw new ToolException("Error reading running samtools-stats results.", e);
        }
        if (lines.size() > 0 && lines.get(0).startsWith("# This file was produced by samtools stats")) {
            try {
            FileUtils.copyFile(stdoutFile, statsFile.toFile());
            } catch (IOException e) {
                throw new ToolException("Error copying samtools-stats results.", e);
            }
        } else {
            throw new ToolException("Something wrong happened running samtools-stats.");
        }

        // Check results and update QC file
        SamtoolsStats samtoolsStats;
        try {
            samtoolsStats = SamtoolsWrapperAnalysis.parseSamtoolsStats(statsFile.toFile());
        } catch (IOException e) {
            throw new ToolException("Error parsing samtools-stats results.");
        }
        fileQc.getAlignment().setSamtoolsStats(samtoolsStats);
    }

    private void runPlotBamStats() throws ToolException {
        Path outPath = getOutDir().resolve(PLOT_BAMSTATS_STEP);
        try {
            FileUtils.forceMkdir(outPath.toFile());
        } catch (IOException e) {
            throw new ToolException("Error creating plot-bamstats output folder: " + outPath, e);
        }

        Path statsFile = AlignmentStatsAnalysis.getResultPath(getOutDir().resolve(SAMTOOLS_STATS_STEP).toString(),
                catalogBamFile.getName());
        SamtoolsWrapperAnalysisExecutor executor = getToolExecutor(SamtoolsWrapperAnalysisExecutor.class,
                SamtoolsWrapperAnalysisExecutor.ID)
                .setCommand("plot-bamstats")
                .setInputFile(statsFile.toString());

        ExecutionResultManager erm = new ExecutionResultManager(SamtoolsWrapperAnalysisExecutor.ID, outPath);
        ObjectMap params = new ObjectMap();
        params.put(EXECUTOR_ID, SamtoolsWrapperAnalysisExecutor.ID);
        executor.setUp(erm, params, outPath);

        // Execute
        executor.execute();
        getErm().addStepAttribute("CLI", executor.getCommandLine());

        // Check results and update QC file
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

    private void runFastQcMetrics() throws ToolException {
        Path outPath = getOutDir().resolve(FASTQC_METRICS_STEP);
        try {
            FileUtils.forceMkdir(outPath.toFile());
        } catch (IOException e) {
            throw new ToolException("Error creating FastQC output folder: " + outPath, e);
        }

        FastqcWrapperAnalysisExecutor executor = getToolExecutor(FastqcWrapperAnalysisExecutor.class, FastqcWrapperAnalysisExecutor.ID)
                .setInputFile(catalogBamFile.getUri().getPath());

        ExecutionResultManager erm = new ExecutionResultManager(FastqcWrapperAnalysisExecutor.ID, outPath);
        ObjectMap params = new ObjectMap();
        params.put(EXECUTOR_ID, FastqcWrapperAnalysisExecutor.ID);
        params.put("extract", "true");
        executor.setUp(erm, params, outPath);

        // Execute
        executor.execute();
        getErm().addStepAttribute("CLI", executor.getCommandLine());

        // Check results and update QC file
        FastQcMetrics fastQcMetrics = AlignmentFastQcMetricsAnalysis.parseResults(outPath, configuration.getJobDir());
        fileQc.getAlignment().setFastQcMetrics(fastQcMetrics);
    }
}
