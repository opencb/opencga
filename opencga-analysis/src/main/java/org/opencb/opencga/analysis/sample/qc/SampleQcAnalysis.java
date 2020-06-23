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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.*;
import org.opencb.opencga.core.models.variant.InferredSexReport;
import org.opencb.opencga.core.models.variant.MendelianErrorReport.SampleAggregation;
import org.opencb.opencga.core.models.variant.MendelianErrorReport.SampleAggregation.ChromosomeAggregation;
import org.opencb.opencga.core.models.sample.SampleQualityControl;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.variant.SampleQcAnalysisExecutor;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Tool(id = SampleQcAnalysis.ID, resource = Enums.Resource.SAMPLE, description = SampleQcAnalysis.DESCRIPTION)
public class SampleQcAnalysis extends OpenCgaTool {

    public static final String ID = "sample-qc";
    public static final String DESCRIPTION = "Run quality control (QC) for a given sample. It includes FastQC, samtools/flagstat and"
        + " picard/CollectHsMetrics";

    public  static final String FASTQC_STEP = "fastqc";
    public  static final String HS_METRICS_STEP = "hs-metrics";
    public  static final String FLAG_STATS_STEP = "flag-stats";

    private String studyId;
    private String sampleId;
    private String fastaFilename;
    private String bamFilename;
    private String baitFilename;
    private String targetFilename;

    public SampleQcAnalysis() {
    }

    @Override
    protected void check() throws Exception {
        super.check();
        setUpStorageEngineExecutor(studyId);

        if (StringUtils.isEmpty(studyId)) {
            throw new ToolException("Missing study ID.");
        }

        try {
            studyId = catalogManager.getStudyManager().get(studyId, null, token).first().getFqn();
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        // Sanity check
        if (StringUtils.isNotEmpty(sampleId)) {
            throw new ToolException("Missing sample ID.");
        }
    }

    @Override
    protected List<String> getSteps() {
        List<String> steps = new ArrayList<>();
        if (canRunFastQc()) {
            steps.add(FASTQC_STEP);
        }
        if (canRunHsMetrics()) {
            steps.add(HS_METRICS_STEP);
        }
        if (canRunFlagStats()) {
            steps.add(FLAG_STATS_STEP);
        }
        return steps;
    }

    @Override
    protected void run() throws ToolException {

        SampleQcAnalysisExecutor executor = getToolExecutor(SampleQcAnalysisExecutor.class);

        executor.setStudyId(studyId);

        if (canRunFastQc()) {
            step(FASTQC_STEP, () -> {
                executor.setQc(SampleQcAnalysisExecutor.Qc.FASTQC).execute();
            });
        } else {
            getErm().addWarning("Skipping step " + FASTQC_STEP + ": you need to provide a BAM file");
        }

        if (canRunFlagStats()) {
            step(FLAG_STATS_STEP, () -> {
                executor.setQc(SampleQcAnalysisExecutor.Qc.FLAG_STATS).execute();
            });
        } else {
            getErm().addWarning("Skipping step " + FLAG_STATS_STEP + ": you need to provide a BAM file");
        }

        if (canRunHsMetrics()) {
            step(HS_METRICS_STEP, () -> {
                executor.setQc(SampleQcAnalysisExecutor.Qc.HS_METRICS).execute();
            });
        } else {
            getErm().addWarning("Skipping step " + HS_METRICS_STEP + ": you need to provide a BAM file, a FASTA file and two BED files," +
                    " one for the bait intervals and one for the target intervals");
        }

        // Save results
        try {
            JacksonUtils.getDefaultObjectMapper().writer().writeValue(getOutDir().resolve(ID + ".report.json").toFile(),
                    executor.getReport());
        } catch (IOException e) {
            throw new ToolException(e);
        }
    }

    private boolean canRunFlagStats() {
        return StringUtils.isEmpty(bamFilename) ? false : true;
    }

    private boolean canRunHsMetrics() {
        if (StringUtils.isEmpty(bamFilename) || StringUtils.isEmpty(fastaFilename) || StringUtils.isEmpty(baitFilename)
                || StringUtils.isEmpty(targetFilename)) {
            return false;
        }
        return true;
    }

    private boolean canRunFastQc() {
        return StringUtils.isEmpty(bamFilename) ? false : true;
    }

    /**
     * Study of the samples.
     * @param studyId Study id
     * @return this
     */
    public SampleQcAnalysis setStudy(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getSampleId() {
        return sampleId;
    }

    public SampleQcAnalysis setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public String getFastaFilename() {
        return fastaFilename;
    }

    public SampleQcAnalysis setFastaFilename(String fastaFilename) {
        this.fastaFilename = fastaFilename;
        return this;
    }

    public String getBamFilename() {
        return bamFilename;
    }

    public SampleQcAnalysis setBamFilename(String bamFilename) {
        this.bamFilename = bamFilename;
        return this;
    }

    public String getBaitFilename() {
        return baitFilename;
    }

    public SampleQcAnalysis setBaitFilename(String baitFilename) {
        this.baitFilename = baitFilename;
        return this;
    }

    public String getTargetFilename() {
        return targetFilename;
    }

    public SampleQcAnalysis setTargetFilename(String targetFilename) {
        this.targetFilename = targetFilename;
        return this;
    }
}
