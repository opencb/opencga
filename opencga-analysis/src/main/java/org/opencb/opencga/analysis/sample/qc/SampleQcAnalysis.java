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
import org.opencb.opencga.core.models.variant.SampleQcReport;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.variant.SampleQcAnalysisExecutor;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Tool(id = SampleQcAnalysis.ID, resource = Enums.Resource.SAMPLE, description = SampleQcAnalysis.DESCRIPTION)
public class SampleQcAnalysis extends OpenCgaTool {

    public static final String ID = "sample-qc";
    public static final String DESCRIPTION = "Run quality control (QC) for a given sample. It includes inferred sex, relatedness," +
            " mendelian errors (UDP), FastQC, samtools/flagstat and picard/CollectHsMetrics";
    public static final String VARIABLE_SET_ID = "opencga_sample_qc";

    public  static final String INFERRED_SEX_STEP = "inferred-sex";
    public  static final String RELATEDNESS_STEP = "relatedness";
    public  static final String MENDELIAN_ERRORS_STEP = "mendelian-errors";
    public  static final String FASTQC_STEP = "fastqc";
    public  static final String HS_METRICS_STEP = "hs-metrics";
    public  static final String FLAG_STATS_STEP = "flag-stats";
    public  static final String INDEX_STEP = "index-variable-set";

    private String studyId;
    private String sampleId;
    private String fastaFilename;
    private String bamFilename;
    private String baitFilename;
    private String targetFilename;
    private String minorAlleleFreq;
    private String relatednessMethod;

    // Internal members
    private List<String> sampleIds;

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

        // Get relatives, i.e., members of a family
        List<Sample> samples = SampleQcUtils.getRelativeSamplesBySampleId(studyId, sampleId, catalogManager, token);
        if (CollectionUtils.isNotEmpty(samples)) {
            sampleIds = samples.stream().map(Sample::getId).collect(Collectors.toList());
        }
    }

    @Override
    protected List<String> getSteps() {
        List<String> steps = new ArrayList<>();
        if (canRunInferredSex()) {
            steps.add(INFERRED_SEX_STEP);
        }
        if (canRunRelatedness()) {
            steps.add(RELATEDNESS_STEP);
        }
        if (canRunMendelianErrors()) {
            steps.add(MENDELIAN_ERRORS_STEP);
        }
        if (canRunFastQc()) {
            steps.add(FASTQC_STEP);
        }
        if (canRunHsMetrics()) {
            steps.add(HS_METRICS_STEP);
        }
        if (canRunFlagStats()) {
            steps.add(FLAG_STATS_STEP);
        }
        steps.add(INDEX_STEP);
        return steps;
    }

    @Override
    protected void run() throws ToolException {

        SampleQcAnalysisExecutor executor = getToolExecutor(SampleQcAnalysisExecutor.class);

        executor.setStudyId(studyId)
                .setSampleIds(sampleIds)
                .setMinorAlleleFreq(minorAlleleFreq)
                .setRelatednessMethod(relatednessMethod);

        if (canRunInferredSex()) {
            step(INFERRED_SEX_STEP, () -> {
                executor.setQc(SampleQcAnalysisExecutor.Qc.INFERRED_SEX).execute();
                SampleQcUtils.updateSexReport(executor.getReport().getInferredSexReport(), studyId, catalogManager, token);
            });
        } else {
            getErm().addWarning("Skipping step " + INFERRED_SEX_STEP + ": you need to provide a BAM file");
        }

        if (canRunRelatedness()) {
            step(RELATEDNESS_STEP, () -> {
                executor.setQc(SampleQcAnalysisExecutor.Qc.RELATEDNESS).execute();
            });
        } else {
            getErm().addWarning("Skipping step " + RELATEDNESS_STEP + ": no members found for the sample family");
        }

        if (canRunMendelianErrors()) {
            step(MENDELIAN_ERRORS_STEP, () -> {
                executor.setQc(SampleQcAnalysisExecutor.Qc.MENDELIAN_ERRORS).execute();
            });
        } else {
            getErm().addWarning("Skipping step " + MENDELIAN_ERRORS_STEP + ": father and mother must exist for sample " + sampleId);
        }

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

        // Index as a variable set
        step("index-variable-set", () -> {
            indexResults(executor.getReport());
        });
    }

    private void indexResults(SampleQcReport report) throws ToolException {
        try {
            // Index variable-set for each target individual
            for (String sampleId : sampleIds) {
                // Create annotation set
                ObjectMap annotations = buildAnnotations(report, sampleId);
                AnnotationSet annotationSet = new AnnotationSet(VARIABLE_SET_ID, VARIABLE_SET_ID, annotations);
                SampleUpdateParams updateParams = new SampleUpdateParams().setAnnotationSets(Collections.singletonList(annotationSet));

                // Get sample from ID and update
                Sample sample = SampleQcUtils.getValidSampleById(studyId, sampleId, catalogManager, token);
                catalogManager.getSampleManager().update(studyId, sample.getId(), updateParams, QueryOptions.empty(), token);
            }
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
    }

    private ObjectMap buildAnnotations(SampleQcReport report, String sampleId) throws ToolException {
        ObjectMap annot = new ObjectMap();

        // Get individual from sample and update
        try {
            annot.put("sample", sampleId);
            Individual individual = SampleQcUtils.getIndividualBySampleId(studyId, sampleId, catalogManager, token);
            if (individual != null) {
                annot.put("individual", individual.getId());
                if (individual.getFather() != null) {
                    annot.put("father", individual.getFather().getId());
                }
                if (individual.getMother() != null) {
                    annot.put("mother", individual.getMother().getId());
                }
            }
        } catch (ToolException e) {
            throw new ToolException(e);
        }

        // Relatedness annotations
        if (report.getRelatednessReport() != null) {
            ObjectMap relatednessAnnot = new ObjectMap();

            relatednessAnnot.put("method", report.getRelatednessReport().getMethod());
            List<ObjectMap> scoreAnnot = new ArrayList<>();
            for (RelatednessReport.RelatednessScore score : report.getRelatednessReport().getScores()) {
                if (sampleId.equals(score.getSampleId1()) || sampleId.equals(score.getSampleId2())) {
                    scoreAnnot.add(new ObjectMap()
                            .append("sampleId1", score.getSampleId1())
                            .append("sampleId2", score.getSampleId2())
                            .append("reportedRelation", score.getReportedRelation())
                            .append("z0", score.getZ0())
                            .append("z1", score.getZ1())
                            .append("z2", score.getZ2())
                            .append("piHat", score.getPiHat())
                    );
                }
            }
            relatednessAnnot.put("scores", scoreAnnot);
            annot.put("relatednessReport", relatednessAnnot);
        }

        // Mendelian error annotations
        if (report.getMendelianErrorReport() != null) {
            for (SampleAggregation sampleAggregation : report.getMendelianErrorReport().getSampleAggregation()) {
                if (sampleId.equals(sampleAggregation.getSample())) {
                    ObjectMap meAnnot = new ObjectMap();
                    meAnnot.put("numErrors", sampleAggregation.getNumErrors());
                    meAnnot.put("numRatio", sampleAggregation.getRatio());

                    List<ObjectMap> chromAnnot = new ArrayList<>();
                    for (ChromosomeAggregation chromAggregation : sampleAggregation.getChromAggregation()) {
                        chromAnnot.add(new ObjectMap()
                                .append("chromosome", chromAggregation.getChromosome())
                                .append("numErrors", chromAggregation.getNumErrors())
                                .append("errorCodeAggregation", chromAggregation.getErrorCodeAggregation())
                        );
                    }
                    meAnnot.put("chromAggregation", chromAnnot);
                    annot.put("mendelianErrorReport", meAnnot);

                    break;
                }
            }
        }

        // Sex annotations
        if (CollectionUtils.isNotEmpty(report.getInferredSexReport())) {
            for (InferredSexReport inferredSexReport : report.getInferredSexReport()) {
                if (sampleId.equals(inferredSexReport.getSampleId())) {
                    // Found
                    ObjectMap sexAnnot = new ObjectMap();
                    sexAnnot.put("reportedSex", inferredSexReport.getReportedSex());
                    sexAnnot.put("reportedKaryotypicSex", inferredSexReport.getReportedKaryotypicSex());
                    sexAnnot.put("ratioX", inferredSexReport.getRatioX());
                    sexAnnot.put("ratioY", inferredSexReport.getRatioY());
                    sexAnnot.put("inferredKaryotypicSex", inferredSexReport.getInferredKaryotypicSex());

                    annot.put("sexReport", sexAnnot);
                    break;
                }
            }
        }

        return annot;
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

    private boolean canRunMendelianErrors() {
        Individual individual;
        try {
            individual = SampleQcUtils.getIndividualBySampleId(studyId, sampleId, catalogManager, token);
        } catch (ToolException e) {
            return false;
        }
        if (individual.getMother() == null || individual.getFather() == null) {
            return false;
        }
        return true;
    }
    private boolean canRunFastQc() {
        return StringUtils.isEmpty(bamFilename) ? false : true;
    }

    private boolean canRunRelatedness() {
        return CollectionUtils.isEmpty(sampleIds) ? false : true;
    }

    private boolean canRunInferredSex() {
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

    public String getMinorAlleleFreq() {
        return minorAlleleFreq;
    }

    public SampleQcAnalysis setMinorAlleleFreq(String minorAlleleFreq) {
        this.minorAlleleFreq = minorAlleleFreq;
        return this;
    }

    public String getRelatednessMethod() {
        return relatednessMethod;
    }

    public SampleQcAnalysis setRelatednessMethod(String relatednessMethod) {
        this.relatednessMethod = relatednessMethod;
        return this;
    }
}
