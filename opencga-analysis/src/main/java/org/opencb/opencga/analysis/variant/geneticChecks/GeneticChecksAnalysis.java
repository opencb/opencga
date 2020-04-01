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

package org.opencb.opencga.analysis.variant.geneticChecks;

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
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.variant.GeneticChecksReport;
import org.opencb.opencga.core.models.variant.InferredSexReport;
import org.opencb.opencga.core.models.variant.MendelianErrorReport.SampleAggregation;
import org.opencb.opencga.core.models.variant.MendelianErrorReport.SampleAggregation.ChromosomeAggregation;
import org.opencb.opencga.core.models.variant.RelatednessReport;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.variant.GeneticChecksAnalysisExecutor;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Tool(id = GeneticChecksAnalysis.ID, resource = Enums.Resource.VARIANT, description = GeneticChecksAnalysis.DESCRIPTION)
public class GeneticChecksAnalysis extends OpenCgaTool {

    public static final String ID = "genetic-checks";
    public static final String DESCRIPTION = "Run genetic checks for sex, relatedness and mendelian errors (UDP).";
    public static final String VARIABLE_SET_ID = "opencga_individual_genetic_checks";

    private String studyId;
    private String familyId;
    private String individualId;
    private String sampleId;
    private String minorAlleleFreq;
    private String relatednessMethod;

    // Internal members
    private List<String> sampleIds;

    public GeneticChecksAnalysis() {
    }

    /**
     * Study of the samples.
     * @param studyId Study id
     * @return this
     */
    public GeneticChecksAnalysis setStudy(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getFamilyId() {
        return familyId;
    }

    public GeneticChecksAnalysis setFamilyId(String familyId) {
        this.familyId = familyId;
        return this;
    }

    public String getIndividualId() {
        return individualId;
    }

    public GeneticChecksAnalysis setIndividualId(String individualId) {
        this.individualId = individualId;
        return this;
    }

    public String getSampleId() {
        return sampleId;
    }

    public GeneticChecksAnalysis setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public String getMinorAlleleFreq() {
        return minorAlleleFreq;
    }

    public GeneticChecksAnalysis setMinorAlleleFreq(String minorAlleleFreq) {
        this.minorAlleleFreq = minorAlleleFreq;
        return this;
    }

    public String getRelatednessMethod() {
        return relatednessMethod;
    }

    public GeneticChecksAnalysis setRelatednessMethod(String relatednessMethod) {
        this.relatednessMethod = relatednessMethod;
        return this;
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
        if (StringUtils.isNotEmpty(familyId) && StringUtils.isNotEmpty(individualId) && StringUtils.isNotEmpty(sampleId)) {
            throw new ToolException("Incorrect parameters: please, provide only a family ID, a individual ID or a sample ID.");
        }

        // Get relatives, i.e., members of a family
        List<Sample> samples;
        if (StringUtils.isNotEmpty(familyId)) {
            // Check and get the individuals from that family ID
            samples = GeneticChecksUtils.getRelativeSamplesByFamilyId(studyId, familyId, catalogManager, token);
        } else if (StringUtils.isNotEmpty(individualId)) {
            // Get father, mother and siblings from that individual
            samples = GeneticChecksUtils.getRelativeSamplesByIndividualId(studyId, individualId, catalogManager, token);
            Family family = GeneticChecksUtils.getFamilyByIndividualId(studyId, individualId, catalogManager, token);
            familyId = family.getId();
        } else if (StringUtils.isNotEmpty(sampleId)) {
            // Get father, mother and siblings from that sample
            samples = GeneticChecksUtils.getRelativeSamplesBySampleId(studyId, individualId, catalogManager, token);
            Family family = GeneticChecksUtils.getFamilyBySampleId(studyId, sampleId, catalogManager, token);
            familyId = family.getId();
        } else {
            throw new ToolException("Missing a family ID, a individual ID or a sample ID.");
        }

        if (CollectionUtils.isEmpty(samples)) {
            throw new ToolException("Member samples not found to execute genetic checks analysis.");
        }

        sampleIds = samples.stream().map(Sample::getId).collect(Collectors.toList());
    }

    @Override
    protected List<String> getSteps() {
        List<String> steps = new ArrayList<>();
        steps.add("sex");
        steps.add("relatedness");
        steps.add("mendelian-errors");
        steps.add("index-variable-set");
        return steps;
    }

    @Override
    protected void run() throws ToolException {

        GeneticChecksAnalysisExecutor executor = getToolExecutor(GeneticChecksAnalysisExecutor.class);

        executor.setStudyId(studyId)
                .setFamilyId(familyId)
                .setSampleIds(sampleIds)
                .setMinorAlleleFreq(minorAlleleFreq)
                .setRelatednessMethod(relatednessMethod);

        step("sex", () -> {
            executor.setGeneticCheck(GeneticChecksAnalysisExecutor.GeneticCheck.INFERRED_SEX).execute();
            GeneticChecksUtils.updateSexReport(executor.getReport().getInferredSexReport(), studyId, catalogManager, token);
        });

        step("relatedness", () -> {
            executor.setGeneticCheck(GeneticChecksAnalysisExecutor.GeneticCheck.RELATEDNESS).execute();
        });

        step("mendelian-errors", () -> {
            executor.setGeneticCheck(GeneticChecksAnalysisExecutor.GeneticCheck.MENDELIAN_ERRORS).execute();
        });

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

    private void indexResults(GeneticChecksReport report) throws ToolException {
        try {
            // Index variable-set for each target individual
            for (String sampleId : sampleIds) {
                // Create annotation set
                ObjectMap annotations = buildAnnotations(report, sampleId);
                AnnotationSet annotationSet = new AnnotationSet(VARIABLE_SET_ID, VARIABLE_SET_ID, annotations);
                IndividualUpdateParams updateParams = new IndividualUpdateParams().setAnnotationSets(Collections.singletonList(annotationSet));

                // Get individual from sample and update
                Individual individual = GeneticChecksUtils.getIndividualBySampleId(studyId, sampleId, catalogManager, token);
                catalogManager.getIndividualManager().update(studyId, individual.getId(), updateParams, QueryOptions.empty(), token);
            }
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
    }

    private ObjectMap buildAnnotations(GeneticChecksReport report, String sampleId) throws ToolException {
        ObjectMap annot = new ObjectMap();

        // Get individual from sample and update
        try {
            Individual individual = GeneticChecksUtils.getIndividualBySampleId(studyId, sampleId, catalogManager, token);
            if (individual != null) {
                annot.put("individual", individual.getId());
                annot.put("sample", sampleId);
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
}
