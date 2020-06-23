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

package org.opencb.opencga.analysis.individual.qc;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.variant.IndividualQcAnalysisExecutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Tool(id = IndividualQcAnalysis.ID, resource = Enums.Resource.SAMPLE, description = IndividualQcAnalysis.DESCRIPTION)
public class IndividualQcAnalysis extends OpenCgaTool {

    public static final String ID = "individual-qc";
    public static final String DESCRIPTION = "Run quality control (QC) for a given individual. It includes inferred sex, relatedness " +
            " and mendelian errors (UDP)";

    public  static final String INFERRED_SEX_STEP = "inferred-sex";
    public  static final String RELATEDNESS_STEP = "relatedness";
    public  static final String MENDELIAN_ERRORS_STEP = "mendelian-errors";

    private String studyId;
    private String individualId;
    private String bamFilename;
    private String minorAlleleFreq;
    private String relatednessMethod;

    // Internal members
    private List<String> sampleIds;

    public IndividualQcAnalysis() {
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
        if (StringUtils.isNotEmpty(individualId)) {
            throw new ToolException("Missing individual ID.");
        }

        // Get relatives, i.e., members of a family
        List<Sample> samples = IndividualQcUtils.getRelativeSamplesByIndividualId(studyId, individualId, catalogManager, token);
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
        return steps;
    }

    @Override
    protected void run() throws ToolException {

        IndividualQcAnalysisExecutor executor = getToolExecutor(IndividualQcAnalysisExecutor.class);

        executor.setStudyId(studyId)
                .setIndividualId(individualId)
                .setSampleIds(sampleIds)
                .setMinorAlleleFreq(minorAlleleFreq)
                .setRelatednessMethod(relatednessMethod);

        if (canRunInferredSex()) {
            step(INFERRED_SEX_STEP, () -> {
                executor.setQc(IndividualQcAnalysisExecutor.Qc.INFERRED_SEX).execute();
            });
        } else {
            getErm().addWarning("Skipping step " + INFERRED_SEX_STEP + ": you need to provide a BAM file");
        }

        if (canRunRelatedness()) {
            step(RELATEDNESS_STEP, () -> {
                executor.setQc(IndividualQcAnalysisExecutor.Qc.RELATEDNESS).execute();
            });
        } else {
            getErm().addWarning("Skipping step " + RELATEDNESS_STEP + ": no members found for the sample family");
        }

        if (canRunMendelianErrors()) {
            step(MENDELIAN_ERRORS_STEP, () -> {
                executor.setQc(IndividualQcAnalysisExecutor.Qc.MENDELIAN_ERRORS).execute();
            });
        } else {
            getErm().addWarning("Skipping step " + MENDELIAN_ERRORS_STEP + ": father and mother must exist for individual " + individualId);
        }

        // Save results
        try {
            JacksonUtils.getDefaultObjectMapper().writer().writeValue(getOutDir().resolve(ID + ".report.json").toFile(),
                    executor.getReport());
        } catch (IOException e) {
            throw new ToolException(e);
        }
    }

    private boolean canRunMendelianErrors() {
        Individual individual;
        try {
            individual = IndividualQcUtils.getIndividualById(studyId, individualId, catalogManager, token);
        } catch (ToolException e) {
            return false;
        }
        if (individual.getMother() == null || individual.getFather() == null) {
            return false;
        }
        return true;
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
    public IndividualQcAnalysis setStudy(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getIndividualId() {
        return individualId;
    }

    public IndividualQcAnalysis setIndividualId(String individualId) {
        this.individualId = individualId;
        return this;
    }

    public String getBamFilename() {
        return bamFilename;
    }

    public IndividualQcAnalysis setBamFilename(String bamFilename) {
        this.bamFilename = bamFilename;
        return this;
    }

    public String getMinorAlleleFreq() {
        return minorAlleleFreq;
    }

    public IndividualQcAnalysis setMinorAlleleFreq(String minorAlleleFreq) {
        this.minorAlleleFreq = minorAlleleFreq;
        return this;
    }

    public String getRelatednessMethod() {
        return relatednessMethod;
    }

    public IndividualQcAnalysis setRelatednessMethod(String relatednessMethod) {
        this.relatednessMethod = relatednessMethod;
        return this;
    }
}
