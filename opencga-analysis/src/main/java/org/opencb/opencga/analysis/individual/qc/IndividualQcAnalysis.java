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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualQualityControl;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.variant.IndividualQcAnalysisExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Tool(id = IndividualQcAnalysis.ID, resource = Enums.Resource.SAMPLE, description = IndividualQcAnalysis.DESCRIPTION)
public class IndividualQcAnalysis extends OpenCgaTool {

    public static final String ID = "individual-qc";
    public static final String DESCRIPTION = "Run quality control (QC) for a given individual. It includes inferred sex and " +
            " mendelian errors (UDP)";

    public  static final String INFERRED_SEX_STEP = "inferred-sex";
    public  static final String MENDELIAN_ERRORS_STEP = "mendelian-errors";

    private String studyId;
    private String individualId;
    private String sampleId;
    private String inferredSexMethod;

    // Internal members
    private Individual individual;
    private Sample sample;
    private Family family;

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

        // Get individual
        individual = IndividualQcUtils.getIndividualById(studyId, individualId, catalogManager, token);

        // Get samples of that individual, but only germline samples
        sample = null;
        List<Sample> samples = IndividualQcUtils.getValidSamplesByIndividualId(studyId, individualId, catalogManager, token);
        List<Sample> germlineSamples = new ArrayList<>();
        for (Sample individualSample : samples) {
            if (!individualSample.isSomatic()) {
                germlineSamples.add(individualSample);
            }
        }

        if (CollectionUtils.isEmpty(germlineSamples)) {
            throw new ToolException("Germline sample not found for individual '" +  individualId + "'");
        }

        if (germlineSamples.size() > 1) {
            if (StringUtils.isNotEmpty(sampleId)) {
                for (Sample germlineSample : germlineSamples) {
                    if (sampleId.equals(germlineSample.getId())) {
                        sample = germlineSample;
                        break;
                    }
                }
                if (sample == null) {
                    throw new ToolException("The provided sample '" + sampleId + "' not found in the individual '" + individualId + "'");
                }
            } else {
                // If multiple germline samples, we take the first one
                sample = germlineSamples.get(0);
            }
        } else {
            sample = germlineSamples.get(0);
        }

        // Get family (for mendelian error)
        family = IndividualQcUtils.getFamilyByIndividualId(studyId, individualId, catalogManager, token);

        if (StringUtils.isEmpty(inferredSexMethod)) {
            inferredSexMethod = IndividualQcAnalysisExecutor.COVERAGE_RATIO_INFERRED_SEX_METHOD;
        }
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(INFERRED_SEX_STEP, MENDELIAN_ERRORS_STEP);
    }

    @Override
    protected void run() throws ToolException {

        // Get individual quality control metrics to update
        IndividualQualityControl qualityControl = individual.getQualityControl();
        if (qualityControl == null) {
            qualityControl = new IndividualQualityControl().setSampleId(sample.getId());
        } else if (!qualityControl.getSampleId().equals(sample.getId())) {
            throw new ToolException("Individual quality control was computed previously for the sample '" + qualityControl.getSampleId()
                    + "'");
        }

        IndividualQcAnalysisExecutor executor = getToolExecutor(IndividualQcAnalysisExecutor.class);

        executor.setStudyId(studyId)
                .setIndividual(individual)
                .setSample(sample)
                .setFamily(family)
                .setInferredSexMethod(inferredSexMethod)
                .setQualityControl(qualityControl);

        step(INFERRED_SEX_STEP, () -> executor.setQcType(IndividualQcAnalysisExecutor.QcType.INFERRED_SEX).execute());
        step(MENDELIAN_ERRORS_STEP, () -> executor.setQcType(IndividualQcAnalysisExecutor.QcType.MENDELIAN_ERRORS).execute());

        // Finally, update individual quality control
        try {
            qualityControl = executor.getQualityControl();
            if (qualityControl != null) {
                IndividualUpdateParams individualUpdateParams = new IndividualUpdateParams().setQualityControl(qualityControl);
                catalogManager.getIndividualManager().update(getStudyId(), individualId, individualUpdateParams,
                        new QueryOptions(Constants.INCREMENT_VERSION, true), token);
            }
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
    }

    public String getStudyId() {
        return studyId;
    }

    public IndividualQcAnalysis setStudyId(String studyId) {
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

    public String getSampleId() {
        return sampleId;
    }

    public IndividualQcAnalysis setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public String getInferredSexMethod() {
        return inferredSexMethod;
    }

    public IndividualQcAnalysis setInferredSexMethod(String inferredSexMethod) {
        this.inferredSexMethod = inferredSexMethod;
        return this;
    }

    public Individual getIndividual() {
        return individual;
    }

    public IndividualQcAnalysis setIndividual(Individual individual) {
        this.individual = individual;
        return this;
    }

    public Sample getSample() {
        return sample;
    }

    public IndividualQcAnalysis setSample(Sample sample) {
        this.sample = sample;
        return this;
    }

    public Family getFamily() {
        return family;
    }

    public IndividualQcAnalysis setFamily(Family family) {
        this.family = family;
        return this;
    }
}
