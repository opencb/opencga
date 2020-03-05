/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.analysis.variant.inferredSex;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.variant.InferredSexReport;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.variant.InferredSexAnalysisExecutor;

import java.io.IOException;

@Tool(id = InferredSexAnalysis.ID, resource = Enums.Resource.VARIANT, description = InferredSexAnalysis.DESCRIPTION)
public class InferredSexAnalysis extends OpenCgaTool {

    public static final String ID = "inferred-sex";
    public static final String DESCRIPTION = "Infer sex from chromosome mean coverages.";

    private String studyId;
    private String individualId;
    private String sampleId;

    // Internal members
    private Individual individual;
    private String validSampleId;

    public InferredSexAnalysis() {
    }

    /**
     * Study of the samples.
     * @param studyId Study ID
     * @return this
     */
    public InferredSexAnalysis setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getIndividualId() {
        return individualId;
    }

    public InferredSexAnalysis setIndividualId(String individualId) {
        this.individualId = individualId;
        return this;
    }

    public String getSampleId() {
        return sampleId;
    }

    public InferredSexAnalysis setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    @Override
    protected void check() throws Exception {
        super.check();
        setUpStorageEngineExecutor(studyId);

        if (StringUtils.isEmpty(studyId)) {
            throw new ToolException("Missing study.");
        }

        try {
            studyId = catalogManager.getStudyManager().get(studyId, null, token).first().getFqn();
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        // Check individual and sample
        if (StringUtils.isEmpty(individualId) && StringUtils.isEmpty(sampleId)) {
            throw new ToolException("Missing individual and sample. You must provide almost one of them.");
        }

        if (StringUtils.isNotEmpty(individualId)) {
            // Individual ID is provided
            OpenCGAResult<Individual> individualResult = catalogManager.getIndividualManager().get(studyId, individualId,
                    QueryOptions.empty(), token);
            if (individualResult.getNumResults() == 0) {
                throw new ToolException("Not found individual for ID '" + individualId + "'.");
            }
            if (individualResult.getNumResults() > 1) {
                throw new ToolException("More than one individual found for ID '" + individualId + "'.");
            }
            individual = individualResult.first();

            Query query = new Query();
            query.put("individual", individualId);
            OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().search(studyId, query, QueryOptions.empty(), token);
            for (Sample individualSample : sampleResult.getResults()) {
                if (Status.READY.equals(individualSample.getInternal().getStatus())) {
                    if (StringUtils.isNotEmpty(validSampleId)) {
                        throw new ToolException("More than one valid sample found for individual '" + individualId + "'.");
                    }
                    validSampleId = individualSample.getId();
                }
            }
            if (StringUtils.isEmpty(validSampleId)) {
                throw new ToolException("Not found samples for individual '" + individualId + "'");
            }
            if (StringUtils.isNotEmpty(sampleId) && !sampleId.equals(validSampleId)) {
                throw new ToolException("Mismatch sample IDs for individual '" + individualId + "': sample provided '" + sampleId + "'"
                        + " and sample found is '" + validSampleId + "'.");
            }
        } else {
            // Sample ID is provided
            validSampleId = sampleId;
            OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(studyId, sampleId, QueryOptions.empty(), token);
            if (sampleResult.getNumResults() == 0) {
                throw new ToolException("Not found sample for ID '" + sampleId + "'.");
            }
            if (sampleResult.getNumResults() > 1) {
                throw new ToolException("More than one sample found for ID '" + sampleId + "'.");
            }
            if (Status.READY.equals(sampleResult.first().getInternal().getStatus())) {
                throw new ToolException("Sample '" + sampleId + "' is not valid. It must be READY.");
            }

            Query query = new Query();
            query.put("samples", sampleId);
            OpenCGAResult<Individual> individualResult = catalogManager.getIndividualManager().search(studyId, query, QueryOptions.empty(),
                    token);
            if (individualResult.getNumResults() == 0) {
                throw new ToolException("None individual found for sample '" + sampleId + "'.");
            }
            if (individualResult.getNumResults() > 1) {
                throw new ToolException("More than one individual found for sample '" + sampleId + "'.");
            }
            individual = individualResult.first();
            if (StringUtils.isNotEmpty(individualId) && !individualId.equals(individual.getId())) {
                throw new ToolException("Mismatch individual IDs for sample '" + sampleId + "': individual provided '" + individualId + "'"
                        + " and individual found is '" + individual.getId() + "'.");
            }
        }
    }

    @Override
    protected void run() throws ToolException {

        step("inferred-sex", () -> {
            InferredSexAnalysisExecutor inferredSexExecutor = getToolExecutor(InferredSexAnalysisExecutor.class);

            inferredSexExecutor.setStudy(studyId)
                    .setSample(validSampleId)
                    .execute();

            try {
                // Update and save inferred sex report
                InferredSexReport report = inferredSexExecutor.getInferredSexReport();
                report.setReportedSex(individual.getSex().name());
                report.setReportedSex(individual.getKaryotypicSex().name());

                JacksonUtils.getDefaultObjectMapper().writer().writeValue(getOutDir().resolve(ID + ".report.json").toFile(), report);
            } catch (IOException e) {
                throw new ToolException(e);
            }
        });
    }
}
