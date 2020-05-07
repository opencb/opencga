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

package org.opencb.opencga.analysis.variant.inferredSex;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.analysis.variant.geneticChecks.GeneticChecksUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.variant.InferredSexReport;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.variant.InferredSexAnalysisExecutor;

import java.io.IOException;
import java.util.Collections;

@Tool(id = InferredSexAnalysis.ID, resource = Enums.Resource.VARIANT, description = InferredSexAnalysis.DESCRIPTION)
public class InferredSexAnalysis extends OpenCgaTool {

    public static final String ID = "inferred-sex";
    public static final String DESCRIPTION = "Infer sex from chromosome mean coverages.";

    private String studyId;
    private String individualId;
    private String sampleId;

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
        if (StringUtils.isNotEmpty(individualId) && StringUtils.isNotEmpty(sampleId)) {
            throw new ToolException("Individual and sample are incompatible parameters: please, provide only a individual or a sample.");
        }

        if (StringUtils.isNotEmpty(individualId)) {
            // Check and get sample from individual
            Sample sample = GeneticChecksUtils.getValidSampleByIndividualId(studyId, individualId, catalogManager, token);
            sampleId = sample.getId();
        }

        if (StringUtils.isEmpty(sampleId)) {
            throw new ToolException("Not found sample to execute inferred sex analysis");
        }
    }

    @Override
    protected void run() throws ToolException {

        step("inferred-sex", () -> {
            InferredSexAnalysisExecutor inferredSexExecutor = getToolExecutor(InferredSexAnalysisExecutor.class);

            inferredSexExecutor.setStudyId(studyId)
                    .setSampleId(sampleId)
                    .execute();

            // Get inferred sex report and update with individual info (ID, sex, and karyotypic sex)
            InferredSexReport report = inferredSexExecutor.getInferredSexReport();
            GeneticChecksUtils.updateSexReport(Collections.singletonList(report), studyId, catalogManager, token);

            try {
                // Save inferred sex report
                JacksonUtils.getDefaultObjectMapper().writer().writeValue(getOutDir().resolve(ID + ".report.json").toFile(), report);
            } catch (IOException e) {
                throw new ToolException(e);
            }
        });
    }
}
