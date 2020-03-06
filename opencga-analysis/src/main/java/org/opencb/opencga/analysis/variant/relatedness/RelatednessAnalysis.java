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

package org.opencb.opencga.analysis.variant.relatedness;

import com.nimbusds.oauth2.sdk.util.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.variant.IBDRelatednessAnalysisExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.util.*;

@Tool(id = RelatednessAnalysis.ID, resource = Enums.Resource.VARIANT, description = RelatednessAnalysis.DESCRIPTION)
public class RelatednessAnalysis extends OpenCgaTool {

    public static final String ID = "relatedness";
    public static final String DESCRIPTION = "Compute a score to quantify relatedness between samples.";

    private String studyId;
    private List<String> individualIds;
    private List<String> sampleIds;
    private String method;
    private String minorAlleleFreq;

    // Internal memeber
    private List<String> validSampleIds;

    public RelatednessAnalysis() {
    }

    /**
     * Study of the samples.
     * @param studyId Study id
     * @return this
     */
    public RelatednessAnalysis setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public List<String> getIndividualIds() {
        return individualIds;
    }

    public RelatednessAnalysis setIndividualIds(List<String> individualIds) {
        this.individualIds = individualIds;
        return this;
    }

    public List<String> getSampleIds() {
        return sampleIds;
    }

    public RelatednessAnalysis setSampleIds(List<String> sampleIds) {
        this.sampleIds = sampleIds;
        return this;
    }

    public String getMethod() {
        return method;
    }

    public RelatednessAnalysis setMethod(String method) {
        this.method = method;
        return this;
    }

    public String getMinorAlleleFreq() {
        return minorAlleleFreq;
    }

    public RelatednessAnalysis setMinorAlleleFreq(String maf) {
        this.minorAlleleFreq = maf;
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

        // Check individuals and samples
        validSampleIds = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(individualIds) && CollectionUtils.isNotEmpty(sampleIds)) {
            throw new ToolException("Incorrect parameters: only a list of individuals or samples is allowed.");
        }

        if (CollectionUtils.isNotEmpty(individualIds)) {
            // A list of individual ID is provided
            for (String individualId : individualIds) {
                OpenCGAResult<Individual> individualResult = catalogManager.getIndividualManager().get(studyId, individualId,
                        QueryOptions.empty(), token);
                if (individualResult.getNumResults() == 0) {
                    throw new ToolException("Individual not found for ID '" + individualId + "'.");
                }
                if (individualResult.getNumResults() > 1) {
                    throw new ToolException("More than one individual found for ID '" + individualId + "'.");
                }

                String validSampleId = "";
                Query query = new Query();
                query.put("individual", individualId);
                OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().search(studyId, query, QueryOptions.empty(), token);
                for (Sample individualSample : sampleResult.getResults()) {
                    if (Status.READY.equals(individualSample.getInternal().getStatus().getName())) {
                        if (StringUtils.isNotEmpty(validSampleId)) {
                            throw new ToolException("More than one valid sample found for individual '" + individualId + "'.");
                        }
                        validSampleId = individualSample.getId();
                    }
                }
                if (StringUtils.isEmpty(validSampleId)) {
                    throw new ToolException("Samples not found for individual '" + individualId + "'");
                }
                // Add valid sample ID to the list
                validSampleIds.add(validSampleId);
            }
        } else {
            // A list of sample IDs is provided
            for (String sampleId : sampleIds) {
                OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(studyId, sampleId, QueryOptions.empty(), token);
                if (sampleResult.getNumResults() == 0) {
                    throw new ToolException("Sample not found for ID '" + sampleId + "'.");
                }
                if (sampleResult.getNumResults() > 1) {
                    throw new ToolException("More than one sample found for ID '" + sampleId + "'.");
                }
                if (Status.READY.equals(sampleResult.first().getInternal().getStatus())) {
                    throw new ToolException("Sample '" + sampleId + "' is not valid: its status must be READY.");
                }
            }
            validSampleIds = sampleIds;
        }
    }

    @Override
    protected void run() throws ToolException {

        step("relatedness", () -> {
            IBDRelatednessAnalysisExecutor relatednessExecutor = getToolExecutor(IBDRelatednessAnalysisExecutor.class);

            relatednessExecutor.setStudyId(studyId)
                    .setSampleIds(validSampleIds)
                    .setMinorAlleleFreq(minorAlleleFreq)
                    .execute();
        });
    }
}
