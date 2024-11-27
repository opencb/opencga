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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.qc.InferredSexReport;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.alignment.AlignmentConstants;
import org.opencb.opencga.analysis.individual.qc.IndividualQcUtils;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.variant.InferredSexAnalysisExecutor;

import java.io.IOException;
import java.util.List;

@Tool(id = InferredSexAnalysis.ID, resource = Enums.Resource.VARIANT, description = InferredSexAnalysis.DESCRIPTION)
public class InferredSexAnalysis extends OpenCgaTool {

    public static final String ID = "inferred-sex";
    public static final String DESCRIPTION = "Infer sex from chromosome mean coverages.";

    private String studyId;
    private String individualId;
    private String sampleId;

    // Internal members
    private Individual individual;
    private Sample sample;

    public InferredSexAnalysis() {
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

        // Main check (this function is shared with the endpoint individual/qc/run)
        checkParameters(individualId, sampleId, studyId, catalogManager, token);

        // Get individual
        individual = IndividualQcUtils.getIndividualById(studyId, individualId, catalogManager, token);

        // Get samples of that individual, but only germline samples
        sample = null;
        List<Sample> childGermlineSamples = IndividualQcUtils.getValidGermlineSamplesByIndividualId(studyId, individualId, catalogManager,
                token);
        if (CollectionUtils.isEmpty(childGermlineSamples)) {
            throw new ToolException("Germline sample not found for individual '" + individualId + "'");
        }

        if (StringUtils.isNotEmpty(sampleId)) {
            for (Sample germlineSample : childGermlineSamples) {
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
            sample = childGermlineSamples.get(0);
        }
    }

    @Override
    protected void run() throws ToolException {

        step("inferred-sex", () -> {
            InferredSexAnalysisExecutor inferredSexExecutor = getToolExecutor(InferredSexAnalysisExecutor.class);

            inferredSexExecutor.setStudyId(studyId)
                    .setIndividualId(individual.getId())
                    .setSampleId(sample.getId())
                    .execute();

            // Get inferred sex report
            InferredSexReport report = inferredSexExecutor.getInferredSexReport();

            try {
                // Save inferred sex report
                JacksonUtils.getDefaultObjectMapper().writer().writeValue(getOutDir().resolve(ID + ".report.json").toFile(), report);
            } catch (IOException e) {
                throw new ToolException(e);
            }
        });
    }

    public static void checkParameters(String individualId, String sampleId, String studyId, CatalogManager catalogManager, String token) throws ToolException, CatalogException {
        // Check individual and sample
        if (StringUtils.isEmpty(individualId)) {
            throw new ToolException("Missing individual ID");
        }
        Individual individual = IndividualQcUtils.getIndividualById(studyId, individualId, catalogManager, token);

        // Get samples of that individual, but only germline samples
        List<Sample> childGermlineSamples = IndividualQcUtils.getValidGermlineSamplesByIndividualId(studyId, individualId, catalogManager,
                token);
        if (CollectionUtils.isEmpty(childGermlineSamples)) {
            throw new ToolException("Germline sample not found for individual '" + individualId + "'");
        }

        Sample sample = null;
        if (StringUtils.isNotEmpty(sampleId)) {
            for (Sample germlineSample : childGermlineSamples) {
                if (sampleId.equals(germlineSample.getId())) {
                    sample = germlineSample;
                    break;
                }
            }
            if (sample == null) {
                throw new ToolException("The provided sample '" + sampleId + "' not found in the individual '" + individualId + "'");
            }
        } else {
            // Taking the first sample
            sample = childGermlineSamples.get(0);
        }

        // Checking sample file BIGWIG required to compute inferred-sex
        String bwFileId = null;
        for (String fileId : sample.getFileIds()) {
            if (fileId.endsWith(AlignmentConstants.BIGWIG_EXTENSION)) {
                if (bwFileId != null) {
                    throw new ToolException("Multiple BIGWIG files found for individual/sample (" + individual.getId() + "/"
                            + sample.getId() + ")");
                }
                bwFileId = fileId;
            }
        }
        checkSampleFile(bwFileId, "BIGWIG", sample, individual, studyId, catalogManager, token);
    }

    private static void checkSampleFile(String fileId, String label, Sample sample, Individual individual, String studyId,
                                        CatalogManager catalogManager, String token)
            throws ToolException, CatalogException {
        if (StringUtils.isEmpty(fileId)) {
            throw new ToolException("None " + label + " file registered for individual/sample (" + individual.getId() + "/"
                    + sample.getId() + ")");
        } else {
            OpenCGAResult<File> fileResult = catalogManager.getFileManager().get(studyId, fileId, QueryOptions.empty(), token);
            if (fileResult.getNumResults() == 0) {
                throw new ToolException(label + " file ID '" + fileId + "' not found in OpenCGA catalog for individual/sample ("
                        + individual.getId() + "/" + sample.getId() + ")");
            }
        }
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
}
