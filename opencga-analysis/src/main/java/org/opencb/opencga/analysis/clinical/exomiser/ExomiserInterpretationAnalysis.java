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

package org.opencb.opencga.analysis.clinical.exomiser;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.InterpretationAnalysis;
import org.opencb.opencga.analysis.wrappers.exomiser.ExomiserWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;

import java.util.List;

@Tool(id = ExomiserInterpretationAnalysis.ID, resource = Enums.Resource.CLINICAL)
public class ExomiserInterpretationAnalysis extends InterpretationAnalysis {

    public final static String ID = "interpretation-exomiser";
    public static final String DESCRIPTION = "Run exomiser interpretation analysis";

    private String studyId;
    private String clinicalAnalysisId;
    private String sampleId;

    private ClinicalAnalysis clinicalAnalysis;
    private List<DiseasePanel> diseasePanels;

    @Override
    protected InterpretationMethod getInterpretationMethod() {
        return getInterpretationMethod(ID);
    }

    @Override
    protected void check() throws Exception {
        super.check();

        // Check study
        if (StringUtils.isEmpty(studyId)) {
            // Missing study
            throw new ToolException("Missing study ID");
        }

        // Check clinical analysis
        if (StringUtils.isEmpty(clinicalAnalysisId)) {
            throw new ToolException("Missing clinical analysis ID");
        }

        // Get clinical analysis to ckeck proband sample ID, family ID
        OpenCGAResult<ClinicalAnalysis> clinicalAnalysisQueryResult;
        try {
            clinicalAnalysisQueryResult = catalogManager.getClinicalAnalysisManager().get(studyId, clinicalAnalysisId, QueryOptions.empty(),
                    token);
        } catch (
                CatalogException e) {
            throw new ToolException(e);
        }
        if (clinicalAnalysisQueryResult.getNumResults() != 1) {
            throw new ToolException("Clinical analysis " + clinicalAnalysisId + " not found in study " + studyId);
        }

        clinicalAnalysis = clinicalAnalysisQueryResult.first();

        // Check sample from proband
        if (clinicalAnalysis.getProband() == null) {
            throw new ToolException("Missing proband in clinical analysis " + clinicalAnalysisId);
        }
        if (CollectionUtils.isEmpty(clinicalAnalysis.getProband().getSamples())) {
            throw new ToolException("Missing sample for proband " + clinicalAnalysis.getProband().getId() + " in clinical analysis "
                    + clinicalAnalysisId);
        }
        sampleId = clinicalAnalysis.getProband().getSamples().get(0).getId();

        // Check primary
//        checkPrimaryInterpretation(clinicalAnalysis);
//
//        // Check interpretation method
//        checkInterpretationMethod(getInterpretationMethod(ID).getName(), clinicalAnalysis);

        // Update executor params with OpenCGA home and session ID
        setUpStorageEngineExecutor(studyId);
    }

    @Override
    protected void run() throws ToolException {
        step(() -> {

//            new TieringInterpretationAnalysisExecutor()
            getToolExecutor(ExomiserWrapperAnalysisExecutor.class)
                    .setStudyId(studyId)
                    .setSampleId(sampleId)
                    .execute();

            saveInterpretation(studyId, clinicalAnalysis, null);
        });
    }

    public String getStudyId() {
        return studyId;
    }

    public ExomiserInterpretationAnalysis setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getClinicalAnalysisId() {
        return clinicalAnalysisId;
    }

    public ExomiserInterpretationAnalysis setClinicalAnalysisId(String clinicalAnalysisId) {
        this.clinicalAnalysisId = clinicalAnalysisId;
        return this;
    }
}
