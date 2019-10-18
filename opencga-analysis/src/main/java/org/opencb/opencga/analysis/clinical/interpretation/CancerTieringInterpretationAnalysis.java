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

package org.opencb.opencga.analysis.clinical.interpretation;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.core.annotations.Analysis;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;


@Analysis(id = CancerTieringInterpretationAnalysis.ID, data = Analysis.AnalysisData.CLINICAL)
public class CancerTieringInterpretationAnalysis extends InterpretationAnalysis {

    public final static String ID = "TieringInterpretationAnalysis";

    private ClinicalAnalysis clinicalAnalysis;
    private List<String> variantIdsToDiscard;
    private CancerTieringInterpretationConfiguration config;

    public CancerTieringInterpretationAnalysis(String clinicalAnalysisId, String studyId, List<String> variantIdsToDiscard, Path outDir,
                                               Path openCgaHome, CancerTieringInterpretationConfiguration config, String sessionId) {
        super(clinicalAnalysisId, studyId, outDir, openCgaHome, sessionId);

        this.variantIdsToDiscard = variantIdsToDiscard;
        this.config = config;
    }

    @Override
    protected void exec() throws org.opencb.oskar.analysis.exceptions.AnalysisException {
        check();

        // Set executor parameters
        updateExecutorParams();

        // Get executor
        CancerTieringInterpretationAnalysisExecutor executor = new CancerTieringInterpretationAnalysisExecutor();
        executor.setup(clinicalAnalysisId, studyId, variantIdsToDiscard, outDir, executorParams, config);

        arm.startStep("get-primary/secondary-findings");
        executor.exec();
        arm.endStep(90.0F);

        arm.startStep("save-interpretation");
        saveResult(ID, new ArrayList<>(), clinicalAnalysis, new Query(), config.isIncludeLowCoverage(), config.getMaxLowCoverage());
        arm.endStep(100.0F);
    }

    protected void check() throws AnalysisException {
        // Check study
        if (StringUtils.isEmpty(studyId)) {
            // Missing study
            throw new AnalysisException("Missing study ID");
        }


        // Check clinical analysis
        if (StringUtils.isEmpty(clinicalAnalysisId)) {
            throw new AnalysisException("Missing clinical analysis ID");
        }

        // Get clinical analysis to ckeck proband sample ID, family ID
        QueryResult<ClinicalAnalysis> clinicalAnalysisQueryResult;
        try {
            clinicalAnalysisQueryResult = catalogManager.getClinicalAnalysisManager().get(studyId, clinicalAnalysisId, QueryOptions.empty(),
                    sessionId);
        } catch (
                CatalogException e) {
            throw new AnalysisException(e);
        }
        if (clinicalAnalysisQueryResult.getNumResults() != 1) {
            throw new AnalysisException("Clinical analysis " + clinicalAnalysisId + " not found in study " + studyId);
        }

        clinicalAnalysis = clinicalAnalysisQueryResult.first();
    }

    private void updateExecutorParams() {
        executorParams = new ObjectMap();

        // Session ID
        executorParams.put(CancerTieringInterpretationAnalysisExecutor.SESSION_ID, sessionId);

        // Clinical interpretation manager
        executorParams.put(CancerTieringInterpretationAnalysisExecutor.CLINICAL_INTERPRETATION_MANAGER, clinicalInterpretationManager);
    }
}