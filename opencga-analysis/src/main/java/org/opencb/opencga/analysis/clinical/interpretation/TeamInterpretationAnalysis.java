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
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.oskar.analysis.exceptions.AnalysisException;

import java.nio.file.Path;
import java.util.List;

import static org.opencb.biodata.models.clinical.interpretation.ClinicalProperty.ModeOfInheritance;

public class TeamInterpretationAnalysis extends FamilyInterpretationAnalysis {


    public final static String ID = "TeamInterpretationAnalysis";

    private ClinicalAnalysis clinicalAnalysis;
    private List<String> diseasePanelIds;
    private ModeOfInheritance moi;
    private TeamInterpretationConfiguration config;

    private List<DiseasePanel> diseasePanels;

    public TeamInterpretationAnalysis(String clinicalAnalysisId, String studyId, List<String> diseasePanelIds, ClinicalProperty.ModeOfInheritance moi,
                                         Path outDir, Path openCgaHome, TeamInterpretationConfiguration config, String sessionId) {
        super(clinicalAnalysisId, studyId, outDir, openCgaHome, sessionId);

        this.diseasePanelIds = diseasePanelIds;
        this.moi = moi;
        this.config = config;
    }

    @Override
    protected void exec() throws AnalysisException {
        check();

        diseasePanels = clinicalInterpretationManager.getDiseasePanels(studyId, diseasePanelIds, sessionId);

        // Set executor parameters
        updateExecutorParams();

        // Get executor
        TeamInterpretationAnalysisExecutor executor = new TeamInterpretationAnalysisExecutor();
        executor.setup(clinicalAnalysisId, studyId, diseasePanels, moi, outDir, executorParams, config);

        arm.startStep("get-primary/secondary-findings");
        executor.exec();
        arm.endStep(90.0F);

        arm.startStep("save-interpretation");
        saveResult(ID, diseasePanels, clinicalAnalysis, new Query(), config.isIncludeLowCoverage(), config.getMaxLowCoverage());
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
        executorParams.put(CustomInterpretationAnalysisExecutor.SESSION_ID, sessionId);

        // Clinical interpretation manager
        executorParams.put(CustomInterpretationAnalysisExecutor.CLINICAL_INTERPRETATION_MANAGER, clinicalInterpretationManager);
    }
}
