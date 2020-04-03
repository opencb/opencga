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

package org.opencb.opencga.analysis.clinical.team;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.InterpretationAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.List;

@Tool(id = TeamInterpretationAnalysis.ID, resource = Enums.Resource.CLINICAL)
public class TeamInterpretationAnalysis extends InterpretationAnalysis {

    public final static String ID = "team";
    public final static String DESCRIPTION = "Run TEAM interpretation analysis";

    private String studyId;
    private String clinicalAnalysisId;
    private List<String> diseasePanelIds;
    private ClinicalProperty.ModeOfInheritance moi;
    private TeamInterpretationConfiguration config;

    private ClinicalAnalysis clinicalAnalysis;
    private List<DiseasePanel> diseasePanels;

    @Override
    protected void check() throws Exception {
        super.check();

        // Check study
        if (StringUtils.isEmpty(studyId)) {
            throw new ToolException("Missing study");
        }
        try {
            catalogManager.getStudyManager().get(studyId, null, token).first().getFqn();
        } catch (CatalogException e) {
            throw new ToolException(e);
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
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
        if (clinicalAnalysisQueryResult.getNumResults() != 1) {
            throw new ToolException("Clinical analysis " + clinicalAnalysisId + " not found in study " + studyId);
        }
        clinicalAnalysis = clinicalAnalysisQueryResult.first();

        // Check disease panels
        if (CollectionUtils.isEmpty(diseasePanelIds)) {
            throw new ToolException("Missing disease panels for TEAM interpretation analysis");
        }
        diseasePanels = clinicalInterpretationManager.getDiseasePanels(studyId, diseasePanelIds, token);
        if (CollectionUtils.isEmpty(diseasePanels)) {
            throw new ToolException("Disease panels not found for TEAM interpretation analysis: "
                    + StringUtils.join(diseasePanelIds, ","));
        }

        // Update executor params with OpenCGA home and session ID
        setUpStorageEngineExecutor(studyId);
    }

    @Override
    protected void run() throws ToolException {
        step(() -> {
            getToolExecutor(TeamInterpretationAnalysisExecutor.class)
                    .setStudyId(studyId)
                    .setClinicalAnalysisId(clinicalAnalysisId)
                    .setDiseasePanels(diseasePanels)
                    .setMoi(moi)
                    .setConfig(config)
                    .execute();

            saveInterpretation(studyId, clinicalAnalysis, diseasePanels, null, config);
        });
    }

    public String getStudyId() {
        return studyId;
    }

    public TeamInterpretationAnalysis setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getClinicalAnalysisId() {
        return clinicalAnalysisId;
    }

    public TeamInterpretationAnalysis setClinicalAnalysisId(String clinicalAnalysisId) {
        this.clinicalAnalysisId = clinicalAnalysisId;
        return this;
    }

    public List<String> getDiseasePanelIds() {
        return diseasePanelIds;
    }

    public TeamInterpretationAnalysis setDiseasePanelIds(List<String> diseasePanelIds) {
        this.diseasePanelIds = diseasePanelIds;
        return this;
    }

    public ClinicalProperty.ModeOfInheritance getMoi() {
        return moi;
    }

    public TeamInterpretationAnalysis setMoi(ClinicalProperty.ModeOfInheritance moi) {
        this.moi = moi;
        return this;
    }

    public TeamInterpretationConfiguration getConfig() {
        return config;
    }

    public TeamInterpretationAnalysis setConfig(TeamInterpretationConfiguration config) {
        this.config = config;
        return this;
    }
}
