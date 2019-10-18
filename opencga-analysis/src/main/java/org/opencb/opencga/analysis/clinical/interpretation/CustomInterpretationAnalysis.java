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
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.storage.core.manager.variant.VariantCatalogQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.core.annotations.Analysis;

import java.nio.file.Path;
import java.util.List;

import static org.opencb.opencga.storage.core.manager.variant.VariantCatalogQueryUtils.FAMILY;
import static org.opencb.opencga.storage.core.manager.variant.VariantCatalogQueryUtils.FAMILY_DISORDER;

@Analysis(id = CustomInterpretationAnalysis.ID, data = Analysis.AnalysisData.CLINICAL)
public class CustomInterpretationAnalysis extends FamilyInterpretationAnalysis {

    public final static String ID = "CustomInterpretationAnalysis";

    private Query query;
    private QueryOptions queryOptions;
    private CustomInterpretationConfiguration config;

    private ClinicalAnalysis clinicalAnalysis;

    public CustomInterpretationAnalysis(String clinicalAnalysisId, String studyId, Query query, QueryOptions queryOptions, Path outDir,
                                        Path openCgaHome, CustomInterpretationConfiguration config, String sessionId) {
        super(clinicalAnalysisId, studyId, outDir, openCgaHome, sessionId);

        this.query = query;
        this.queryOptions = queryOptions;
        this.config = config;
    }


    @Override
    protected void exec() throws AnalysisException {
        check();

        // Set executor parameters
        updateExecutorParams();

        // Get executor
        CustomInterpretationAnalysisExecutor executor = new CustomInterpretationAnalysisExecutor();
        executor.setup(clinicalAnalysisId, query, queryOptions, outDir, executorParams, config);

        arm.startStep("get-primary/secondary-findings");
        executor.exec();
        arm.endStep(90.0F);

        arm.startStep("save-interpretation");
        List<DiseasePanel> diseasePanels = clinicalInterpretationManager.getDiseasePanels(query, sessionId);
        saveResult(ID, diseasePanels, clinicalAnalysis,query, config.isIncludeLowCoverage(), config.getMaxLowCoverage());
        arm.endStep(100.0F);
    }

    protected void check() throws AnalysisException {
        // Check study
        if (StringUtils.isNotEmpty(studyId)) {
            if (query.containsKey(VariantQueryParam.STUDY.key()) && !studyId.equals(query.get(VariantQueryParam.STUDY.key()))) {
                // Query contains a different study than the input parameter
                throw new AnalysisException("Mismatch study: query (" + query.getString(VariantQueryParam.STUDY.key())
                        + ") and input parameter (" + studyId + ")");
            } else {
                query.put(VariantQueryParam.STUDY.key(), studyId);
            }
        } else if (!query.containsKey(VariantQueryParam.STUDY.key())) {
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
        } catch (CatalogException e) {
            throw new AnalysisException(e);
        }
        if (clinicalAnalysisQueryResult.getNumResults() != 1) {
            throw new AnalysisException("Clinical analysis " + clinicalAnalysisId + " not found in study " + studyId);
        }

        clinicalAnalysis = clinicalAnalysisQueryResult.first();
        query.put(VariantCatalogQueryUtils.CLINICAL_ANALYSIS.key(), clinicalAnalysisId);

        // Proband ID
        if (clinicalAnalysis.getProband() != null && StringUtils.isNotEmpty(clinicalAnalysis.getProband().getId())) {
            String probandSampleId = clinicalAnalysis.getProband().getId();
            if (query.containsKey(VariantQueryParam.SAMPLE.key()) && !probandSampleId.equals(query.get(VariantQueryParam.SAMPLE.key()))) {
                // Query contains a different sample than clinical analysis
                throw new AnalysisException("Mismatch sample: query (" + query.getString(VariantQueryParam.SAMPLE.key())
                        + ") and clinical analysis (" + probandSampleId + ")");
            } else {
                query.put(VariantQueryParam.SAMPLE.key(), probandSampleId);
            }
        }

        // Family ID
        if (clinicalAnalysis.getFamily() != null && StringUtils.isNotEmpty(clinicalAnalysis.getFamily().getId())) {
            String familyId = clinicalAnalysis.getFamily().getId();
            if (query.containsKey(FAMILY.key()) && !familyId.equals(query.get(FAMILY.key()))) {
                // Query contains a different family than clinical analysis
                throw new AnalysisException("Mismatch family: query (" + query.getString(FAMILY.key()) + ") and clinical analysis ("
                        + familyId + ")");
            } else {
                query.put(FAMILY.key(), familyId);
            }
        }

        // Check disorder
        if (clinicalAnalysis.getDisorder() != null && StringUtils.isNotEmpty(clinicalAnalysis.getDisorder().getId())) {
            String disorderId = clinicalAnalysis.getDisorder().getId();
            if (query.containsKey(FAMILY_DISORDER.key())
                    && !disorderId.equals(query.get(FAMILY_DISORDER.key()))) {
                // Query contains a different disorder than clinical analysis
                throw new AnalysisException("Mismatch disorder: query (" + query.getString(FAMILY_DISORDER.key())
                        + ") and clinical analysis (" + disorderId + ")");
            } else {
                query.put(FAMILY_DISORDER.key(), disorderId);
            }
        }
    }

    private void updateExecutorParams() {
        executorParams = new ObjectMap();

        // Session ID
        executorParams.put(CustomInterpretationAnalysisExecutor.SESSION_ID, sessionId);

        // Clinical interpretation manager
        executorParams.put(CustomInterpretationAnalysisExecutor.CLINICAL_INTERPRETATION_MANAGER, clinicalInterpretationManager);
    }
}
