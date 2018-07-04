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

package org.opencb.opencga.analysis;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.ClinicalAnalysisManager;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.core.models.OntologyTerm;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.clinical.Panel;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.util.ArrayList;
import java.util.List;

public class ClinicalInterpretationAnalysis extends OpenCgaAnalysis {

    private ClinicalAnalysis clinicalAnalysis;

    private String sessionId;
    private String studyStr;

    private String clinicalAnalysisId;

    private String disease;
    private String family;
    private List<String> subjects;
    private String type;
    private String panelId;
    private String panelVersion;
    private String saveId;
    private String saveName;

    // query
    private Query query;

    private Interpretation result;

//    public ClinicalInterpretationAnalysis(String clnicalAnalysisId, String panelId, Query variantQuery, String sessionId) {
//
//    }

    public ClinicalInterpretationAnalysis(
            String opencgaHome,
            String sessionId,
            String studyStr,
            // specific parameters
            String clinicalAnalysisId,
            String disease,
            String family,
            List<String> subjects,
            String type,
            String panelId,
            String panelVersion,
            String saveId,
            String saveName,
            Query query
    ) {
        super(opencgaHome);
        this.sessionId = sessionId;
        this.studyStr = studyStr;
        this.clinicalAnalysisId = clinicalAnalysisId;
        this.disease = disease;
        this.family = family;
        // should deeply clone this one...
        this.subjects = subjects;
        this.type = type;
        this.panelId = panelId;
        this.panelVersion = panelVersion;
        this.saveId = saveId;
        this.saveName = saveName;
        // ... and maybe these two, if not immutable
        this.query = query;
    }

    private static Query extractQueryFromPanel(Panel panel) {
        // TODO: implement
        return null;
    }

    private ClinicalAnalysis getClinicalAnalysis() throws CatalogException {
        assert(null != catalogManager);
        if (StringUtils.isNotEmpty(clinicalAnalysisId)) {
            final ClinicalAnalysisManager clinicalAnalysisManager = catalogManager.getClinicalAnalysisManager();

            // have to convert session
            QueryResult<ClinicalAnalysis> clinicalAnalyses = clinicalAnalysisManager.get(
                    studyStr,
                    clinicalAnalysisId,
                    QueryOptions.empty(),
                    sessionId
            );

            clinicalAnalysis = clinicalAnalyses.first();
            return clinicalAnalysis;
        }
        return null;
    }

    public void execute() throws Exception {
        final String userId = catalogManager.getUserManager().getUserId(sessionId);

        List<String> samples = new ArrayList<>();
        List<String> variants = null;

        if (StringUtils.isNotEmpty(clinicalAnalysisId)) {
            ClinicalAnalysis clinicalAnalysis = getClinicalAnalysis();

            for (Individual individual : clinicalAnalysis.getSubjects()) {
                samples.add(individual.getSamples().get(0).getId());
            }
        }

        if (StringUtils.isNotEmpty(this.panelId)) {
//            Query panelQuery = new Query();
//            panelQuery.put(PanelDBAdaptor.QueryParams.ID.key(), panelId);
//            panelQuery.put(PanelDBAdaptor.QueryParams.VERSION.key(), panelVersion);
//            QueryResult<Panel> panelResutl = catalogManager.getPanelManager().get(panelQuery, QueryOptions.empty(), sessionId);
//            Panel panel = panelResutl.first();

//            List<String> variants = panel.getVariants();
//            String variantParam = StringUtils.join(variants, ",");
        } else {
            // petamos
        }

        Query variantQuery = new Query();
        variantQuery.put(VariantQueryParam.ID.key(), StringUtils.join(variants, ","));
        variantQuery.put(VariantQueryParam.SAMPLE.key(), StringUtils.join(samples, ","));

        StorageEngineFactory storageEngineFactory = StorageEngineFactory.get(storageConfiguration);
        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        VariantQueryResult<Variant> variantVariantQueryResult = variantManager.get(variantQuery, QueryOptions.empty(), sessionId);
//        variantVariantQueryResult.getResult()
        if (variantVariantQueryResult.getNumResults() == 0) {
            variantQuery = new Query();
            variantQuery.put(VariantQueryParam.SAMPLE.key(), StringUtils.join(samples, ","));
            variantQuery.put(VariantQueryParam.ANNOT_BIOTYPE.key(), "protein_coding");
            // ...

            QueryOptions queryOptions = new QueryOptions();
            queryOptions.put(QueryOptions.LIMIT, 1000);
            variantVariantQueryResult = variantManager.get(variantQuery, queryOptions, sessionId);
        }

        // hallazgos

        // creat interpertation with variantVariantQueryResult


        OntologyTerm hpoDisease;

    }

    public Interpretation getInterpretation() {
        // TODO: what to do if interpretation is null?
        if (null != this.result) {
            return result;
        } else {
            // TODO: what to do in this case?
            // null object pattern?
            // I thing launching a "non executed exception" is
            // the right thing to do
            return null;
        }
    }
}
