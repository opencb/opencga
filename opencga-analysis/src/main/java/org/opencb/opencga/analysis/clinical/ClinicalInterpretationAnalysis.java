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

package org.opencb.opencga.analysis.clinical;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.Interpretation;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisResult;
import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.catalog.db.api.DiseasePanelDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.ClinicalAnalysisManager;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.DiseasePanel;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.util.ArrayList;
import java.util.List;

import static org.opencb.biodata.models.clinical.interpretation.DiseasePanel.GenePanel;
import static org.opencb.biodata.models.clinical.interpretation.DiseasePanel.VariantPanel;

public class ClinicalInterpretationAnalysis extends OpenCgaAnalysis<Interpretation> {

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

    private Interpretation interpretation;

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
        super(opencgaHome, studyStr, sessionId);
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

    @Override
    public AnalysisResult<Interpretation> execute() throws Exception {
        final String userId = catalogManager.getUserManager().getUserId(sessionId);

        List<String> samples = new ArrayList<>();
        List<VariantPanel> variants;

        if (StringUtils.isNotEmpty(clinicalAnalysisId)) {
            ClinicalAnalysis clinicalAnalysis = getClinicalAnalysis();
            if (clinicalAnalysis.getProband() != null && clinicalAnalysis.getProband().getSamples() != null
                    && !clinicalAnalysis.getProband().getSamples().isEmpty()) {
                samples.add(getClinicalAnalysis().getProband().getSamples().get(0).getId());
            }
        }

        // TODO throw a proper Exception
        if (StringUtils.isEmpty(this.panelId)) {
            logger.error("No disease panel provided");
            return null;
        }

        // fetch disease panel
        Query panelQuery = new Query();
        panelQuery.put(DiseasePanelDBAdaptor.QueryParams.ID.key(), panelId);
        panelQuery.put(DiseasePanelDBAdaptor.QueryParams.VERSION.key(), panelVersion);
        QueryResult<DiseasePanel> panelResult = catalogManager.getDiseasePanelManager().get(studyStr, panelQuery, QueryOptions.empty(), sessionId);
        DiseasePanel diseasePanel = panelResult.first();

        // we create the variant strage manager
        StorageEngineFactory storageEngineFactory = StorageEngineFactory.get(storageConfiguration);
        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        // Step 1 - we first try to fetch diagnostic variants
        variants = diseasePanel.getVariants();
        Query variantQuery = new Query();
        variantQuery.put(VariantQueryParam.ID.key(), StringUtils.join(variants, ","));
        variantQuery.put(VariantQueryParam.SAMPLE.key(), StringUtils.join(samples, ","));

        // Step 2 - we first try to fetch VUS variants
        VariantQueryResult<Variant> variantVariantQueryResult = variantManager.get(variantQuery, QueryOptions.empty(), sessionId);
        List<String> geneIds = getGeneIdsFromPanel(diseasePanel);
        if (variantVariantQueryResult.getNumResults() == 0) {
            variantQuery = new Query();
            query.put(VariantQueryParam.GENE.key(), StringUtils.join(geneIds, ","));
            variantQuery.put(VariantQueryParam.SAMPLE.key(), StringUtils.join(samples, ","));
            variantQuery.put(VariantQueryParam.ANNOT_BIOTYPE.key(), "protein_coding");
            // ...

            QueryOptions queryOptions = new QueryOptions();
            queryOptions.put(QueryOptions.LIMIT, 1000);
            variantVariantQueryResult = variantManager.get(variantQuery, queryOptions, sessionId);
        }

        // hallazgos

        // creat interpertation with variantVariantQueryResult

        if (saveId != null && clinicalAnalysis != null) {
            // save in catalog
        }

        return null;
    }

    private List<String> getGeneIdsFromPanel(DiseasePanel diseasePanel) throws CatalogException {
        List<String> geneIds = new ArrayList<>(diseasePanel.getGenes().size());
        for (GenePanel gene : diseasePanel.getGenes()) {
            geneIds.add(gene.getId());
        }
        return geneIds;
    }


    public Interpretation getInterpretation() {
        return interpretation;
    }

    public ClinicalInterpretationAnalysis setInterpretation(Interpretation interpretation) {
        this.interpretation = interpretation;
        return this;
    }
}
