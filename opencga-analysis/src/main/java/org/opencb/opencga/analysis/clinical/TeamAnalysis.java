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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.clinical.interpretation.Interpretation;
import org.opencb.biodata.models.clinical.interpretation.ReportedLowCoverage;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.commons.Analyst;
import org.opencb.biodata.models.commons.Software;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.analysis.exceptions.AnalysisException;
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.ClinicalAnalysisManager;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Panel;
import org.opencb.opencga.core.models.User;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.clinical.interpretation.DiseasePanel.GenePanel;
import static org.opencb.biodata.models.clinical.interpretation.DiseasePanel.VariantPanel;

public class TeamAnalysis extends OpenCgaAnalysis<Interpretation> {

    private ClinicalAnalysis clinicalAnalysis;

    private String sessionId;
    private String studyStr;

    private String clinicalAnalysisId;
    private List<String> diseasePanelIds;

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

//    public TeamAnalysis(String clnicalAnalysisId, String panelId, Query variantQuery, String sessionId) {
//
//    }

    public TeamAnalysis(
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

    public TeamAnalysis(String opencgaHome, String studyStr, String token, String clinicalAnalysisId, List<String> panelList, ObjectMap teamAnalysisOptions) {
        super(opencgaHome, studyStr, token);

        this.clinicalAnalysisId = clinicalAnalysisId;
        this.diseasePanelIds = diseasePanelIds;
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
    public InterpretationResult execute() throws Exception {
        StopWatch watcher = StopWatch.createStarted();

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
        panelQuery.put(PanelDBAdaptor.QueryParams.ID.key(), panelId);
        panelQuery.put(PanelDBAdaptor.QueryParams.VERSION.key(), panelVersion);
        QueryResult<Panel> panelResult = catalogManager.getPanelManager().get(studyStr, panelQuery, QueryOptions.empty(), sessionId);
        Panel diseasePanel = panelResult.first();

        // we create the variant strage manager
        StorageEngineFactory storageEngineFactory = StorageEngineFactory.get(storageConfiguration);
        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        // Step 1 - diagnostic variants
        variants = diseasePanel.getDiseasePanel().getVariants();
        Query variantQuery = new Query();
        variantQuery.put(VariantQueryParam.ID.key(), StringUtils.join(variants, ","));
        variantQuery.put(VariantQueryParam.SAMPLE.key(), StringUtils.join(samples, ","));

        // Step 2 - VUS variants
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

        // Step 3: findings

        // User analyst management
        String userId;
        QueryResult<User> userQueryResult;
        try {
            userId = catalogManager.getUserManager().getUserId(token);
            userQueryResult = catalogManager.getUserManager().get(userId, new QueryOptions(QueryOptions.INCLUDE,
                    Arrays.asList(UserDBAdaptor.QueryParams.EMAIL.key(), UserDBAdaptor.QueryParams.ORGANIZATION.key())), token);
        } catch (CatalogException e) {
            throw new AnalysisException(e.getMessage(), e);
        }

        // TODO: reported variants
        List<ReportedVariant> reportedVariants = null;

        // TODO: disease panels
        List<Panel> diseasePanels = new ArrayList<>();
        List<DiseasePanel> biodataDiseasPanelList = diseasePanels.stream().map(Panel::getDiseasePanel).collect(Collectors.toList());

        // TODO: reported low coverages
        List<ReportedLowCoverage> reportedLowCoverages = new ArrayList<>();


        // Create Interpretation
        Interpretation interpretation = new Interpretation()
                .setId("OpenCGA-TEAM-" + TimeUtils.getTime())
                .setAnalyst(new Analyst(userId, userQueryResult.first().getEmail(), userQueryResult.first().getOrganization()))
                .setClinicalAnalysisId(clinicalAnalysisId)
                .setCreationDate(TimeUtils.getTime())
                .setPanels(biodataDiseasPanelList)
                .setFilters(null) //TODO
                .setSoftware(new Software().setName("TEAM"))
                .setReportedVariants(reportedVariants)
                .setReportedLowCoverages(reportedLowCoverages);

        // Return interpretation result
        int numResults = CollectionUtils.isEmpty(reportedVariants) ? 0 : reportedVariants.size();
        return new InterpretationResult(
                interpretation,
                Math.toIntExact(watcher.getTime()),
                new HashMap<>(),
                Math.toIntExact(watcher.getTime()), // DB time
                numResults,
                numResults,
                "", // warning message
                ""); // error message
    }

    private List<String> getGeneIdsFromPanel(Panel diseasePanel) throws CatalogException {
        List<String> geneIds = new ArrayList<>(diseasePanel.getDiseasePanel().getGenes().size());
        for (GenePanel gene : diseasePanel.getDiseasePanel().getGenes()) {
            geneIds.add(gene.getId());
        }
        return geneIds;
    }


    public Interpretation getInterpretation() {
        return interpretation;
    }

    public TeamAnalysis setInterpretation(Interpretation interpretation) {
        this.interpretation = interpretation;
        return this;
    }
}
