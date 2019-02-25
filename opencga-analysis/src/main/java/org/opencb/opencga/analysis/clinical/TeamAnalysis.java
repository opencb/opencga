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
import org.opencb.biodata.models.commons.OntologyTerm;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.commons.Software;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.clinical.ReportedVariantCreator;
import org.opencb.biodata.tools.clinical.TeamReportedVariantCreator;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.analysis.exceptions.AnalysisException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.core.models.Panel;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.clinical.interpretation.DiseasePanel.GenePanel;
import static org.opencb.biodata.models.clinical.interpretation.DiseasePanel.VariantPanel;

public class TeamAnalysis extends FamilyAnalysis {

    public TeamAnalysis(String opencgaHome, String studyStr, String token, String clinicalAnalysisId, List<String> diseasePanelIds, ObjectMap teamAnalysisOptions) {
        super(opencgaHome, studyStr, token, clinicalAnalysisId, diseasePanelIds, teamAnalysisOptions);
    }

    @Override
    public InterpretationResult execute() throws Exception {
        StopWatch watcher = StopWatch.createStarted();

        // Get and check clinical analysis and proband
        ClinicalAnalysis clinicalAnalysis = getClinicalAnalysis();
        Individual proband = getProband(clinicalAnalysis);

        // Disease panels management
        List<DiseasePanel> biodataDiseasPanelList = null;
        List<Panel> diseasePanels = getDiseasePanelsFromIds(diseasePanelIds);
        if (CollectionUtils.isNotEmpty(diseasePanels)) {
            biodataDiseasPanelList = diseasePanels.stream().map(Panel::getDiseasePanel).collect(Collectors.toList());
        }

        // Get sample names and update proband information (to be able to navigate to the parents and their samples easily)
        List<String> sampleList = getSampleNames(clinicalAnalysis, proband);

        // Get diagnostic variants from panels
        List<VariantPanel> diagnosticVariants = new ArrayList<>();
        for (Panel diseasePanel : diseasePanels) {
            if (diseasePanel.getDiseasePanel() != null && CollectionUtils.isNotEmpty(diseasePanel.getDiseasePanel().getVariants())) {
                diagnosticVariants.addAll(diseasePanel.getDiseasePanel().getVariants());
            }
        }

        // Step 1 - diagnostic variants
        Query query = new Query();
        QueryOptions queryOptions = QueryOptions.empty();
        query.put(VariantQueryParam.STUDY.key(), studyStr);
        query.put(VariantQueryParam.ID.key(), StringUtils.join(diagnosticVariants, ","));
        query.put(VariantQueryParam.SAMPLE.key(), StringUtils.join(sampleList, ","));
        VariantQueryResult<Variant> queryResult = variantStorageManager.get(query, queryOptions, token);

        if (queryResult.getNumResults() == 0) {

            // Step 2 - VUS variants from genes in panels
            List<String> geneIds = getGeneIdsFromDiseasePanels(diseasePanels);
            // Remove variant IDs from the query, and set gene IDs
            query.remove(VariantQueryParam.ID.key());
            query.put(VariantQueryParam.GENE.key(), StringUtils.join(geneIds, ","));

            // VUS filter
            //
            //   Pop. frequncy:
            //     1kG_phase3:EUR<0.01
            //     1kG_phase3:IBS<0.01
            //     EXAC/gnomAD < 0.01 (ALL ??, GNOMAD_GENOMES and/or GNOMAD_EXOMES ??)
            //     MGP< 0.01, (ALL ?)
            //   Conservation:
            //     GERP > 2
            //   SO (consequence type)
            //     if (Loss of Function)
            //       ScaledCADD > 15
            //     else if (Protein Coding)
            //       SIFT < 0.05
            //       Polyphen2 > 0.91
            //       ScaledCADD > 15

            List<String> lof = new ArrayList<>();
            lof.addAll(ReportedVariantCreator.LOF_SET);
            query.put(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), StringUtils.join(lof, VariantQueryUtils.OR));

            query.put(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:EUR<0.01"
                    + VariantQueryUtils.OR + "1kG_phase3:IBS<0.01"
                    + VariantQueryUtils.OR + "EXAC:ALL<0.01"
                    + VariantQueryUtils.OR + "GNOMAD_GENOMES:ALL<0.01"
                    + VariantQueryUtils.OR + "GNOMAD_EXOMES:ALL<0.01"
                    + VariantQueryUtils.OR + "MGP:ALL<0.01");
            query.put(VariantQueryParam.ANNOT_CONSERVATION.key(), "gerp>2");
            query.put(VariantQueryParam.ANNOT_FUNCTIONAL_SCORE.key(), "scaled_cadd>15");

            queryResult = variantStorageManager.get(query, queryOptions, token);

            if (queryResult.getNumResults() == 0) {
                // No loss of function variants, then try with protein_coding and protein substitution scores
                query.put(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), "protein_coding");
                query.put(VariantQueryParam.ANNOT_PROTEIN_SUBSTITUTION.key(), "sift<0.05" + VariantQueryUtils.AND + "polyphen>0.91");
                queryResult = variantStorageManager.get(query, queryOptions, token);
            }
        }

        // Step 3: findings
        query = new Query();
        query.put(VariantQueryParam.STUDY.key(), studyStr);
        query.put(VariantQueryParam.SAMPLE.key(), StringUtils.join(sampleList, ","));

        List<ReportedVariant> reportedVariants = null;
        if (queryResult.getNumResults() > 0) {
            // Phenotype
            OntologyTerm disease = clinicalAnalysis.getDisorder();
            Phenotype phenotype = new Phenotype(disease.getId(), disease.getName(), disease.getSource(), Phenotype.Status.UNKNOWN);

            TeamReportedVariantCreator creator = new TeamReportedVariantCreator(biodataDiseasPanelList, null, phenotype, null, null);
            creator.setIncludeNoTier(config.getBoolean("includeNoTier", true));

            reportedVariants = creator.create(queryResult.getResult());
        }

        // Reported low coverages management
        List<ReportedLowCoverage> reportedLowCoverages = null;
        if (config.getBoolean("includeLowCoverage", false)) {
            reportedLowCoverages = getReportedLowCoverage(clinicalAnalysis, diseasePanels);
        }


        // Create Interpretation
        Interpretation interpretation = new Interpretation()
                .setId("OpenCGA-TEAM-" + TimeUtils.getTime())
                .setAnalyst(getAnalyst(token))
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

    private List<String> getGeneIdsFromDiseasePanels(List<Panel> diseasePanels) throws CatalogException {
        List<String> geneIds = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(diseasePanels)) {
            for (Panel diseasePanel : diseasePanels) {
                if (diseasePanel.getDiseasePanel() != null && CollectionUtils.isNotEmpty(diseasePanel.getDiseasePanel().getGenes())) {
                    for (GenePanel gene : diseasePanel.getDiseasePanel().getGenes()) {
                        geneIds.add(gene.getId());
                    }
                }
            }
        }
        return geneIds;
    }
}
