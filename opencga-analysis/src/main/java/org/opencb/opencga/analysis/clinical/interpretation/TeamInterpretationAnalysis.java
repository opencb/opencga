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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.clinical.interpretation.Interpretation;
import org.opencb.biodata.models.clinical.interpretation.ReportedLowCoverage;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.commons.Software;
import org.opencb.biodata.tools.clinical.TeamReportedVariantCreator;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.ClinicalUtils;
import org.opencb.opencga.analysis.clinical.CompoundHeterozygousAnalysis;
import org.opencb.opencga.analysis.clinical.DeNovoAnalysis;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.oskar.analysis.exceptions.AnalysisException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.clinical.interpretation.ClinicalProperty.*;
import static org.opencb.biodata.models.clinical.interpretation.ClinicalProperty.ModeOfInheritance.COMPOUND_HETEROZYGOUS;
import static org.opencb.biodata.models.clinical.interpretation.ClinicalProperty.ModeOfInheritance.DE_NOVO;
import static org.opencb.biodata.models.clinical.interpretation.DiseasePanel.VariantPanel;
import static org.opencb.biodata.tools.pedigree.ModeOfInheritance.lof;
import static org.opencb.biodata.tools.pedigree.ModeOfInheritance.proteinCoding;

public class TeamInterpretationAnalysis extends FamilyInterpretationAnalysis {

    private ModeOfInheritance moi;

    public TeamInterpretationAnalysis(String clinicalAnalysisId, String studyId, List<String> diseasePanelIds, ModeOfInheritance moi,
                                      ObjectMap options, String opencgaHome, String sessionId) {
        super(clinicalAnalysisId, studyId, diseasePanelIds, options, opencgaHome, sessionId);
        this.moi = moi;
    }

    @Override
    protected void exec() throws AnalysisException {
    }

    public InterpretationResult compute() throws Exception {
        StopWatch watcher = StopWatch.createStarted();

        List<ReportedVariant> primaryFindings;

        // Get and check clinical analysis and proband
        ClinicalAnalysis clinicalAnalysis = getClinicalAnalysis();
        Individual proband = getProband(clinicalAnalysis);

        // Disease panels management
        List<DiseasePanel> biodataDiseasePanels = null;
        List<DiseasePanel> diseasePanels = getDiseasePanelsFromIds(diseasePanelIds);

        // Get sample names and update proband information (to be able to navigate to the parents and their samples easily)
        List<String> sampleList = getSampleNames(clinicalAnalysis, proband);

        // Reported variant creator
        TeamReportedVariantCreator creator = new TeamReportedVariantCreator(biodataDiseasePanels, roleInCancerManager.getRoleInCancer(),
                actionableVariantManager.getActionableVariants(ClinicalUtils.getAssembly(catalogManager, studyId, sessionId)),
                clinicalAnalysis.getDisorder(), null, Penetrance.COMPLETE);

        // Step 1 - diagnostic variants
        // Get diagnostic variants from panels
        List<VariantPanel> diagnosticVariants = new ArrayList<>();
        for (DiseasePanel diseasePanel : diseasePanels) {
            if (diseasePanel != null && CollectionUtils.isNotEmpty(diseasePanel.getVariants())) {
                diagnosticVariants.addAll(diseasePanel.getVariants());
            }
        }

        // ...and then query
        Query query = new Query();
        QueryOptions queryOptions = QueryOptions.empty();
        query.put(VariantQueryParam.STUDY.key(), studyId);
        query.put(VariantQueryParam.ID.key(), StringUtils.join(diagnosticVariants.stream()
                .map(VariantPanel::getId).collect(Collectors.toList()), ","));
        query.put(VariantQueryParam.SAMPLE.key(), StringUtils.join(sampleList, ","));

        primaryFindings = getReportedVariants(query, queryOptions, creator);

        if (CollectionUtils.isEmpty(primaryFindings)) {
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
            //     if (SO: Loss of Function)
            //       ScaledCADD > 15
            //     else if (biotype: Protein Coding)
            //       SIFT < 0.05
            //       Polyphen2 > 0.91
            //       ScaledCADD > 15

            query.put(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), lof);

            query.put(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:EUR<0.01"
                    + VariantQueryUtils.OR + "1kG_phase3:IBS<0.01"
                    + VariantQueryUtils.OR + "EXAC:ALL<0.01"
                    + VariantQueryUtils.OR + "GNOMAD_GENOMES:ALL<0.01"
                    + VariantQueryUtils.OR + "GNOMAD_EXOMES:ALL<0.01"
                    + VariantQueryUtils.OR + "MGP:ALL<0.01");
            query.put(VariantQueryParam.ANNOT_CONSERVATION.key(), "gerp>2");
            query.put(VariantQueryParam.ANNOT_FUNCTIONAL_SCORE.key(), "scaled_cadd>15");

            primaryFindings = getReportedVariants(query, queryOptions, creator);

            if (CollectionUtils.isEmpty(primaryFindings)) {
                // No loss of function variants, then try with protein_coding and protein substitution scores
                query.remove(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key());
                query.put(VariantQueryParam.ANNOT_BIOTYPE.key(), proteinCoding);
                query.put(VariantQueryParam.ANNOT_PROTEIN_SUBSTITUTION.key(), "sift<0.05" + VariantQueryUtils.AND + "polyphen>0.91");

                primaryFindings = getReportedVariants(query, queryOptions, creator);
            }
        }

        // Step 3: secondary findings, if clinical consent is TRUE
        List<ReportedVariant> secondaryFindings = getSecondaryFindings(clinicalAnalysis, sampleList, creator);

        // Reported low coverages management
        List<ReportedLowCoverage> reportedLowCoverages = null;
        if (options.getBoolean(INCLUDE_LOW_COVERAGE_PARAM, false)) {
            reportedLowCoverages = getReportedLowCoverage(clinicalAnalysis, diseasePanels);
        }


        // Create Interpretation
        Interpretation interpretation = new Interpretation()
                .setId("OpenCGA-TEAM-" + TimeUtils.getTime())
                .setAnalyst(getAnalyst(sessionId))
                .setClinicalAnalysisId(clinicalAnalysisId)
                .setCreationDate(TimeUtils.getTime())
                .setPanels(biodataDiseasePanels)
                .setFilters(null) //TODO
                .setSoftware(new Software().setName("TEAM"))
                .setPrimaryFindings(primaryFindings)
                .setSecondaryFindings(secondaryFindings)
                .setLowCoverageRegions(reportedLowCoverages);

        // Return interpretation result
        int numResults = CollectionUtils.isEmpty(primaryFindings) ? 0 : primaryFindings.size();
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

    List<ReportedVariant> getReportedVariants(Query query, QueryOptions queryOptions, TeamReportedVariantCreator creator) throws Exception {
        List<ReportedVariant> reportedVariants;
        if (moi != null && (moi == DE_NOVO || moi == COMPOUND_HETEROZYGOUS)) {
            if (moi == DE_NOVO) {
                DeNovoAnalysis deNovoAnalysis = new DeNovoAnalysis(clinicalAnalysisId, studyId, query, options, opencgaHome, sessionId);
                reportedVariants = creator.create(deNovoAnalysis.compute().getResult());
            } else {
                CompoundHeterozygousAnalysis compoundAnalysis = new CompoundHeterozygousAnalysis(clinicalAnalysisId, studyId, query,
                        options, opencgaHome, sessionId);
                reportedVariants = getCompoundHeterozygousReportedVariants(compoundAnalysis.compute().getResult(), creator);
            }
        } else {
            reportedVariants = creator.create(variantStorageManager.get(query, queryOptions, sessionId).getResult());
        }
        return reportedVariants;
    }
}
