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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.clinical.interpretation.exceptions.InterpretationAnalysisException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.clinical.TeamClinicalVariantCreator;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.ClinicalInterpretationAnalysisExecutor;
import org.opencb.opencga.analysis.clinical.ClinicalInterpretationManager;
import org.opencb.opencga.analysis.clinical.ClinicalUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.clinical.ClinicalProperty.ModeOfInheritance.COMPOUND_HETEROZYGOUS;
import static org.opencb.biodata.models.clinical.ClinicalProperty.ModeOfInheritance.DE_NOVO;
import static org.opencb.biodata.tools.pedigree.ModeOfInheritance.lof;
import static org.opencb.biodata.tools.pedigree.ModeOfInheritance.proteinCoding;
import static org.opencb.opencga.analysis.clinical.InterpretationAnalysis.PRIMARY_FINDINGS_FILENAME;
import static org.opencb.opencga.analysis.clinical.InterpretationAnalysis.SECONDARY_FINDINGS_FILENAME;

@ToolExecutor(id = "opencga-local",
        tool = TeamInterpretationAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class TeamInterpretationAnalysisExecutor extends OpenCgaToolExecutor implements ClinicalInterpretationAnalysisExecutor {

    private String studyId;
    private String clinicalAnalysisId;
    private List<DiseasePanel> diseasePanels;
    private ClinicalProperty.ModeOfInheritance moi;
    private TeamInterpretationConfiguration config;

    private String sessionId;
    private ClinicalInterpretationManager clinicalInterpretationManager;

    @Override
    public void run() throws ToolException {
        sessionId = getToken();
        clinicalInterpretationManager = getClinicalInterpretationManager();

        // Get assembly
        String assembly;
        try {
            assembly = clinicalInterpretationManager.getAssembly(studyId, sessionId);
        } catch (CatalogException e) {
            throw new ToolException("Error retrieving assembly", e);
        }

        // Get and check search analysis and proband
        ClinicalAnalysis clinicalAnalysis;
        try {
            clinicalAnalysis = clinicalInterpretationManager.getClinicalAnalysis(studyId, clinicalAnalysisId, sessionId);
        } catch (CatalogException e) {
            throw new ToolException("Error getting search analysis", e);
        }
        Individual proband = ClinicalUtils.getProband(clinicalAnalysis);

        // Get sample names and update proband information (to be able to navigate to the parents and their samples easily)
        List<String> sampleList = ClinicalUtils.getSampleNames(clinicalAnalysis, proband);

        // Clinical variant creator
        TeamClinicalVariantCreator creator;

        try {
            creator = new TeamClinicalVariantCreator(diseasePanels,
                    clinicalInterpretationManager.getRoleInCancerManager().getRoleInCancer(),
                    clinicalInterpretationManager.getActionableVariantManager().getActionableVariants(assembly),
                    clinicalAnalysis.getDisorder(), null, ClinicalProperty.Penetrance.COMPLETE);
        } catch (IOException e) {
            throw new ToolException("Error creating Team search variant creator", e);
        }

        List<ClinicalVariant> primaryFindings;

        // Step 1 - diagnostic variants
        // Get diagnostic variants from panels
        List<DiseasePanel.VariantPanel> diagnosticVariants = new ArrayList<>();
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
                .map(DiseasePanel.VariantPanel::getId).collect(Collectors.toList()), ","));
        query.put(VariantQueryParam.SAMPLE.key(), StringUtils.join(sampleList, ","));

        try {
            primaryFindings = getClinicalVariants(query, queryOptions, creator);
        } catch (InterpretationAnalysisException | CatalogException | IOException | StorageEngineException e) {
            throw new ToolException("Error retrieving primary findings variants", e);
        }

        if (CollectionUtils.isEmpty(primaryFindings)) {
            // Step 2 - VUS variants from genes in panels
            List<String> geneIds = ClinicalUtils.getGeneIds(diseasePanels);
            // Remove variant IDs from the query, and set gene IDs
            query.remove(VariantQueryParam.ID.key());
            query.put(VariantQueryParam.GENE.key(), StringUtils.join(geneIds, ","));

            // VUS filter
            //
            //   Pop. frequency:
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

            try {
                primaryFindings = getClinicalVariants(query, queryOptions, creator);
            } catch (InterpretationAnalysisException | CatalogException | IOException | StorageEngineException e) {
                throw new ToolException("Error retrieving primary findings variants", e);
            }

            if (CollectionUtils.isEmpty(primaryFindings)) {
                // No loss of function variants, then try with protein_coding and protein substitution scores
                query.remove(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key());
                query.put(VariantQueryParam.ANNOT_BIOTYPE.key(), proteinCoding);
                query.put(VariantQueryParam.ANNOT_PROTEIN_SUBSTITUTION.key(), "sift<0.05" + VariantQueryUtils.AND + "polyphen>0.91");

                try {
                    primaryFindings = getClinicalVariants(query, queryOptions, creator);
                } catch (InterpretationAnalysisException | CatalogException | IOException | StorageEngineException e) {
                    throw new ToolException("Error retrieving primary findings variants", e);
                }
            }
        }

        // Write primary findings
        ClinicalUtils.writeClinicalVariants(primaryFindings, Paths.get(getOutDir() + "/" + PRIMARY_FINDINGS_FILENAME));

        // Step 3: secondary findings, if search consent is TRUE
        List<ClinicalVariant> secondaryFindings;
        try {
            secondaryFindings = clinicalInterpretationManager.getSecondaryFindings(clinicalAnalysis, sampleList, studyId,
                    creator, sessionId);
        } catch (StorageEngineException | CatalogException | IOException e) {
            throw new ToolException("Error retrieving secondary findings variants", e);
        }

        // Write primary findings
        ClinicalUtils.writeClinicalVariants(secondaryFindings, Paths.get(getOutDir() + "/" + SECONDARY_FINDINGS_FILENAME));
    }

    private List<ClinicalVariant> getClinicalVariants(Query query, QueryOptions queryOptions, TeamClinicalVariantCreator creator)
            throws InterpretationAnalysisException, CatalogException, IOException, StorageEngineException, ToolException {
        List<ClinicalVariant> clinicalVariants;
        if (moi != null && (moi == DE_NOVO || moi == COMPOUND_HETEROZYGOUS)) {
            if (moi == DE_NOVO) {
                List<Variant> deNovoVariants = clinicalInterpretationManager.getDeNovoVariants(clinicalAnalysisId, studyId, query,
                        QueryOptions.empty(), sessionId);
                clinicalVariants = creator.create(deNovoVariants);
            } else {
                Map<String, List<Variant>> chVariants = clinicalInterpretationManager.getCompoundHeterozigousVariants(clinicalAnalysisId,
                        studyId, query, QueryOptions.empty(), sessionId);
                clinicalVariants = ClinicalUtils.getCompoundHeterozygousClinicalVariants(chVariants, creator);
            }
        } else {
            clinicalVariants = creator.create(clinicalInterpretationManager.getVariantStorageManager().get(query, queryOptions, sessionId)
                    .getResults());
        }
        return clinicalVariants;
    }

    public String getClinicalAnalysisId() {
        return clinicalAnalysisId;
    }

    public TeamInterpretationAnalysisExecutor setClinicalAnalysisId(String clinicalAnalysisId) {
        this.clinicalAnalysisId = clinicalAnalysisId;
        return this;
    }

    public String getStudyId() {
        return studyId;
    }

    public TeamInterpretationAnalysisExecutor setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public List<DiseasePanel> getDiseasePanels() {
        return diseasePanels;
    }

    public TeamInterpretationAnalysisExecutor setDiseasePanels(List<DiseasePanel> diseasePanels) {
        this.diseasePanels = diseasePanels;
        return this;
    }

    public ClinicalProperty.ModeOfInheritance getMoi() {
        return moi;
    }

    public TeamInterpretationAnalysisExecutor setMoi(ClinicalProperty.ModeOfInheritance moi) {
        this.moi = moi;
        return this;
    }

    public TeamInterpretationConfiguration getConfig() {
        return config;
    }

    public TeamInterpretationAnalysisExecutor setConfig(TeamInterpretationConfiguration config) {
        this.config = config;
        return this;
    }
}
