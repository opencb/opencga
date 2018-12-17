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
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.clinical.interpretation.*;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Analyst;
import org.opencb.biodata.models.commons.OntologyTerm;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.commons.Software;
import org.opencb.biodata.models.core.Exon;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.core.Transcript;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.analysis.AnalysisResult;
import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.analysis.exceptions.AnalysisException;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Panel;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.User;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.AlignmentStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.clinical.interpretation.ClinicalProperty.ModeOfInheritance.*;
import static org.opencb.biodata.models.clinical.interpretation.ClinicalProperty.Penetrance;
import static org.opencb.biodata.models.clinical.interpretation.DiseasePanel.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;

public class TieringAnalysis extends OpenCgaAnalysis<Interpretation> {

    private String clinicalAnalysisId;
    private List<String> diseasePanelIds;

    private int maxCoverage;

    private CellBaseClient cellBaseClient;
    private AlignmentStorageManager alignmentStorageManager;

    private final static String SEPARATOR = "__";

    public TieringAnalysis(String opencgaHome, String studyStr, String token) {
        super(opencgaHome, studyStr, token);
    }

    public TieringAnalysis(String opencgaHome, String studyStr, String token, String clinicalAnalysisId,
                           List<String> diseasePanelIds, ObjectMap config) {
        super(opencgaHome, studyStr, token);

        this.clinicalAnalysisId = clinicalAnalysisId;
        this.diseasePanelIds = diseasePanelIds;

        this.maxCoverage = 20;

        this.cellBaseClient = new CellBaseClient(storageConfiguration.getCellbase().toClientConfiguration());
        this.alignmentStorageManager = new AlignmentStorageManager(catalogManager, StorageEngineFactory.get(storageConfiguration));
    }


    @Override
    public AnalysisResult<Interpretation> execute() throws Exception {
        // checks

        // set defaults

        QueryResult<ClinicalAnalysis> clinicalAnalysisQueryResult = catalogManager.getClinicalAnalysisManager().get(studyStr,
                clinicalAnalysisId, QueryOptions.empty(), token);
        if (clinicalAnalysisQueryResult.getNumResults() == 0) {
            throw new AnalysisException("Clinical analysis " + clinicalAnalysisId + " not found in study " + studyStr);
        }

        ClinicalAnalysis clinicalAnalysis = clinicalAnalysisQueryResult.first();

        if (clinicalAnalysis.getFamily() == null || StringUtils.isEmpty(clinicalAnalysis.getFamily().getId())) {
            throw new AnalysisException("Missing family in clinical analysis " + clinicalAnalysisId);
        }

        if (clinicalAnalysis.getProband() == null || StringUtils.isEmpty(clinicalAnalysis.getProband().getId())) {
            throw new AnalysisException("Missing proband in clinical analysis " + clinicalAnalysisId);
        }

        org.opencb.opencga.core.models.Individual proband = clinicalAnalysis.getProband();
        if (ListUtils.isEmpty(proband.getSamples())) {
            throw new AnalysisException("Missing samples in proband " + proband.getId() + " in clinical analysis " + clinicalAnalysisId);
        }

        if (proband.getSamples().size() > 1) {
            throw new AnalysisException("Found more than one sample for proband " + proband.getId() + " in clinical analysis "
                    + clinicalAnalysisId);
        }

        List<Panel> diseasePanels = new ArrayList<>();
        if (diseasePanelIds != null && !diseasePanelIds.isEmpty()) {
            List<QueryResult<Panel>> queryResults = catalogManager.getPanelManager()
                    .get(studyStr, diseasePanelIds, new Query(), QueryOptions.empty(), token);

            if (queryResults.size() != diseasePanelIds.size()) {
                throw new AnalysisException("The number of disease panels retrieved doesn't match the number of disease panels queried");
            }

            for (QueryResult<Panel> queryResult : queryResults) {
                if (queryResult.getNumResults() != 1) {
                    throw new AnalysisException("The number of disease panels retrieved doesn't match the number of disease panels " +
                            "queried");
                }
                diseasePanels.add(queryResult.first());
            }
        } else {
            throw new AnalysisException("Missing disease panels");
        }

        // Check sample and proband exists

        Pedigree pedigree = FamilyManager.getPedigreeFromFamily(clinicalAnalysis.getFamily());
        OntologyTerm disease = clinicalAnalysis.getDisorder();
        Phenotype phenotype = new Phenotype(disease.getId(), disease.getName(), disease.getSource(), Phenotype.Status.UNKNOWN);

        // Query with the filters: genotypes, popFreq < 0.01, biotype = protein_coding, genes
        Query query = new Query()
                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), "protein_coding")
                .append(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "<0.01");

        VariantQueryResult<Variant> variantQueryResult;
        Map<String, ReportedVariant> reportedVariantMap = new HashMap<>();

        // Reported low coverage map
        Set<String> lowCoverageByGeneDone = new HashSet<>();
        List<ReportedLowCoverage> reportedLowCoverages = new ArrayList<>();

        // Look for the bam file of the proband
        QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(studyStr, new Query()
                        .append(FileDBAdaptor.QueryParams.SAMPLES.key(), clinicalAnalysis.getProband().getSamples().get(0).getId())
                        .append(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.BAM),
                new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.UUID.key()), token);
        if (fileQueryResult.getNumResults() > 1) {
            throw new AnalysisException("More than one BAM file found for proband " + proband.getId() + " in clinical analysis "
                    + clinicalAnalysisId);
        }

        String bamFileId = fileQueryResult.getNumResults() == 1 ? fileQueryResult.first().getUuid() : null;

        for (Panel diseasePanel: diseasePanels) {
            Map<String, List<String>> genePenetranceMap = new HashMap<>();

            for (GenePanel genePanel : diseasePanel.getDiseasePanel().getGenes()) {
                String key;
                if (StringUtils.isEmpty(genePanel.getModeOfInheritance())) {
                    key = "all";
                } else {
                    if (genePanel.getPenetrance() == null) {
                        key = genePanel.getModeOfInheritance() + SEPARATOR + Penetrance.COMPLETE;
                    } else {
                        key = genePanel.getModeOfInheritance() + SEPARATOR + genePanel.getPenetrance().name();
                    }
                }

                if (!genePenetranceMap.containsKey(key)) {
                    genePenetranceMap.put(key, new ArrayList<>());
                }

                // Add gene id to the list
                genePenetranceMap.get(key).add(genePanel.getId());
            }

            if (bamFileId != null) {
                for (GenePanel genePanel : diseasePanel.getDiseasePanel().getGenes()) {
                    String geneName = genePanel.getId();
                    if (!lowCoverageByGeneDone.contains(geneName)) {
                        reportedLowCoverages.addAll(getReportedLowCoverages(geneName, bamFileId, maxCoverage));
                        lowCoverageByGeneDone.add(geneName);
                    }
                }
            }

            Map<String, List<String>> genotypes;

            Penetrance penetrance;
            boolean incompletePenetrance;

            for (String key : genePenetranceMap.keySet()) {
                if (key.equals("all")) {
                    penetrance = Penetrance.COMPLETE;
                    incompletePenetrance = penetrance == Penetrance.INCOMPLETE;

                    genotypes = ModeOfInheritance.dominant(pedigree, phenotype, incompletePenetrance);
                    queryAndGenerateReport(phenotype, query, reportedVariantMap, diseasePanel,
                            ClinicalProperty.ModeOfInheritance.MONOALLELIC, penetrance, genotypes);

                    genotypes = ModeOfInheritance.recessive(pedigree, phenotype, incompletePenetrance);
                    queryAndGenerateReport(phenotype, query, reportedVariantMap, diseasePanel, ClinicalProperty.ModeOfInheritance.BIALLELIC,
                            penetrance, genotypes);

                    genotypes = ModeOfInheritance.xLinked(pedigree, phenotype, false);
                    queryAndGenerateReport(phenotype, query, reportedVariantMap, diseasePanel, XLINKED_BIALLELIC, penetrance, genotypes);

                    genotypes = ModeOfInheritance.xLinked(pedigree, phenotype, true);
                    queryAndGenerateReport(phenotype, query, reportedVariantMap, diseasePanel, XLINKED_MONOALLELIC, penetrance, genotypes);

                    genotypes = ModeOfInheritance.yLinked(pedigree, phenotype);
                    queryAndGenerateReport(phenotype, query, reportedVariantMap, diseasePanel, YLINKED, penetrance, genotypes);

                    processDeNovo(clinicalAnalysis, pedigree, phenotype, query, reportedVariantMap, diseasePanel);

                    processCompoundHeterozygous(clinicalAnalysis, pedigree, phenotype, query, reportedVariantMap, diseasePanel);
                } else {
                    String[] splitString = key.split(SEPARATOR);

                    // TODO: splitString[0] is a free string, it will never match a valid ClinicalProperty.ModeOfInheritance
                    ClinicalProperty.ModeOfInheritance moi = ClinicalProperty.ModeOfInheritance.valueOf(splitString[0]);
                    penetrance = Penetrance.valueOf(splitString[1]);
                    incompletePenetrance = penetrance == Penetrance.INCOMPLETE;

                    // Genes
                    query.put(VariantQueryParam.ANNOT_XREF.key(), genePenetranceMap.get(key));

                    switch (moi) {
                        case MONOALLELIC:
                            genotypes = ModeOfInheritance.dominant(pedigree, phenotype, incompletePenetrance);
                            queryAndGenerateReport(phenotype, query, reportedVariantMap, diseasePanel, moi, penetrance, genotypes);
                            break;
                        case BIALLELIC:
                            genotypes = ModeOfInheritance.recessive(pedigree, phenotype, incompletePenetrance);
                            queryAndGenerateReport(phenotype, query, reportedVariantMap, diseasePanel, moi, penetrance, genotypes);
                            break;
                        case MONOALLELIC_AND_BIALLELIC:
                        case MONOALLELIC_AND_MORE_SEVERE_BIALLELIC:
                            genotypes = ModeOfInheritance.dominant(pedigree, phenotype, incompletePenetrance);
                            queryAndGenerateReport(phenotype, query, reportedVariantMap, diseasePanel, moi, penetrance, genotypes);

                            genotypes = ModeOfInheritance.recessive(pedigree, phenotype, incompletePenetrance);
                            queryAndGenerateReport(phenotype, query, reportedVariantMap, diseasePanel, moi, penetrance, genotypes);
                            break;
                        case XLINKED_BIALLELIC:
                            genotypes = ModeOfInheritance.xLinked(pedigree, phenotype, false);
                            queryAndGenerateReport(phenotype, query, reportedVariantMap, diseasePanel, moi, penetrance, genotypes);
                            break;
                        case XLINKED_MONOALLELIC:
                            genotypes = ModeOfInheritance.xLinked(pedigree, phenotype, true);
                            queryAndGenerateReport(phenotype, query, reportedVariantMap, diseasePanel, moi, penetrance, genotypes);
                            break;
                        case YLINKED:
                            genotypes = ModeOfInheritance.yLinked(pedigree, phenotype);
                            queryAndGenerateReport(phenotype, query, reportedVariantMap, diseasePanel, moi, penetrance, genotypes);
                            break;
                        case DE_NOVO:
                            processDeNovo(clinicalAnalysis, pedigree, phenotype, query, reportedVariantMap, diseasePanel);
                            break;
                        case COMPOUND_HETEROZYGOUS:
                            processCompoundHeterozygous(clinicalAnalysis, pedigree, phenotype, query, reportedVariantMap, diseasePanel);
                            break;
                        case MITOCHRONDRIAL:
                        case MONOALLELIC_NOT_IMPRINTED:
                        case MONOALLELIC_MATERNALLY_IMPRINTED:
                        case MONOALLELIC_PATERNALLY_IMPRINTED:
                        case UNKNOWN:
                        default:
                            break;
                    }
                }
            }
        }

        String userId = catalogManager.getUserManager().getUserId(token);
        QueryResult<User> userQueryResult = catalogManager.getUserManager().get(userId, new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(UserDBAdaptor.QueryParams.EMAIL.key(), UserDBAdaptor.QueryParams.ORGANIZATION.key())), token);

        List<org.opencb.biodata.models.clinical.interpretation.DiseasePanel> biodataDiseasePanels =
                diseasePanels.stream().map(Panel::getDiseasePanel).collect(Collectors.toList());

        // Create Interpretation
        Interpretation interpretation = new Interpretation()
                .setId("JT-PF-007")
                .setAnalyst(new Analyst(userId, userQueryResult.first().getEmail(), userQueryResult.first().getOrganization()))
                .setClinicalAnalysisId(clinicalAnalysisId)
                .setCreationDate(TimeUtils.getTime())
                .setPanels(biodataDiseasePanels)
                .setFilters(null) //TODO
                .setSoftware(new Software().setName("Tiering"))
                .setReportedVariants(new ArrayList<>(reportedVariantMap.values()))
                .setReportedLowCoverages(reportedLowCoverages);

        // Return interpretation result
        return new AnalysisResult<>(interpretation);
    }

    void processCompoundHeterozygous(ClinicalAnalysis clinicalAnalysis, Pedigree pedigree, Phenotype phenotype, Query query, Map<String, ReportedVariant> reportedVariantMap, Panel diseasePanel) throws Exception {
        VariantQueryResult<Variant> variantQueryResult;
        Map<String, List<String>> probandGenotype;

        // Calculate compound heterozygous
        probandGenotype = new HashMap<>();
        probandGenotype.put(clinicalAnalysis.getProband().getId(),
                Arrays.asList(ModeOfInheritance.toGenotypeString(ModeOfInheritance.GENOTYPE_0_1)));
        putGenotypes(probandGenotype, query);
        for (GenePanel gene : diseasePanel.getDiseasePanel().getGenes()) {
            query.put(VariantQueryParam.ANNOT_XREF.key(), gene);

            variantQueryResult = variantStorageManager.get(query, QueryOptions.empty(), token);

            List<Variant> compoundHetVariantList = ModeOfInheritance.compoundHeterozygosity(pedigree,
                    variantQueryResult.getResult().iterator());
            // TODO: We need to create another ReportedModeOfInheritance for compound heterozygous!!??
            generateReportedVariants(compoundHetVariantList, phenotype, diseasePanel,
                    ClinicalProperty.ModeOfInheritance.COMPOUND_HETEROZYGOUS, reportedVariantMap);
        }
    }

    void processDeNovo(ClinicalAnalysis clinicalAnalysis, Pedigree pedigree, Phenotype phenotype, Query query, Map<String, ReportedVariant> reportedVariantMap, Panel diseasePanel) throws CatalogException, StorageEngineException, IOException {
        VariantQueryResult<Variant> variantQueryResult;
        Map<String, List<String>> probandGenotype = new HashMap<>();
        probandGenotype.put(clinicalAnalysis.getProband().getId(),
                Arrays.asList(ModeOfInheritance.toGenotypeString(ModeOfInheritance.GENOTYPE_0_0)));
        putGenotypesNegated(probandGenotype, query);

        variantQueryResult = variantStorageManager.get(query, QueryOptions.empty(), token);

        List<Variant> deNovoVariantList = ModeOfInheritance.deNovoVariants(pedigree.getProband(),
                variantQueryResult.getResult().iterator());
        // TODO: We need to create another ReportedModeOfInheritance for de novo!!??
        generateReportedVariants(deNovoVariantList, phenotype, diseasePanel, ClinicalProperty.ModeOfInheritance.DE_NOVO,
                reportedVariantMap);
    }

    void queryAndGenerateReport(Phenotype phenotype, Query query, Map<String, ReportedVariant> reportedVariantMap,
                                Panel diseasePanel, ClinicalProperty.ModeOfInheritance moi, Penetrance penetrance,
                                Map<String, List<String>> genotypes) throws CatalogException, StorageEngineException, IOException {
        VariantQueryResult<Variant> variantQueryResult;
        putGenotypes(genotypes, query);
        variantQueryResult = variantStorageManager.get(query, QueryOptions.empty(), token);
        generateReportedVariants(variantQueryResult, phenotype, diseasePanel, moi, penetrance, reportedVariantMap);
    }

    private List<ReportedLowCoverage> getReportedLowCoverages(String geneName, String bamFileId, int maxCoverage) {
        List<ReportedLowCoverage> reportedLowCoverages = new ArrayList<>();
        try {
            // Get gene exons from CellBase
            QueryResponse<Gene> geneQueryResponse = cellBaseClient.getGeneClient().get(Collections.singletonList(geneName),
                    QueryOptions.empty());
            List<RegionCoverage> regionCoverages;
            for (Transcript transcript: geneQueryResponse.getResponse().get(0).first().getTranscripts()) {
                for (Exon exon: transcript.getExons()) {
                    regionCoverages = alignmentStorageManager.getLowCoverageRegions(studyStr, bamFileId,
                            new Region(exon.getChromosome(), exon.getStart(), exon.getEnd()), maxCoverage, token).getResult();
                    for (RegionCoverage regionCoverage: regionCoverages) {
                        ReportedLowCoverage reportedLowCoverage = new ReportedLowCoverage(regionCoverage)
                                .setGeneName(geneName)
                                .setId(exon.getId());
                        reportedLowCoverages.add(reportedLowCoverage);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error getting low coverage regions for panel genes.", e.getMessage());
        }
        // And for that exon regions, get low coverage regions
        return reportedLowCoverages;
    }

    private void putGenotypes(Map<String, List<String>> genotypes, Query query) {
        query.put(VariantQueryParam.GENOTYPE.key(),
                StringUtils.join(genotypes.entrySet().stream()
                        .map(entry -> entry.getKey() + ":" + StringUtils.join(entry.getValue(), VariantQueryUtils.OR))
                        .collect(Collectors.toList()), ";"));

    }

    private void putGenotypesNegated(Map<String, List<String>> genotypes, Query query) {
        query.put(VariantQueryParam.GENOTYPE.key(),
                StringUtils.join(genotypes.entrySet().stream()
                        .map(entry -> entry.getKey() + IS + StringUtils.join(NOT + entry.getValue(), AND))
                        .collect(Collectors.toList()), AND));
    }

    private void generateReportedVariants(VariantQueryResult<Variant> variantQueryResult, Phenotype phenotype, Panel diseasePanel,
                                          ClinicalProperty.ModeOfInheritance moi, Penetrance penetrance,
                                          Map<String, ReportedVariant> reportedVariantMap) {
        for (Variant variant: variantQueryResult.getResult()) {
            if (!reportedVariantMap.containsKey(variant.getId())) {
                reportedVariantMap.put(variant.getId(), new ReportedVariant(variant.getImpl(), 0, new ArrayList<>(),
                        Collections.emptyList(), Collections.emptyMap()));
            }
            ReportedVariant reportedVariant = reportedVariantMap.get(variant.getId());

            // Sanity check
            if (variant.getAnnotation() != null && ListUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
                for (ConsequenceType ct: variant.getAnnotation().getConsequenceTypes()) {
                    // Create the reported event
                    ReportedEvent reportedEvent = new ReportedEvent()
                            .setId("JT-PF-" + reportedVariant.getReportedEvents().size())
                            .setPhenotypes(Collections.singletonList(phenotype))
                            .setConsequenceTypeIds(Collections.singletonList(ct.getBiotype()))
                            .setGenomicFeature(new GenomicFeature(ct.getEnsemblGeneId(), ct.getEnsemblTranscriptId(), ct.getGeneName(),
                                    null, null))
                            .setModeOfInheritance(moi)
                            .setPanelId(diseasePanel.getDiseasePanel().getId())
                            .setPenetrance(penetrance);

                    // TODO: add additional reported event fields

                    // Add reported event to the reported variant
                    reportedVariant.getReportedEvents().add(reportedEvent);
                }
            }
        }
    }

    private void generateReportedVariants(List<Variant> variantList, Phenotype phenotype, Panel diseasePanel,
                                          ClinicalProperty.ModeOfInheritance moi, Map<String, ReportedVariant> reportedVariantMap) {
        for (Variant variant : variantList) {
            if (!reportedVariantMap.containsKey(variant.getId())) {
                reportedVariantMap.put(variant.getId(), new ReportedVariant(variant.getImpl(), 0, new ArrayList<>(),
                        Collections.emptyList(), Collections.emptyMap()));
            }
            ReportedVariant reportedVariant = reportedVariantMap.get(variant.getId());

            // Sanity check
            if (variant.getAnnotation() != null && ListUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
                for (ConsequenceType ct: variant.getAnnotation().getConsequenceTypes()) {
                    // Create the reported event
                    ReportedEvent reportedEvent = new ReportedEvent()
                            .setId("JT-PF-" + reportedVariant.getReportedEvents().size())
                            .setPhenotypes(Collections.singletonList(phenotype))
                            .setConsequenceTypeIds(Collections.singletonList(ct.getBiotype()))
                            .setGenomicFeature(new GenomicFeature(ct.getEnsemblGeneId(), ct.getEnsemblTranscriptId(), ct.getGeneName(),
                                    null, null))
                            .setModeOfInheritance(moi)
                            .setPanelId(diseasePanel.getDiseasePanel().getId());

                    // TODO: add additional reported event fields

                    // Add reported event to the reported variant
                    reportedVariant.getReportedEvents().add(reportedEvent);
                }
            }
        }
    }

    private List<ReportedVariant> dominant() {
        return null;
    }

    private List<ReportedVariant> recessive() {
        // MoI -> genotypes
        // Variant Query query -> (biotype, gene, genoptype)
        // Iterator for (Var) -> getReportedEvents(rv)
        // create RV
        return null;
    }


    private List<ReportedEvent> getReportedEvents(Variant variant) {
        return null;
    }

    private Interpretation createInterpretation() {
        return null;
    }
}
