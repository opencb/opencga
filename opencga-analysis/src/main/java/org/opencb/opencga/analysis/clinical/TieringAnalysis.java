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

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.clinical.interpretation.Interpretation;
import org.opencb.biodata.models.clinical.interpretation.*;
import org.opencb.biodata.models.clinical.interpretation.exceptions.InterpretationAnalysisException;
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
import org.opencb.biodata.tools.clinical.TieringReportedVariantCreator;
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
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.AlignmentStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.clinical.interpretation.ClinicalProperty.ModeOfInheritance.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.NONE;

public class TieringAnalysis extends OpenCgaAnalysis<Interpretation> {

    private String clinicalAnalysisId;
    private List<String> diseasePanelIds;

    private ObjectMap config;
    @Deprecated
    private int maxCoverage;

    private CellBaseClient cellBaseClient;
    private AlignmentStorageManager alignmentStorageManager;

    private final static String SEPARATOR = "__";

    private final static Query dominantQuery;
    private final static Query recessiveQuery;

    static {
        recessiveQuery = new Query()
                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), "protein_coding,IG_C_gene,IG_D_gene,IG_J_gene,IG_V_gene,"
                        + "nonsense_mediated_decay,non_stop_decay,TR_C_gene,TR_D_gene,TR_J_gene,TR_V_gene")
                .append(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:AFR<0.01;1kG_phase3:AMR<0.01;"
                        + "1kG_phase3:EAS<0.01;1kG_phase3:EUR<0.01;1kG_phase3:SAS<0.01;GNOMAD_EXOMES:AFR<0.01;GNOMAD_EXOMES:AMR<0.01;"
                        + "GNOMAD_EXOMES:EAS<0.01;GNOMAD_EXOMES:FIN<0.01;GNOMAD_EXOMES:NFE<0.01;GNOMAD_EXOMES:ASJ<0.01;"
                        + "GNOMAD_EXOMES:OTH<0.01")
                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001893,SO:0001574,SO:0001575,SO:0001587,SO:0001589,SO:0001578,"
                        + "SO:0001582,SO:0001889,SO:0001821,SO:0001822,SO:0001583,SO:0001630,SO:0001626");

        dominantQuery = new Query()
                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), "protein_coding,IG_C_gene,IG_D_gene,IG_J_gene,IG_V_gene,"
                        + "nonsense_mediated_decay,non_stop_decay,TR_C_gene,TR_D_gene,TR_J_gene,TR_V_gene")
                .append(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:AFR<0.002;1kG_phase3:AMR<0.002;"
                        + "1kG_phase3:EAS<0.002;1kG_phase3:EUR<0.002;1kG_phase3:SAS<0.002;GNOMAD_EXOMES:AFR<0.001;GNOMAD_EXOMES:AMR<0.001;"
                        + "GNOMAD_EXOMES:EAS<0.001;GNOMAD_EXOMES:FIN<0.001;GNOMAD_EXOMES:NFE<0.001;GNOMAD_EXOMES:ASJ<0.001;"
                        + "GNOMAD_EXOMES:OTH<0.002")
                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001893,SO:0001574,SO:0001575,SO:0001587,SO:0001589,SO:0001578,"
                        + "SO:0001582,SO:0001889,SO:0001821,SO:0001822,SO:0001583,SO:0001630,SO:0001626");
    }


    public TieringAnalysis(String opencgaHome, String studyStr, String token) {
        super(opencgaHome, studyStr, token);
    }

    public TieringAnalysis(String opencgaHome, String studyStr, String token, String clinicalAnalysisId,
                           List<String> diseasePanelIds, ObjectMap config) {
        super(opencgaHome, studyStr, token);

        this.clinicalAnalysisId = clinicalAnalysisId;
        this.diseasePanelIds = diseasePanelIds;

        this.config = config;
        this.maxCoverage = 20;

        this.cellBaseClient = new CellBaseClient(storageConfiguration.getCellbase().toClientConfiguration());
        this.alignmentStorageManager = new AlignmentStorageManager(catalogManager, StorageEngineFactory.get(storageConfiguration));
    }

    @Override
    public InterpretationResult execute() throws AnalysisException {
        StopWatch watcher = StopWatch.createStarted();

        // Sanity check
        QueryResult<ClinicalAnalysis> clinicalAnalysisQueryResult;
        try {
            clinicalAnalysisQueryResult = catalogManager.getClinicalAnalysisManager()
                    .get(studyStr, clinicalAnalysisId, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
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

        Individual proband = clinicalAnalysis.getProband();
        if (ListUtils.isEmpty(proband.getSamples())) {
            throw new AnalysisException("Missing samples in proband " + proband.getId() + " in clinical analysis " + clinicalAnalysisId);
        }

        if (proband.getSamples().size() > 1) {
            throw new AnalysisException("Found more than one sample for proband " + proband.getId() + " in clinical analysis "
                    + clinicalAnalysisId);
        }

        List<Panel> diseasePanels = new ArrayList<>();
        if (diseasePanelIds != null && !diseasePanelIds.isEmpty()) {
            List<QueryResult<Panel>> queryResults;
            try {
                queryResults = catalogManager.getPanelManager()
                        .get(studyStr, diseasePanelIds, new Query(), QueryOptions.empty(), token);
            } catch (CatalogException e) {
                throw new AnalysisException(e.getMessage(), e);
            }

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

        List<String> sampleList = new ArrayList<>();
        Map<String, Individual> individualMap = new HashMap<>();
        for (Individual member : clinicalAnalysis.getFamily().getMembers()) {
            if (member.getSamples().size() > 1) {
                throw new AnalysisException("More than one sample found for member " + member.getId());
            }
            if (member.getSamples().size() == 0) {
                throw new AnalysisException("No samples found for member " + member.getId());
            }
            sampleList.add(member.getSamples().get(0).getId());
            individualMap.put(member.getId(), member);
        }

        // Fill proband information to be able to navigate to the parents and their samples easily
        proband.setFather(individualMap.get(proband.getFather().getId()));
        proband.setMother(individualMap.get(proband.getMother().getId()));

        Query query = new Query(dominantQuery)
                .append(VariantQueryParam.SAMPLE.key(), StringUtils.join(sampleList, ","))
                .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.INCLUDE_FILE.key(), NONE)
                .append(VariantQueryParam.STUDY.key(), studyStr)
                .append(VariantQueryParam.FILTER.key(), VCFConstants.PASSES_FILTERS_v4)
                .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./.");
        VariantDBIterator dominantQueryResult;
        try {
            dominantQueryResult = variantStorageManager.iterator(query, QueryOptions.empty(), token);
        } catch (CatalogException | StorageEngineException e) {
            throw new AnalysisException(e.getMessage(), e);
        }

        Set<String> dominantCompleteSet = genotypeMapToSet(ModeOfInheritance.dominant(pedigree, phenotype, false));
        Set<String> xLinkedDominantSet = genotypeMapToSet(ModeOfInheritance.xLinked(pedigree, phenotype, true));
        Set<String> yLinkedSet = genotypeMapToSet(ModeOfInheritance.yLinked(pedigree, phenotype));

        List<Variant> variantList = new ArrayList<>();
        Map<String, List<ClinicalProperty.ModeOfInheritance>> variantMoIMap = new HashMap<>();

        while (dominantQueryResult.hasNext()) {
            Variant variant = dominantQueryResult.next();

            boolean dominantMoI = true;
            boolean yLinkedMoI = true;
            boolean xDominantMoI = true;
            for (int i = 0; i < sampleList.size(); i++) {
                String genotype = clinicalAnalysis.getFamily().getMembers().get(i).getId() + SEPARATOR
                        + variant.getStudies().get(0).getSampleData(i).get(0);

                dominantMoI &= dominantCompleteSet.contains(genotype);
                yLinkedMoI &= yLinkedSet.contains(genotype);
                xDominantMoI &= xLinkedDominantSet.contains(genotype);
                // TODO: De novo
            }

            if (dominantMoI) {
                addVariantToList(variant, variantList, variantMoIMap, MONOALLELIC);
            }
            if (yLinkedMoI) {
                addVariantToList(variant, variantList, variantMoIMap, YLINKED);
            }
            if (xDominantMoI) {
                addVariantToList(variant, variantList, variantMoIMap, XLINKED_MONOALLELIC);
            }
        }

        query = new Query(recessiveQuery)
                .append(VariantQueryParam.SAMPLE.key(), StringUtils.join(sampleList, ","))
                .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.INCLUDE_FILE.key(), NONE)
                .append(VariantQueryParam.STUDY.key(), studyStr)
                .append(VariantQueryParam.FILTER.key(), VCFConstants.PASSES_FILTERS_v4)
                .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./.");
        VariantDBIterator recessiveQueryResult;
        try {
            recessiveQueryResult = variantStorageManager.iterator(query, QueryOptions.empty(), token);
        } catch (CatalogException | StorageEngineException e) {
            throw new AnalysisException(e.getMessage(), e);
        }

        Set<String> recessiveCompleteSet = genotypeMapToSet(ModeOfInheritance.recessive(pedigree, phenotype, false));
        Set<String> xLinkedRecessiveSet = genotypeMapToSet(ModeOfInheritance.xLinked(pedigree, phenotype, false));

        Map<String, List<Variant>> compoundHeterozygousMap = new HashMap<>();

        while (recessiveQueryResult.hasNext()) {
            Variant variant = dominantQueryResult.next();

            boolean recessiveMoI = true;
            boolean xRecessiveMoI = true;
            for (int i = 0; i < sampleList.size(); i++) {
                String genotype = clinicalAnalysis.getFamily().getMembers().get(i).getId() + SEPARATOR
                        + variant.getStudies().get(0).getSampleData(i).get(0);

                recessiveMoI &= recessiveCompleteSet.contains(genotype);
                xRecessiveMoI &= xLinkedRecessiveSet.contains(genotype);
            }

            if (recessiveMoI) {
                addVariantToList(variant, variantList, variantMoIMap, BIALLELIC);
            }
            if (xRecessiveMoI) {
                addVariantToList(variant, variantList, variantMoIMap, XLINKED_BIALLELIC);
            }

            checkCompoundHeterozygous(variant, proband, compoundHeterozygousMap);
        }

        // Create instance of reported variant creator to obtain reported variants
        List<DiseasePanel> biodataDiseasPanelList = diseasePanels.stream().map(Panel::getDiseasePanel).collect(Collectors.toList());
        TieringReportedVariantCreator creator = new TieringReportedVariantCreator(biodataDiseasPanelList, phenotype,
                ClinicalProperty.Penetrance.COMPLETE);
        List<ReportedVariant> reportedVariants;
        try {
            reportedVariants = creator.create(variantList, variantMoIMap);
        } catch (InterpretationAnalysisException e) {
            throw new AnalysisException(e.getMessage(), e);
        }


        String userId;
        try {
            userId = catalogManager.getUserManager().getUserId(token);
        } catch (CatalogException e) {
            throw new AnalysisException(e.getMessage(), e);
        }

        QueryResult<User> userQueryResult;
        try {
            userQueryResult = catalogManager.getUserManager().get(userId, new QueryOptions(QueryOptions.INCLUDE,
                    Arrays.asList(UserDBAdaptor.QueryParams.EMAIL.key(), UserDBAdaptor.QueryParams.ORGANIZATION.key())), token);
        } catch (CatalogException e) {
            throw new AnalysisException(e.getMessage(), e);
        }

        List<ReportedLowCoverage> reportedLowCoverages = new ArrayList<>();
        if (config.getBoolean("lowRegionCoverage", false)) {
            // Reported low coverage map
            Set<String> lowCoverageByGeneDone = new HashSet<>();


            // Look for the bam file of the proband
            QueryResult<File> fileQueryResult;
            try {
                fileQueryResult = catalogManager.getFileManager().get(studyStr, new Query()
                                .append(FileDBAdaptor.QueryParams.SAMPLES.key(), clinicalAnalysis.getProband().getSamples().get(0).getId())
                                .append(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.BAM),
                        new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.UUID.key()), token);
            } catch (CatalogException e) {
                throw new AnalysisException(e.getMessage(), e);
            }
            if (fileQueryResult.getNumResults() > 1) {
                throw new AnalysisException("More than one BAM file found for proband " + proband.getId() + " in clinical analysis "
                        + clinicalAnalysisId);
            }

            String bamFileId = fileQueryResult.getNumResults() == 1 ? fileQueryResult.first().getUuid() : null;

            if (bamFileId != null) {
                for (Panel diseasePanel : diseasePanels) {
                    for (DiseasePanel.GenePanel genePanel : diseasePanel.getDiseasePanel().getGenes()) {
                        String geneName = genePanel.getId();
                        if (!lowCoverageByGeneDone.contains(geneName)) {
                            reportedLowCoverages.addAll(getReportedLowCoverages(geneName, bamFileId, maxCoverage));
                            lowCoverageByGeneDone.add(geneName);
                        }
                    }
                }
            }
        }

        // Create Interpretation
        Interpretation interpretation = new Interpretation()
                .setId("OpenCGA-Tiering-" + TimeUtils.getTime())
                .setAnalyst(new Analyst(userId, userQueryResult.first().getEmail(), userQueryResult.first().getOrganization()))
                .setClinicalAnalysisId(clinicalAnalysisId)
                .setCreationDate(TimeUtils.getTime())
                .setPanels(biodataDiseasPanelList)
                .setFilters(null) //TODO
                .setSoftware(new Software().setName("Tiering"))
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

    private void checkCompoundHeterozygous(Variant variant, Individual proband, Map<String, List<Variant>> compoundHeterozygousMap) {
        String probandGT = variant.getStudies().get(0).getSampleData(proband.getSamples().get(0).getId(), "GT");
        if (probandGT.equals("0/1")) {
            String mother = variant.getStudies().get(0).getSampleData(proband.getMother().getSamples().get(0).getId(), "GT");
            String father = variant.getStudies().get(0).getSampleData(proband.getFather().getSamples().get(0).getId(), "GT");
            if ((father.equals("0/0") && mother.equals("0/1")) || (father.equals("0/1") && mother.equals("0/0"))) {
                // We first create a set of genes to avoid adding the same variant several times to the same gene
                Set<String> allGenes = new HashSet<>();
                for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
                    allGenes.add(consequenceType.getGeneName());
                }
                // Add the variant to the map
                for (String gene : allGenes) {
                    if (!compoundHeterozygousMap.containsKey(gene)) {
                        compoundHeterozygousMap.put(gene, new ArrayList<>());
                    }
                    compoundHeterozygousMap.get(gene).add(variant);
                }
            }
        }
    }

    private void addVariantToList(Variant variant, List<Variant> variantList,
                                  Map<String, List<ClinicalProperty.ModeOfInheritance>> variantMoIMap,
                                  ClinicalProperty.ModeOfInheritance moi) {
        if (!variantMoIMap.containsKey(variant.getId())) {
            variantList.add(variant);
            variantMoIMap.put(variant.getId(), new ArrayList<>());
        }
        variantMoIMap.get(variant.getId()).add(moi);
    }

    private Set<String> genotypeMapToSet(Map<String, List<String>> genotypes) {
        Set<String> individualGenotypesSet = new HashSet<>();
        for (Map.Entry<String, List<String>> entry : genotypes.entrySet()) {
            String individual = entry.getKey();
            for (String genotype : entry.getValue()) {
                individualGenotypesSet.add(individual + "__" + genotype);
            }
        }
        return individualGenotypesSet;
    }
//
//    void processCompoundHeterozygous(ClinicalAnalysis clinicalAnalysis, Pedigree pedigree, Phenotype phenotype, Query query, Map<String, ReportedVariant> reportedVariantMap, Panel diseasePanel) throws Exception {
//        VariantQueryResult<Variant> variantQueryResult;
//        Map<String, List<String>> probandGenotype;
//
//        // Calculate compound heterozygous
//        probandGenotype = new HashMap<>();
//        probandGenotype.put(clinicalAnalysis.getProband().getId(),
//                Arrays.asList(ModeOfInheritance.toGenotypeString(ModeOfInheritance.GENOTYPE_0_1)));
//        putGenotypes(probandGenotype, query);
//        for (GenePanel gene : diseasePanel.getDiseasePanel().getGenes()) {
//            query.put(VariantQueryParam.ANNOT_XREF.key(), gene);
//
//            variantQueryResult = variantStorageManager.get(query, QueryOptions.empty(), token);
//
//            List<Variant> compoundHetVariantList = ModeOfInheritance.compoundHeterozygosity(pedigree,
//                    variantQueryResult.getResult().iterator());
//            // TODO: We need to create another ReportedModeOfInheritance for compound heterozygous!!??
//            generateReportedVariants(compoundHetVariantList, phenotype, diseasePanel,
//                    ClinicalProperty.ModeOfInheritance.COMPOUND_HETEROZYGOUS, reportedVariantMap);
//        }
//    }
//
//    void processDeNovo(ClinicalAnalysis clinicalAnalysis, Pedigree pedigree, Phenotype phenotype, Query query,
//                       Map<String, ReportedVariant> reportedVariantMap, Panel diseasePanel)
//            throws CatalogException, StorageEngineException, IOException {
//        VariantQueryResult<Variant> variantQueryResult;
//        Map<String, List<String>> probandGenotype = new HashMap<>();
//        probandGenotype.put(clinicalAnalysis.getProband().getId(),
//                Arrays.asList(ModeOfInheritance.toGenotypeString(ModeOfInheritance.GENOTYPE_0_0)));
//        putGenotypesNegated(probandGenotype, query);
//
//        variantQueryResult = variantStorageManager.get(query, QueryOptions.empty(), token);
//
//        List<Variant> deNovoVariantList = ModeOfInheritance.deNovoVariants(pedigree.getProband(),
//                variantQueryResult.getResult().iterator());
//        // TODO: We need to create another ReportedModeOfInheritance for de novo!!??
//        generateReportedVariants(deNovoVariantList, phenotype, diseasePanel, ClinicalProperty.ModeOfInheritance.DE_NOVO,
//                reportedVariantMap);
//    }
//
//    void queryAndGenerateReport(Phenotype phenotype, Query query, Map<String, ReportedVariant> reportedVariantMap,
//                                Panel diseasePanel, ClinicalProperty.ModeOfInheritance moi, Penetrance penetrance,
//                                Map<String, List<String>> genotypes) throws CatalogException, StorageEngineException, IOException {
//        VariantQueryResult<Variant> variantQueryResult;
//        putGenotypes(genotypes, query);
//        variantQueryResult = variantStorageManager.get(query, QueryOptions.empty(), token);
//        generateReportedVariants(variantQueryResult, phenotype, diseasePanel, moi, penetrance, reportedVariantMap);
//    }

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

//    private void putGenotypes(Map<String, List<String>> genotypes, Query query) {
//        query.put(VariantQueryParam.GENOTYPE.key(),
//                StringUtils.join(genotypes.entrySet().stream()
//                        .map(entry -> entry.getKey() + ":" + StringUtils.join(entry.getValue(), VariantQueryUtils.OR))
//                        .collect(Collectors.toList()), ";"));
//
//    }
//
//    private void putGenotypesNegated(Map<String, List<String>> genotypes, Query query) {
//        query.put(VariantQueryParam.GENOTYPE.key(),
//                StringUtils.join(genotypes.entrySet().stream()
//                        .map(entry -> entry.getKey() + IS + StringUtils.join(NOT + entry.getValue(), AND))
//                        .collect(Collectors.toList()), AND));
//    }
//
//    private void generateReportedVariants(VariantQueryResult<Variant> variantQueryResult, Phenotype phenotype, Panel diseasePanel,
//                                          ClinicalProperty.ModeOfInheritance moi, Penetrance penetrance,
//                                          Map<String, ReportedVariant> reportedVariantMap) {
//        for (Variant variant: variantQueryResult.getResult()) {
//            if (!reportedVariantMap.containsKey(variant.getId())) {
//                reportedVariantMap.put(variant.getId(), new ReportedVariant(variant.getImpl(), 0, new ArrayList<>(),
//                        Collections.emptyList(), Collections.emptyMap()));
//            }
//            ReportedVariant reportedVariant = reportedVariantMap.get(variant.getId());
//
//            // Sanity check
//            if (variant.getAnnotation() != null && ListUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
//                for (ConsequenceType ct: variant.getAnnotation().getConsequenceTypes()) {
//                    // Create the reported event
//                    ReportedEvent reportedEvent = new ReportedEvent()
//                            .setId("JT-PF-" + reportedVariant.getReportedEvents().size())
//                            .setPhenotypes(Collections.singletonList(phenotype))
//                            .setConsequenceTypeIds(Collections.singletonList(ct.getBiotype()))
//                            .setGenomicFeature(new GenomicFeature(ct.getEnsemblGeneId(), ct.getEnsemblTranscriptId(), ct.getGeneName(),
//                                    null, null))
//                            .setModeOfInheritance(moi)
//                            .setPanelId(diseasePanel.getDiseasePanel().getId())
//                            .setPenetrance(penetrance);
//
//                    // TODO: add additional reported event fields
//
//                    // Add reported event to the reported variant
//                    reportedVariant.getReportedEvents().add(reportedEvent);
//                }
//            }
//        }
//    }
//
//    private void generateReportedVariants(List<Variant> variantList, Phenotype phenotype, Panel diseasePanel,
//                                          ClinicalProperty.ModeOfInheritance moi, Map<String, ReportedVariant> reportedVariantMap) {
//        for (Variant variant : variantList) {
//            if (!reportedVariantMap.containsKey(variant.getId())) {
//                reportedVariantMap.put(variant.getId(), new ReportedVariant(variant.getImpl(), 0, new ArrayList<>(),
//                        Collections.emptyList(), Collections.emptyMap()));
//            }
//            ReportedVariant reportedVariant = reportedVariantMap.get(variant.getId());
//
//            // Sanity check
//            if (variant.getAnnotation() != null && ListUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
//                for (ConsequenceType ct: variant.getAnnotation().getConsequenceTypes()) {
//                    // Create the reported event
//                    ReportedEvent reportedEvent = new ReportedEvent()
//                            .setId("JT-PF-" + reportedVariant.getReportedEvents().size())
//                            .setPhenotypes(Collections.singletonList(phenotype))
//                            .setConsequenceTypeIds(Collections.singletonList(ct.getBiotype()))
//                            .setGenomicFeature(new GenomicFeature(ct.getEnsemblGeneId(), ct.getEnsemblTranscriptId(), ct.getGeneName(),
//                                    null, null))
//                            .setModeOfInheritance(moi)
//                            .setPanelId(diseasePanel.getDiseasePanel().getId());
//
//                    // TODO: add additional reported event fields
//
//                    // Add reported event to the reported variant
//                    reportedVariant.getReportedEvents().add(reportedEvent);
//                }
//            }
//        }
//    }
//
//    private List<ReportedVariant> dominant() {
//        return null;
//    }
//
//    private List<ReportedVariant> recessive() {
//        // MoI -> genotypes
//        // Variant Query query -> (biotype, gene, genoptype)
//        // Iterator for (Var) -> getReportedEvents(rv)
//        // create RV
//        return null;
//    }
//
//
//    private List<ReportedEvent> getReportedEvents(Variant variant) {
//        return null;
//    }
//
//    private Interpretation createInterpretation() {
//        return null;
//    }
}
