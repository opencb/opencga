package org.opencb.opencga.analysis.clinical;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.collections.MapUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.analysis.AnalysisResult;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.util.*;

public class CompoundHeterozygousAnalysis extends FamilyAnalysis<List<ReportedVariant>> {

    private Query query;

    private static Query defaultQuery;
    private static Set<String> extendedLof;
    private static Set<String> proteinCoding;

    static {
        proteinCoding = new HashSet<>(Arrays.asList("protein_coding", "IG_C_gene", "IG_D_gene", "IG_J_gene", "IG_V_gene",
                "nonsense_mediated_decay", "non_stop_decay", "TR_C_gene", "TR_D_gene", "TR_J_gene", "TR_V_gene"));

        extendedLof = new HashSet<>(Arrays.asList("SO:0001893", "SO:0001574", "SO:0001575", "SO:0001587", "SO:0001589", "SO:0001578",
                "SO:0001582", "SO:0001889", "SO:0001821", "SO:0001822", "SO:0001583", "SO:0001630", "SO:0001626"));

        defaultQuery = new Query()
                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), proteinCoding)
                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), extendedLof);
    }

    public CompoundHeterozygousAnalysis(String clinicalAnalysisId, List<String> diseasePanelIds, Query query,
                                        Map<String, ClinicalProperty.RoleInCancer> roleInCancer,
                                        Map<String, List<String>> actionableVariants, ObjectMap config, String studyStr, String opencgaHome,
                                        String token) {
        super(clinicalAnalysisId, diseasePanelIds, roleInCancer, actionableVariants, config, studyStr, opencgaHome, token);
        this.query = new Query(defaultQuery);
        this.query.append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.STUDY.key(), studyStr)
                .append(VariantQueryParam.FILTER.key(), VCFConstants.PASSES_FILTERS_v4)
                .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./.");

        if (MapUtils.isNotEmpty(query)) {
            this.query.putAll(query);
        }

    }

    @Override
    public AnalysisResult<List<ReportedVariant>> execute() throws Exception {
        return null;
    }
//        StopWatch watcher = StopWatch.createStarted();
//
//        // Get and check clinical analysis and proband
//        ClinicalAnalysis clinicalAnalysis = getClinicalAnalysis();
//        Individual proband = getProband(clinicalAnalysis);
//
//        // Get disease panels from IDs
//        List<Panel> diseasePanels = getDiseasePanelsFromIds(diseasePanelIds);
//
//        // Get pedigree
//        Pedigree pedigree = FamilyManager.getPedigreeFromFamily(clinicalAnalysis.getFamily());
//
//        // Get the map of individual - sample id and update proband information (to be able to navigate to the parents and their
//        // samples easily)
//        Map<String, String> sampleMap = getSampleMap(clinicalAnalysis, proband);
//
//        Map<String, List<String>> genotypeMap = ModeOfInheritance.compoundHeterozygous(pedigree);
//
//        List<String> samples = new ArrayList<>();
//        List<String> genotypeList = new ArrayList<>();
//
//        for (Map.Entry<String, List<String>> entry : genotypeMap.entrySet()) {
//            if (sampleMap.containsKey(entry.getKey())) {
//                samples.add(entry.getKey());
//                genotypeList.add(sampleMap.get(entry.getKey()) + ":" + StringUtils.join(entry.getValue(), VariantQueryUtils.OR));
//            }
//        }
//        query.put(VariantQueryParam.GENOTYPE.key(), StringUtils.join(genotypeList, ";"));
//
//        VariantQueryResult<Variant> variantQueryResult = variantStorageManager.get(query, QueryOptions.empty(), token);
//
//        logger.debug("Number of variants retrieved: {}", variantQueryResult.getResult().size());
//
//        // Map: transcript to pair (pair-left for mother and pair-right for father)
//        Map<String, Pair<List<Variant>, List<Variant>>> transcriptToVariantsMap = new HashMap<>();
//
//        int fatherSampleIdx = samples.indexOf(proband.getFather().getSamples().get(0).getId());
//        int motherSampleIdx = samples.indexOf(proband.getMother().getSamples().get(0).getId());
//
//        for (Variant variant : variantQueryResult.getResult()) {
//            StudyEntry studyEntry = variant.getStudies().get(0);
//            int gtIdx = studyEntry.getFormat().indexOf("GT");
//
//            String fatherGenotype = studyEntry.getSampleData(fatherSampleIdx).get(gtIdx);
//            String motherGenotype = studyEntry.getSampleData(motherSampleIdx).get(gtIdx);
//
//            if ((fatherGenotype.contains("1") && motherGenotype.contains("1"))
//                    || (!fatherGenotype.contains("1") && !motherGenotype.contains("1"))) {
//                logger.debug("Skipping variant '{}'. The parents are both 0/0 or 0/1", variant);
//                continue;
//            }
//
//            int pairIndex;
//            if (fatherGenotype.contains("1") && !motherGenotype.contains("1")) {
//                pairIndex = 0;
//            } else if (motherGenotype.contains("1") && !fatherGenotype.contains("1")) {
//                pairIndex = 1;
//            } else {
//                logger.warn("This should never happen!!!");
//                continue;
//            }
//
//            for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
//                if (proteinCoding.contains(consequenceType.getBiotype())) {
//                    String transcriptId = consequenceType.getEnsemblTranscriptId();
//                    if (CollectionUtils.isNotEmpty(consequenceType.getSequenceOntologyTerms())) {
//                        for (SequenceOntologyTerm soTerm : consequenceType.getSequenceOntologyTerms()) {
//                            if (extendedLof.contains(soTerm.getAccession())) {
//                                if (transcriptToVariantsMap.containsKey(transcriptId)) {
//                                    transcriptToVariantsMap.put(transcriptId, Pair.of(new ArrayList<>(), new ArrayList<>()));
//                                }
//                                if (pairIndex == 0) {
//                                    // From mother
//                                    transcriptToVariantsMap.get(transcriptId).getLeft().add(variant);
//                                } else {
//                                    // From father
//                                    transcriptToVariantsMap.get(transcriptId).getRight().add(variant);
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//        }
//
//        // Creating reported variants
//        List<ReportedVariant> reportedVariants;
//        List<DiseasePanel> biodataDiseasePanelList = diseasePanels.stream().map(Panel::getDiseasePanel).collect(Collectors.toList());
//        TieringReportedVariantCreator creator = new TieringReportedVariantCreator(biodataDiseasePanelList, roleInCancer, actionableVariants,
//                clinicalAnalysis.getDisorder(), null, ClinicalProperty.Penetrance.COMPLETE);
//
//        try {
//            reportedVariants = creator.create(variantList, transcriptToVariantsMap);
//        } catch (InterpretationAnalysisException e) {
//            throw new AnalysisException(e.getMessage(), e);
//        }
//
//
//        TieringReportedVariantCreator creator = new TieringReportedVariantCreator();
//
//        List<ReportedVariant> reportedVariantList = new ArrayList<>(transcriptToVariantsMap.size());
//
//        int counter = 0;
//        for (Map.Entry<String, Pair<List<Variant>, List<Variant>>> entry : transcriptToVariantsMap.entrySet()) {
//            if (entry.getValue().getLeft().size() > 0 && entry.getValue().getRight().size() > 0) {
//                for (Variant variant : entry.getValue().getLeft()) {
//
//                }
//            }
//        }
//
//
//        int numResults = reportedVariantList.size();
//
//        return new AnalysisResult<>(
//                reportedVariants,
//                Math.toIntExact(watcher.getTime()),
//                new HashMap<>(),
//                Math.toIntExact(watcher.getTime()), // DB time
//                numResults,
//                numResults,
//                "", // warning message
//                ""); // error message
//
//
//
//        List<Future<Boolean>> futureList = new ArrayList<>(6);
//        futureList.add(threadPool.submit(getNamedThread(MONOALLELIC.name(),
//                () -> query(pedigree, clinicalAnalysis.getDisorder(), sampleMap, MONOALLELIC, resultMap))));
//        futureList.add(threadPool.submit(getNamedThread(XLINKED_MONOALLELIC.name(),
//                () -> query(pedigree, clinicalAnalysis.getDisorder(), sampleMap, XLINKED_MONOALLELIC, resultMap))));
//        futureList.add(threadPool.submit(getNamedThread(YLINKED.name(),
//                () -> query(pedigree, clinicalAnalysis.getDisorder(), sampleMap, YLINKED, resultMap))));
//        futureList.add(threadPool.submit(getNamedThread(BIALLELIC.name(),
//                () -> query(pedigree, clinicalAnalysis.getDisorder(), sampleMap, BIALLELIC, resultMap))));
//        futureList.add(threadPool.submit(getNamedThread(XLINKED_BIALLELIC.name(),
//                () -> query(pedigree, clinicalAnalysis.getDisorder(), sampleMap, XLINKED_BIALLELIC, resultMap))));
//        futureList.add(threadPool.submit(getNamedThread(MITOCHRONDRIAL.name(),
//                () -> query(pedigree, clinicalAnalysis.getDisorder(), sampleMap, MITOCHRONDRIAL, resultMap))));
//        threadPool.shutdown();
//
//        threadPool.awaitTermination(1, TimeUnit.MINUTES);
//        if (!threadPool.isTerminated()) {
//            for (Future<Boolean> future : futureList) {
//                future.cancel(true);
//            }
//        }
//
//        List<Variant> variantList = new ArrayList<>();
//        Map<String, List<ClinicalProperty.ModeOfInheritance>> variantMoIMap = new HashMap<>();
//
//        for (Map.Entry<ClinicalProperty.ModeOfInheritance, VariantQueryResult<Variant>> entry : resultMap.entrySet()) {
//            logger.debug("MOI: {}; variant size: {}; variant ids: {}", entry.getKey(), entry.getValue().getResult().size(),
//                    entry.getValue().getResult().stream().map(Variant::toString).collect(Collectors.joining(",")));
//
//            for (Variant variant : entry.getValue().getResult()) {
//                if (!variantMoIMap.containsKey(variant.getId())) {
//                    variantMoIMap.put(variant.getId(), new ArrayList<>());
//                    variantList.add(variant);
//                }
//                variantMoIMap.get(variant.getId()).add(entry.getKey());
//            }
//        }
//
//        // Primary findings,
//        List<ReportedVariant> primaryFindings;
//        List<DiseasePanel> biodataDiseasePanelList = diseasePanels.stream().map(Panel::getDiseasePanel).collect(Collectors.toList());
//        TieringReportedVariantCreator creator = new TieringReportedVariantCreator(biodataDiseasePanelList, roleInCancer, actionableVariants,
//                clinicalAnalysis.getDisorder(), null, ClinicalProperty.Penetrance.COMPLETE);
//        try {
//            primaryFindings = creator.create(variantList, variantMoIMap);
//        } catch (InterpretationAnalysisException e) {
//            throw new AnalysisException(e.getMessage(), e);
//        }
//
//        // Secondary findings, if clinical consent is TRUE
//        List<ReportedVariant> secondaryFindings = getSecondaryFindings(clinicalAnalysis, primaryFindings,
//                new ArrayList<>(sampleMap.keySet()), creator);
//
//        logger.debug("Variant size: {}", variantList.size());
//        logger.debug("Reported variant size: {}", primaryFindings.size());
//
//        // Reported low coverage
//        List<ReportedLowCoverage> reportedLowCoverages = new ArrayList<>();
//        if (config.getBoolean("lowRegionCoverage", false)) {
//            reportedLowCoverages = getReportedLowCoverage(clinicalAnalysis, diseasePanels);
//        }
//
//        // Create Interpretation
//        Interpretation interpretation = new Interpretation()
//                .setId("OpenCGA-Tiering-" + TimeUtils.getTime())
//                .setAnalyst(getAnalyst(token))
//                .setClinicalAnalysisId(clinicalAnalysisId)
//                .setCreationDate(TimeUtils.getTime())
//                .setPanels(biodataDiseasePanelList)
//                .setFilters(null) //TODO
//                .setSoftware(new Software().setName("Tiering"))
//                .setPrimaryFindings(primaryFindings)
//                .setSecondaryFindings(secondaryFindings)
//                .setReportedLowCoverages(reportedLowCoverages);
//
//        // Return interpretation result
//        int numResults = CollectionUtils.isEmpty(primaryFindings) ? 0 : primaryFindings.size();
//        return new InterpretationResult(
//                interpretation,
//                Math.toIntExact(watcher.getTime()),
//                new HashMap<>(),
//                Math.toIntExact(watcher.getTime()), // DB time
//                numResults,
//                numResults,
//                "", // warning message
//                ""); // error message
//    }

}
