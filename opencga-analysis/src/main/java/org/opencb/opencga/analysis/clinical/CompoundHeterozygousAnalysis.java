package org.opencb.opencga.analysis.clinical;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.clinical.interpretation.exceptions.InterpretationAnalysisException;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.biodata.tools.clinical.TieringReportedVariantCreator;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisResult;
import org.opencb.opencga.analysis.exceptions.AnalysisException;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.core.models.Panel;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;

import java.util.*;
import java.util.stream.Collectors;

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
        StopWatch watcher = StopWatch.createStarted();

        // Get and check clinical analysis and proband
        ClinicalAnalysis clinicalAnalysis = getClinicalAnalysis();
        Individual proband = getProband(clinicalAnalysis);

        // Get disease panels from IDs
        List<Panel> diseasePanels = getDiseasePanelsFromIds(diseasePanelIds);

        // Get pedigree
        Pedigree pedigree = FamilyManager.getPedigreeFromFamily(clinicalAnalysis.getFamily(), proband.getId());

        // Get the map of individual - sample id and update proband information (to be able to navigate to the parents and their
        // samples easily)
        Map<String, String> sampleMap = getSampleMap(clinicalAnalysis, proband);

        Map<String, List<String>> genotypeMap = ModeOfInheritance.compoundHeterozygous(pedigree);
        logger.debug("CH Clinical Anal: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(clinicalAnalysis));
        logger.debug("CH Pedigree: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(pedigree));
        logger.debug("CH Pedigree proband: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(pedigree.getProband()));
        logger.debug("CH Genotype: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(genotypeMap));
        logger.debug("CH Proband: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(proband));
        logger.debug("CH Sample map: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(sampleMap));

        List<String> samples = new ArrayList<>();
        List<String> genotypeList = new ArrayList<>();

        for (Map.Entry<String, List<String>> entry : genotypeMap.entrySet()) {
            if (sampleMap.containsKey(entry.getKey())) {
                samples.add(sampleMap.get(entry.getKey()));
                genotypeList.add(sampleMap.get(entry.getKey()) + ":" + StringUtils.join(entry.getValue(), VariantQueryUtils.OR));
            }
        }
        if (genotypeList.isEmpty()) {
            logger.error("No genotypes found");
            return null;
        }
        query.put(VariantQueryParam.GENOTYPE.key(), StringUtils.join(genotypeList, ";"));

        logger.debug("CH Query: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(query));
        VariantDBIterator iterator = variantStorageManager.iterator(query, QueryOptions.empty(), token);

        int variantsRetrieved = 0;

        // Map: transcript to pair (pair-left for mother and pair-right for father)
        Map<String, Pair<List<Variant>, List<Variant>>> transcriptToVariantsMap = new HashMap<>();

        int fatherSampleIdx = samples.indexOf(proband.getFather().getSamples().get(0).getId());
        int motherSampleIdx = samples.indexOf(proband.getMother().getSamples().get(0).getId());

        logger.debug("CH - samples: {}; FatherSampleIndex: {}; MotherSampleIndex: {}", samples, fatherSampleIdx, motherSampleIdx);

        while (iterator.hasNext()) {
            Variant variant = iterator.next();
            variantsRetrieved += 1;

            StudyEntry studyEntry = variant.getStudies().get(0);
            int gtIdx = studyEntry.getFormat().indexOf("GT");

            String fatherGenotype = studyEntry.getSampleData(fatherSampleIdx).get(gtIdx);
            String motherGenotype = studyEntry.getSampleData(motherSampleIdx).get(gtIdx);

            if ((fatherGenotype.contains("1") && motherGenotype.contains("1"))
                    || (!fatherGenotype.contains("1") && !motherGenotype.contains("1"))) {
                logger.debug("Skipping variant '{}'. The parents are both 0/0 or 0/1", variant);
                continue;
            }

            int pairIndex;
            if (fatherGenotype.contains("1") && !motherGenotype.contains("1")) {
                pairIndex = 0;
            } else if (motherGenotype.contains("1") && !fatherGenotype.contains("1")) {
                pairIndex = 1;
            } else {
                logger.warn("This should never happen!!!");
                continue;
            }

            for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
                if (proteinCoding.contains(consequenceType.getBiotype())) {
                    String transcriptId = consequenceType.getEnsemblTranscriptId();
                    if (CollectionUtils.isNotEmpty(consequenceType.getSequenceOntologyTerms())) {
                        for (SequenceOntologyTerm soTerm : consequenceType.getSequenceOntologyTerms()) {
                            if (extendedLof.contains(soTerm.getAccession())) {
                                transcriptToVariantsMap.computeIfAbsent(transcriptId, k -> Pair.of(new ArrayList<>(), new ArrayList<>()));
                                if (pairIndex == 0) {
                                    // From mother
                                    transcriptToVariantsMap.get(transcriptId).getLeft().add(variant);
                                } else {
                                    // From father
                                    transcriptToVariantsMap.get(transcriptId).getRight().add(variant);
                                }
                            }
                        }
                    }
                }
            }
        }

        logger.debug("Number of variants analysed: {}", variantsRetrieved);

        List<ClinicalProperty.ModeOfInheritance> modeOfInheritances =
                Collections.singletonList(ClinicalProperty.ModeOfInheritance.COMPOUND_HETEROZYGOUS);
        Map<String, List<ClinicalProperty.ModeOfInheritance>> moiMap = new HashMap<>();

        logger.debug("CH TranscriptsToVariantsMap size: {}", transcriptToVariantsMap.size());

        Map<String, Variant> variantMap = new HashMap<>();
        int numberOfTranscripts = 0;
        for (Map.Entry<String, Pair<List<Variant>, List<Variant>>> entry : transcriptToVariantsMap.entrySet()) {
            if (entry.getValue().getLeft().size() > 0 && entry.getValue().getRight().size() > 0) {
                numberOfTranscripts++;
                for (Variant variant : entry.getValue().getLeft()) {
                    variantMap.put(variant.getId(), variant);
                    moiMap.put(variant.getId(), modeOfInheritances);
                }
                for (Variant variant : entry.getValue().getRight()) {
                    variantMap.put(variant.getId(), variant);
                    moiMap.put(variant.getId(), modeOfInheritances);
                }
            }
        }
        logger.debug("CH numberOfTranscripts: {}", numberOfTranscripts);
        logger.debug("CH moiMap size: {}", moiMap.size());

        // Creating reported variants
        List<ReportedVariant> reportedVariants;
        List<DiseasePanel> biodataDiseasePanelList = diseasePanels.stream().map(Panel::getDiseasePanel).collect(Collectors.toList());
        TieringReportedVariantCreator creator = new TieringReportedVariantCreator(biodataDiseasePanelList, roleInCancer, actionableVariants,
                clinicalAnalysis.getDisorder(), null, ClinicalProperty.Penetrance.COMPLETE);
        try {
            reportedVariants = creator.create(new ArrayList<>(variantMap.values()), moiMap);
        } catch (InterpretationAnalysisException e) {
            throw new AnalysisException(e.getMessage(), e);
        }

        logger.debug("CH time: {}", watcher.getTime());
        return new AnalysisResult<>(reportedVariants, Math.toIntExact(watcher.getTime()), null);
    }

}
