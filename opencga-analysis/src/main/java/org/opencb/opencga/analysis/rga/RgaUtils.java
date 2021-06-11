package org.opencb.opencga.analysis.rga;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.analysis.rga.exceptions.RgaException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutVariant;
import org.opencb.opencga.storage.core.variant.query.KeyOpValue;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opencb.opencga.analysis.rga.RgaDataModel.*;

class RgaUtils {

    private static final Pattern OPERATION_PATTERN = Pattern.compile("^([^=<>~!]*)(<?<=?|>>?=?|!=?|!?=?~|==?)([^=<>~!]+.*)$");

    static final String SEPARATOR = "__";
    static final String INNER_SEPARATOR = "--";

    private static final Map<String, String> ENCODE_MAP;
    private static final Map<String, String> DECODE_MAP;

    private static final List<Float> POP_FREQS;
    private static final List<String> POP_FREQ_STUDIES;

    public static final String THOUSAND_GENOMES_STUDY = "1kG_phase3";
    public static final String GNOMAD_GENOMES_STUDY = "GNOMAD_GENOMES";

    public static final String PASS = "PASS";
    public static final String NOT_PASS = "NOT_PASS";

    public static final Set<String> ALL_PARAMS;
    public static final Map<String, Set<String>> PARAM_TYPES;

    private static final Logger logger;

    static {
        logger = LoggerFactory.getLogger(RgaUtils.class);

        ENCODE_MAP = new HashMap<>();

        // KNOCKOUT TYPE
        ENCODE_MAP.put(KnockoutVariant.KnockoutType.COMP_HET.name(), "CH");
        ENCODE_MAP.put(KnockoutVariant.KnockoutType.HOM_ALT.name(), "HOA");
        ENCODE_MAP.put(KnockoutVariant.KnockoutType.HET_ALT.name(), "HEA");
        ENCODE_MAP.put(KnockoutVariant.KnockoutType.DELETION_OVERLAP.name(), "DO");

        // FILTER
        ENCODE_MAP.put(PASS, "P");
        ENCODE_MAP.put(NOT_PASS, "NP");

        // CONSEQUENCE TYPE
        List<String> consequenceTypeList = Arrays.asList("start_retained_variant", "upstream_variant", "3_prime_UTR_variant",
                "splice_acceptor_variant", "transcript_amplification", "upstream_gene_variant", "RNA_polymerase_promoter",
                "non_coding_transcript_exon_variant", "non_coding_transcript_variant", "inframe_variant", "transcript_ablation",
                "splice_donor_variant", "synonymous_variant", "feature_elongation", "feature_truncation", "miRNA_target_site",
                "exon_variant", "downstream_gene_variant", "stop_retained_variant", "TF_binding_site_variant", "initiator_codon_variant",
                "coding_sequence_variant", "protein_altering_variant", "intergenic_variant", "terminator_codon_variant",
                "frameshift_variant", "DNAseI_hypersensitive_site", "feature_variant", "2KB_downstream_variant", "intron_variant",
                "splice_region_variant", "5_prime_UTR_variant", "SNP", "stop_gained", "regulatory_region_amplification",
                "2KB_upstream_variant", "miRNA", "lincRNA", "start_lost", "SNV", "CpG_island", "downstream_variant",
                "NMD_transcript_variant", "2KB_downstream_gene_variant", "TFBS_amplification", "missense_variant",
                "regulatory_region_ablation", "mature_miRNA_variant", "stop_lost", "structural_variant", "regulatory_region_variant",
                "TFBS_ablation", "copy_number_change", "2KB_upstream_gene_variant", "polypeptide_variation_site", "inframe_deletion",
                "inframe_insertion", "incomplete_terminal_codon_variant");
        for (String consequenceType : consequenceTypeList) {
            ENCODE_MAP.put(consequenceType, String.valueOf(VariantQueryUtils.parseConsequenceType(consequenceType)));
        }

        // POPULATION FREQUENCY
        POP_FREQ_STUDIES = Arrays.asList(THOUSAND_GENOMES_STUDY, GNOMAD_GENOMES_STUDY);
        POP_FREQS = Arrays.asList(0f, 0.0001f, 0.0005f, 0.001f, 0.005f, 0.01f, 0.05f, 1f);
        for (int i = 1; i <= POP_FREQ_STUDIES.size(); i++) {
            String popFreqStudy = POP_FREQ_STUDIES.get(i - 1);
            for (int j = 1; j <= POP_FREQS.size(); j++) {
                Float popFreq = POP_FREQS.get(j - 1);
                ENCODE_MAP.put(popFreqStudy.toUpperCase() + SEPARATOR + popFreq, "P" + i + "-" + j);
            }
        }

        // Generate decodeMap
        DECODE_MAP = new HashMap<>();
        for (Map.Entry<String, String> entry : ENCODE_MAP.entrySet()) {
            DECODE_MAP.put(entry.getValue(), entry.getKey());
        }

        // All params
        ALL_PARAMS = new HashSet<>();
        ALL_PARAMS.add(RgaDataModel.ID);
        ALL_PARAMS.add(RgaDataModel.INDIVIDUAL_ID);
        ALL_PARAMS.add(RgaDataModel.SAMPLE_ID);
        ALL_PARAMS.add(RgaDataModel.SEX);
        ALL_PARAMS.add(RgaDataModel.PHENOTYPES);
        ALL_PARAMS.add(RgaDataModel.DISORDERS);
        ALL_PARAMS.add(RgaDataModel.GENE_ID);
        ALL_PARAMS.add(RgaDataModel.GENE_NAME);
        ALL_PARAMS.add(RgaDataModel.GENE_BIOTYPE);
        ALL_PARAMS.add(RgaDataModel.CHROMOSOME);
        ALL_PARAMS.add(RgaDataModel.STRAND);
        ALL_PARAMS.add(RgaDataModel.START);
        ALL_PARAMS.add(RgaDataModel.END);
        ALL_PARAMS.add(RgaDataModel.TRANSCRIPT_ID);
        ALL_PARAMS.add(RgaDataModel.TRANSCRIPT_BIOTYPE);
        ALL_PARAMS.add(RgaDataModel.VARIANTS);
        ALL_PARAMS.add(RgaDataModel.KNOCKOUT_TYPES);
        ALL_PARAMS.add(RgaDataModel.FILTERS);
        ALL_PARAMS.add(RgaDataModel.CONSEQUENCE_TYPES);
        ALL_PARAMS.add(RgaDataModel.POPULATION_FREQUENCIES);
        ALL_PARAMS.add(RgaDataModel.COMPOUND_FILTERS);
        ALL_PARAMS.add(PHENOTYPE_JSON);
        ALL_PARAMS.add(DISORDER_JSON);

        PARAM_TYPES = new HashMap<>();
        // Variant params
        Set<String> params = new HashSet<>();
        params.add(VARIANTS);
        params.add(KNOCKOUT_TYPES);
        params.add(FILTERS);
        params.add(CONSEQUENCE_TYPES);
        params.add(POPULATION_FREQUENCIES);
        params.add(COMPOUND_FILTERS);
        params.add(PHENOTYPE_JSON);
        params.add(DISORDER_JSON);
        PARAM_TYPES.put("variants", params);

        // Transcript params
        params = new HashSet<>();
        params.add(TRANSCRIPT_ID);
        params.add(TRANSCRIPT_BIOTYPE);
        params.addAll(PARAM_TYPES.get("variants"));
        PARAM_TYPES.put("transcripts", params);

        // Gene params
        params = new HashSet<>();
        params.add(GENE_ID);
        params.add(GENE_NAME);
        params.add(GENE_BIOTYPE);
        params.add(CHROMOSOME);
        params.add(STRAND);
        params.add(START);
        params.add(END);
        params.addAll(PARAM_TYPES.get("transcripts"));
        PARAM_TYPES.put("genes", params);
    }

    static String getPopulationFrequencyKey(Float popFreq) throws RgaException {
        for (Float freq : POP_FREQS) {
            if (popFreq <= freq) {
                return String.valueOf(freq);
            }
        }
        throw new RgaException("Population frequency must be a value between 0 and 1. Passed '" + popFreq + "'.");
    }

    static String encode(String value) throws RgaException {
        if (ENCODE_MAP.containsKey(value)) {
            return ENCODE_MAP.get(value);
        } else {
            throw new RgaException("Unknown filter value '" + value + "'");
        }
    }

    static String decode(String value) throws RgaException {
        if (DECODE_MAP.containsKey(value)) {
            return DECODE_MAP.get(value);
        } else {
            throw new RgaException("Unknown filter value '" + value + "'");
        }
    }

    /** Calculate the list of population frequency values to look for in the db.
     *
     * @param filters A list containing {study}[<|>|<=|>=]{number}. e.g. 1kG_phase3<0.01";
     * @return the list of population frequency values to look for in the db with their corresponding population key.
     * @throws RgaException RgaException.
     */
    static Map<String, List<String>> parsePopulationFrequencyQuery(List<String> filters) throws RgaException {
        Map<String, List<String>> result = new HashMap<>();
        for (String filter : filters) {
            KeyOpValue<String, String> keyOpValue = parseKeyOpValue(filter);
            if (keyOpValue.getKey() == null) {
                throw new RgaException("Unexpected operation '" + filter + "'");
            }

            List<String> values = new LinkedList<>();
            float value = Float.parseFloat(keyOpValue.getValue());
            switch (keyOpValue.getOp()) {
                case "<":
                    for (int i = 0; POP_FREQS.get(i) < value; i++) {
                        Float popFreq = POP_FREQS.get(i);
                        values.add(encode(keyOpValue.getKey().toUpperCase() + SEPARATOR + popFreq));
                    }
                    break;
                case "<=":
                    for (int i = 0; POP_FREQS.get(i) <= value; i++) {
                        Float popFreq = POP_FREQS.get(i);
                        values.add(encode(keyOpValue.getKey().toUpperCase() + SEPARATOR + popFreq));
                    }
                    break;
                case ">":
                    for (int i = POP_FREQS.size() - 1; POP_FREQS.get(i) > value; i--) {
                        Float popFreq = POP_FREQS.get(i);
                        values.add(encode(keyOpValue.getKey().toUpperCase() + SEPARATOR + popFreq));
                    }
                    break;
                case ">=":
                    for (int i = POP_FREQS.size() - 1; POP_FREQS.get(i) >= value; i--) {
                        Float popFreq = POP_FREQS.get(i);
                        values.add(encode(keyOpValue.getKey().toUpperCase() + SEPARATOR + popFreq));
                    }
                    break;
                default:
                    throw new RgaException("Unknown operator '" + keyOpValue.getOp() + "'");
            }

            result.put(keyOpValue.getKey(), values);
        }

        return result;
    }

    static List<String> parseFilterQuery(List<String> filters) throws RgaException {
        List<String> result = new ArrayList<>(filters.size());
        for (String filter : filters) {
            result.add(encode(filter));
        }
        return result;
    }

    static List<String> parseKnockoutTypeQuery(List<String> knockoutValues) throws RgaException {
        List<String> result = new ArrayList<>(knockoutValues.size());
        for (String knockoutValue : knockoutValues) {
            result.add(encode(knockoutValue.toUpperCase()));
        }
        return result;
    }

    /**
     * This method parses a typical key-op-value param such as 'ALL<=0.2' in an array ["ALL", "<=", "0.2"].
     * In case of not having a key, first element will be empty
     * In case of not matching with {@link #OPERATION_PATTERN}, key will be null and will use the default operator "="
     *
     * @param value The key-op-value parameter to be parsed
     * @return KeyOpValue
     */
    static KeyOpValue<String, String> parseKeyOpValue(String value) {
        Matcher matcher = OPERATION_PATTERN.matcher(value);

        if (matcher.find()) {
            return new KeyOpValue<>(matcher.group(1).trim(), matcher.group(2).trim(), matcher.group(3).trim());
        } else {
            return new KeyOpValue<>(null, "=", value.trim());
        }
    }

    public static Set<String> generateCompoundHeterozygousCombinations(List<List<List<String>>> maternalChVariantList,
                                                                       List<List<List<String>>> paternalChVariantList) throws RgaException {
        if (maternalChVariantList.isEmpty() || paternalChVariantList.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> result = new HashSet<>();
        for (List<List<String>> maternalVariant : maternalChVariantList) {
            for (List<List<String>> paternalVariant : paternalChVariantList) {
                result.addAll(generateCompoundHeterozygousPairCombination(maternalVariant, paternalVariant));
            }
        }
        return result;
    }

//    public static Set<String> generateCompoundHeterozygousCombinations(List<List<List<String>>> compoundHeterozygousVariantList)
//            throws RgaException {
//        if (compoundHeterozygousVariantList.size() < 2) {
//            return Collections.emptySet();
//        }
//        Set<String> result = new HashSet<>();
//        for (int i = 0; i < compoundHeterozygousVariantList.size() - 1; i++) {
//            for (int j = i + 1; j < compoundHeterozygousVariantList.size(); j++) {
//                result.addAll(generateCompoundHeterozygousPairCombination(compoundHeterozygousVariantList.get(i),
//                        compoundHeterozygousVariantList.get(j)));
//            }
//        }
//        return result;
//    }

    public static Set<String> generateCompoundHeterozygousPairCombination(List<List<String>> variant1, List<List<String>> variant2)
            throws RgaException {
        List<List<String>> result = new LinkedList<>();
        /* Compound heterozygous combinations:
         * KO - F1 - F2
         * KO - F1 - F2 - CT1 - CT2
         * KO - F1 - F2 - CT1 - CT2 - PF1 - PF2
         * KO - F1 - F2 - PF1 - PF2
         * KO - F1 - F2 - PF' ; where PF' is equivalent to the highest PF of both variants (to easily respond to PF<=x)
         */
        String knockout = RgaUtils.encode(KnockoutVariant.KnockoutType.COMP_HET.name());
        result.add(Collections.singletonList(knockout));

        // Generate combinations: KO - F1 - F2; KO - F1 - F2 - CT1 - CT2; KO - F1 - F2 - CT1 - CT2 - PF1 - PF2; KO - F1 - F2 - PF1 - PF2
        List<List<String>> previousIteration = result;
        for (int i = 1; i < 4; i++) {
            // The list will contain all Filter, CT or PF combinations between variant1 and variant2 in a sorted manner to reduce the
            // number of terms
            List<List<String>> sortedCombinations = generateSortedCombinations(variant1.get(i), variant2.get(i));

            List<List<String>> newResults = new ArrayList<>(previousIteration.size() * sortedCombinations.size());
            for (List<String> previousValues : previousIteration) {
                for (List<String> values : sortedCombinations) {
                    List<String> newValues = new ArrayList<>(previousValues);
                    newValues.addAll(values);
                    newResults.add(newValues);
                }
            }
            if (i == 1) {
                // Remove single Knockout string list because there is already a filter just for that field
                result.clear();
            } else if (i == 2) {
                // Generate this particular combination KO - F1 - F2 - PF1 - PF2
                List<List<String>> sortedPfCombinations = generateSortedCombinations(variant1.get(3), variant2.get(3));
                for (List<String> previousValues : previousIteration) {
                    for (List<String> values : sortedPfCombinations) {
                        List<String> newValues = new ArrayList<>(previousValues);
                        newValues.addAll(values);
                        result.add(newValues);
                    }
                }
            }
            result.addAll(newResults);
            previousIteration = newResults;
        }

        // Generate also combination: KO - F1 - F2 - PF' ; where PF' is equivalent to the highest PF of both variants
        List<List<String>> sortedFilterList = generateSortedCombinations(variant1.get(1), variant2.get(1));
        List<String> simplifiedPopFreqList = generateSimplifiedPopulationFrequencyList(variant1.get(3), variant2.get(3));
        for (List<String> filterList : sortedFilterList) {
            for (String popFreq : simplifiedPopFreqList) {
                List<String> terms = new LinkedList<>();
                terms.add(knockout);
                terms.addAll(filterList);
                terms.add(popFreq);
                result.add(terms);
            }
        }

        Set<String> combinations = new HashSet<>();
        for (List<String> strings : result) {
            combinations.add(StringUtils.join(strings, RgaUtils.SEPARATOR));
        }

        return combinations;
    }

    /**
     * Given a list of [[KO], [FILTER1, FILTER2], [CTs], [PFs]], it will return all possible String combinations merging those values.
     * [KO - FILTER1, KO - FILTER2]
     * [KO - FILTER1 - CT', KO - FILTER2 - CT']
     * [KO - FILTER1 - CT' - PF', KO - FILTER2 - CT' - PF']
     * [KO - FILTER1 - PF', KO - FILTER2 - PF']
     * @param values List of size 4 containing values for the 4 filters.
     * @return a list containing all possible merge combinations to store in the DB.
     */
    public static List<String> generateCombinations(List<List<String>> values) {
        if (values.isEmpty()) {
            return Collections.emptyList();
        }
        List<List<String>> result = new LinkedList<>();
        for (String value : values.get(0)) {
            result.add(Collections.singletonList(value));
        }

        // Generate combinations: KO - F; KO - F - CT; KO -F - CT - PF
        List<List<String>> previousIteration = result;
        for (int i = 1; i < values.size(); i++) {
            List<List<String>> newResults = new ArrayList<>(previousIteration.size() * values.get(i).size());
            for (List<String> previousValues : previousIteration) {
                for (String currentValue : values.get(i)) {
                    List<String> newValues = new ArrayList<>(previousValues);
                    newValues.add(currentValue);
                    newResults.add(newValues);
                }
            }
            if (i == 1) {
                // Remove single Knockout string list because there is already a filter just for that field
                result.clear();
            }
            result.addAll(newResults);
            previousIteration = newResults;
        }

        // Generate also combination: KO - F - PF (no consequence type this time)
        for (String ko : values.get(0)) {
            for (String filter : values.get(1)) {
                for (String popFreq : values.get(3)) {
                    result.add(Arrays.asList(ko, filter, popFreq));
                }
            }
        }

        List<String> combinations = new ArrayList<>(result.size());
        for (List<String> strings : result) {
            combinations.add(StringUtils.join(strings, RgaUtils.SEPARATOR));
        }

        return combinations;
    }

    /**
     * Given two lists it will return a list containing all possible combinations, but sorted. For instance:
     * [A, D, F] - [B, G] will return [[A, B], [A, G], [B, D], [D, G], [B, F], [F, G]]
     *
     * @param list1 List 1.
     * @param list2 List 2.
     * @return A list containing list1-list2 sorted pairwise combinations.
     */
    public static List<List<String>> generateSortedCombinations(List<String> list1, List<String> list2) {
        List<List<String>> results = new ArrayList<>(list1.size() * list2.size());
        for (String v1Term : list1) {
            for (String v2Term : list2) {
                if (StringUtils.compare(v1Term, v2Term) <= 0) {
                    results.add(Arrays.asList(v1Term, v2Term));
                } else {
                    results.add(Arrays.asList(v2Term, v1Term));
                }
            }
        }

        return results;
    }

    /**
     * Given two lists containing frequencies for the same populations, it will return a unified frequency for each population containing
     * the least restrictive value. Example: [P1-12, P2-6] - [P1-15, P2-2] will generate [P1-15, P2-6]
     *
     * @param list1 List containing the population frequencies of variant1.
     * @param list2 List containing the population frequencies of variant2.
     * @return A list containing the least restrictive population frequencies.
     * @throws RgaException If there is an issue with the provided lists.
     */
    private static List<String> generateSimplifiedPopulationFrequencyList(List<String> list1, List<String> list2) throws RgaException {
        if (list1.size() != list2.size()) {
            throw new RgaException("Both lists should be the same size and contain the same population frequency values");
        }

        Map<String, Integer> map1 = new HashMap<>();
        Map<String, Integer> map2 = new HashMap<>();

        for (String terms : list1) {
            String[] split = terms.split("-");
            map1.put(split[0], Integer.parseInt(split[1]));
        }
        for (String terms : list2) {
            String[] split = terms.split("-");
            map2.put(split[0], Integer.parseInt(split[1]));
        }

        List<String> terms = new ArrayList<>(list1.size());
        for (String popFreqKey : map1.keySet()) {
            if (map1.get(popFreqKey) > map2.get(popFreqKey)) {
                terms.add(popFreqKey + "-" + map1.get(popFreqKey));
            } else {
                terms.add(popFreqKey + "-" + map2.get(popFreqKey));
            }
        }

        return terms;
    }

    /**
     * Extract complete list of KnockoutVariants for current RgaDataModel document.
     *
     * @param rgaDataModel RgaDataModel document.
     * @param variantMap   Map of variants.
     * @param variantIds   Set of variants to be included in the result.
     * @return a complete list of KnockoutVariants.
     */
    static List<KnockoutVariant> extractKnockoutVariants(RgaDataModel rgaDataModel, Map<String, Variant> variantMap,
                                                         Set<String> variantIds) {
        List<KnockoutVariant> knockoutVariantList = new LinkedList<>();
        if (rgaDataModel.getVariants() != null) {
            for (int i = 0; i < rgaDataModel.getVariants().size(); i++) {
                String variantId = rgaDataModel.getVariants().get(i);

                if (variantIds.isEmpty() || variantIds.contains(variantId)) {
                    KnockoutVariant knockoutVariant;

                    if (variantMap.containsKey(variantId)) {
                        Variant variant = variantMap.get(variantId);

                        SampleEntry sampleEntry = variant.getStudies().get(0).getSample(rgaDataModel.getSampleId());
                        KnockoutVariant.KnockoutType knockoutType = null;
                        if (CollectionUtils.isNotEmpty(rgaDataModel.getKnockoutTypes())) {
                            knockoutType = KnockoutVariant.KnockoutType.valueOf(rgaDataModel.getKnockoutTypes().get(i));
                        }

                        // Convert just once
                        knockoutVariant = convertToKnockoutVariant(variant, rgaDataModel.getTranscriptId(), sampleEntry, knockoutType);
                    } else if (CollectionUtils.isNotEmpty(rgaDataModel.getVariantSummary())) {
                        String variantSummaryId = rgaDataModel.getVariantSummary().get(i);
                        // Get the basic information from variant summary object
                        try {
                            CodedVariant codedFeature = CodedVariant.parseEncodedId(variantSummaryId);
                            Variant variant = new Variant(variantId);

                            knockoutVariant = new KnockoutVariant()
                                    .setId(codedFeature.getId())
                                    .setDbSnp(codedFeature.getDbSnp())
                                    .setKnockoutType(KnockoutVariant.KnockoutType.valueOf(codedFeature.getKnockoutType()))
                                    .setType(VariantType.valueOf(codedFeature.getType()))
                                    .setChromosome(variant.getChromosome())
                                    .setStart(variant.getStart())
                                    .setEnd(variant.getEnd())
                                    .setLength(variant.getLength());
                        } catch (RgaException e) {
                            logger.warn("Could not parse coded variant {}", variantSummaryId, e);
                            knockoutVariant = new KnockoutVariant().setId(variantId);
                        }
                    } else {
                        Variant variant = new Variant(variantId);
                        knockoutVariant = new KnockoutVariant()
                                .setId(variantId)
                                .setChromosome(variant.getChromosome())
                                .setStart(variant.getStart())
                                .setEnd(variant.getEnd())
                                .setLength(variant.getLength());
                    }
                    knockoutVariantList.add(knockoutVariant);
                }
            }
        }

        return knockoutVariantList;
    }

    static KnockoutVariant convertToKnockoutVariant(Variant variant) {
        return convertToKnockoutVariant(variant, null, null, null);
    }

    // Default converter
    static KnockoutVariant convertToKnockoutVariant(Variant variant, String transcriptId, SampleEntry sampleEntry,
                                                    KnockoutVariant.KnockoutType knockoutType) {
        StudyEntry studyEntry = CollectionUtils.isNotEmpty(variant.getStudies()) ? variant.getStudies().get(0) : null;

        FileEntry fileEntry = studyEntry != null && CollectionUtils.isNotEmpty(studyEntry.getFiles()) && sampleEntry != null
                ? studyEntry.getFiles().get(sampleEntry.getFileIndex())
                : null;
        VariantAnnotation variantAnnotation = variant.getAnnotation();

        ConsequenceType consequenceType = null;
        if (variantAnnotation != null && StringUtils.isNotEmpty(transcriptId)
                && CollectionUtils.isNotEmpty(variantAnnotation.getConsequenceTypes())) {
            for (ConsequenceType ct : variantAnnotation.getConsequenceTypes()) {
                if (transcriptId.equals(ct.getEnsemblTranscriptId())) {
                    consequenceType = ct;
                    break;
                }
            }
        }

        return new KnockoutVariant(variant, studyEntry, fileEntry, sampleEntry, variantAnnotation, consequenceType, knockoutType);
    }

    public static class CodedIndividual extends CodedFeature {
        //  id __ SNV __ COMP_HET __ VR_R __ A_J__numParents
        private int numParents;

        public CodedIndividual(String id, String type, String knockoutType, List<String> consequenceTypeList,
                               String thousandGenomesPopFreq, String gnomadPopFreq, int numParents) {
            super("", id, type, knockoutType, consequenceTypeList, thousandGenomesPopFreq, gnomadPopFreq);
            this.numParents = numParents;
        }

        public static CodedIndividual parseEncodedId(String encodedId) throws RgaException {
            String[] split = encodedId.split(SEPARATOR);
            if (split.length != 6) {
                throw new RgaException("Unexpected individual string received '" + encodedId
                        + "'. Expected {id}__{type}__{knockoutType}__{conseqType}__{popFreqs}__{numParents}");
            }

            Set<String> consequenceType = new HashSet<>(Arrays.asList(split[3].split(INNER_SEPARATOR)));
            String[] popFreqs = split[4].split(INNER_SEPARATOR);

            return new CodedIndividual(split[0], split[1], split[2], new ArrayList<>(consequenceType), popFreqs[0], popFreqs[1],
                    Integer.parseInt(split[5]));
        }

        public String getEncodedId() {
            return getId() + SEPARATOR + getType() + SEPARATOR + getKnockoutType() + SEPARATOR
                    + StringUtils.join(getConsequenceType(), INNER_SEPARATOR) + SEPARATOR
                    + StringUtils.join(getPopulationFrequencies(), INNER_SEPARATOR) + SEPARATOR + numParents;
        }

        public int getNumParents() {
            return numParents;
        }
    }

    public static class CodedVariant extends CodedFeature {
        //  transcriptId__id __ dbSnp __ SNV __ COMP_HET __ clinicalSignificance __ VR_R __ A_J
        private String dbSnp;
        private String parentalOrigin;
        private List<String> clinicalSignificances;

        public CodedVariant(String transcriptId, String id, String dbSnp, String type, String knockoutType, String parentalOrigin,
                            List<String> clinicalSignificances, List<String> consequenceTypeList, String thousandGenomesPopFreq,
                            String gnomadPopFreq) {
            super(transcriptId, id, type, knockoutType, consequenceTypeList, thousandGenomesPopFreq, gnomadPopFreq);
            this.dbSnp = dbSnp;
            this.parentalOrigin = parentalOrigin;
            this.clinicalSignificances = clinicalSignificances;
        }

        public static CodedVariant parseEncodedId(String encodedId) throws RgaException {
            String[] split = encodedId.split(SEPARATOR);
            if (split.length != 8) {
                throw new RgaException("Unexpected variant string received '" + encodedId
                        + "'. Expected {transcriptId}__{id}__{dbSnp}__{type}__{knockoutType}--{parentalOrigin}__{clinicalSignificances}"
                        +  "__{conseqType}__{popFreqs}");
            }

            Set<String> consequenceType = new HashSet<>(Arrays.asList(split[6].split(INNER_SEPARATOR)));
            String[] popFreqs = split[7].split(INNER_SEPARATOR);
            List<String> clinicalSignificances = Collections.emptyList();
            if (StringUtils.isNotEmpty(split[5])) {
                clinicalSignificances = Arrays.asList(split[5].split(INNER_SEPARATOR));
            }
            String[] ktSplit = split[4].split(INNER_SEPARATOR);
            String knockoutType = ktSplit[0];
            String parentalOrigin = ktSplit[1];

            return new CodedVariant(split[0], split[1], split[2], split[3], knockoutType, parentalOrigin, clinicalSignificances,
                    new ArrayList<>(consequenceType), popFreqs[0], popFreqs[1]);
        }

        public String getEncodedId() {
            return getTranscriptId() + SEPARATOR + getId() + SEPARATOR + dbSnp + SEPARATOR + getType() + SEPARATOR + getKnockoutType()
                    + INNER_SEPARATOR + parentalOrigin + SEPARATOR + StringUtils.join(clinicalSignificances, INNER_SEPARATOR) + SEPARATOR
                    + StringUtils.join(getConsequenceType(), INNER_SEPARATOR) + SEPARATOR
                    + StringUtils.join(getPopulationFrequencies(), INNER_SEPARATOR);
        }

        public String getDbSnp() {
            return dbSnp;
        }

        public String getParentalOrigin() {
            return parentalOrigin;
        }

        public List<String> getClinicalSignificances() {
            return clinicalSignificances;
        }
    }

    public static abstract class CodedFeature {
        private String transcriptId;
        private String id;
        private String type;
        private String knockoutType;
        private List<String> populationFrequencies;
        private Set<String> consequenceType;

        public CodedFeature(String transcriptId, String id, String type, String knockoutType, List<String> consequenceTypeList,
                            String thousandGenomesPopFreq, String gnomadPopFreq) {
            this.transcriptId = transcriptId;
            this.id = id;
            this.type = type;
            this.knockoutType = knockoutType;
            this.consequenceType = new HashSet<>();
            for (String consequenceType : consequenceTypeList) {
                this.consequenceType.add(String.valueOf(VariantQueryUtils.parseConsequenceType(consequenceType)));
            }
            this.populationFrequencies = Arrays.asList(thousandGenomesPopFreq, gnomadPopFreq);
        }

        public String getTranscriptId() {
            return transcriptId;
        }

        public String getId() {
            return id;
        }

        public String getType() {
            return type;
        }

        public String getKnockoutType() {
            return knockoutType;
        }

        public Set<String> getConsequenceType() {
            return consequenceType;
        }

        public List<String> getPopulationFrequencies() {
            return populationFrequencies;
        }

        public String getThousandGenomesFrequency() {
            return populationFrequencies.get(0);
        }

        public String getGnomadFrequency() {
            return populationFrequencies.get(1);
        }
    }

    public static class CodedChPairVariants {
        private final static String VARIANT_SEPARATOR;

        static {
            VARIANT_SEPARATOR = "--_--";
        }

        private CodedVariant maternalCodedVariant;
        private CodedVariant paternalCodedVariant;

        public CodedChPairVariants(CodedVariant maternalCodedVariant, CodedVariant paternalCodedVariant) {
            this.maternalCodedVariant = maternalCodedVariant;
            this.paternalCodedVariant = paternalCodedVariant;
        }

        public static CodedChPairVariants parseEncodedId(String encodedId) throws RgaException {
            String[] split = encodedId.split(VARIANT_SEPARATOR);
            if (split.length != 2) {
                throw new RgaException("Unexpected CH variant string received '" + encodedId
                        + "'. Expected {variant1}" + VARIANT_SEPARATOR + "{variant2}");
            }

            return new CodedChPairVariants(decodeEncodedVariantId(split[0]), decodeEncodedVariantId(split[1]));
        }

        public String getEncodedId() {
            return getEncodedVariant(maternalCodedVariant) + VARIANT_SEPARATOR + getEncodedVariant(paternalCodedVariant);
        }

        public CodedVariant getMaternalCodedVariant() {
            return maternalCodedVariant;
        }

        public CodedVariant getPaternalCodedVariant() {
            return paternalCodedVariant;
        }

        private static String getEncodedVariant(CodedVariant codedVariant) {
            return codedVariant.getId() + SEPARATOR + codedVariant.getParentalOrigin() + SEPARATOR
                    + StringUtils.join(codedVariant.getConsequenceType(), INNER_SEPARATOR) + SEPARATOR
                    + StringUtils.join(codedVariant.getPopulationFrequencies(), INNER_SEPARATOR);
        }

        private static CodedVariant decodeEncodedVariantId(String encodedVariant) throws RgaException {
            String[] split = encodedVariant.split(SEPARATOR);
            if (split.length != 4) {
                throw new RgaException("Unexpected encoded variant '" + encodedVariant + "'. "
                        + "Expected {id}__{parentalOrigin}__{conseqType}__{popFreqs}");
            }
            Set<String> consequenceType = new HashSet<>(Arrays.asList(split[2].split(INNER_SEPARATOR)));
            String[] popFreqs = split[3].split(INNER_SEPARATOR);

            return new CodedVariant("", split[0], "", "", KnockoutVariant.KnockoutType.COMP_HET.name(), split[1], Collections.emptyList(),
                    new ArrayList<>(consequenceType), popFreqs[0], popFreqs[1]);
        }

    }

    public static class KnockoutTypeCount {
        private Set<String> variantIdQuery;
        private Set<String> dbSnpQuery;
        private Set<String> typeQuery;
        private Set<String> knockoutTypeQuery;
        private Set<String> clinicalSignificanceQuery;
        private Set<String> consequenceTypeQuery;
        private List<Set<String>> popFreqQuery;

        private Set<String> ids;
        private Map<String, Set<String>> transcriptCompHetIdsMap;
        private Set<String> homIds;
        private Set<String> hetIds;
        private Set<String> delOverlapIds;

        public KnockoutTypeCount(Query query) throws RgaException {
            variantIdQuery = new HashSet<>();
            dbSnpQuery = new HashSet<>();
            knockoutTypeQuery = new HashSet<>();
            popFreqQuery = new LinkedList<>();
            clinicalSignificanceQuery = new HashSet<>();
            typeQuery = new HashSet<>();
            consequenceTypeQuery = new HashSet<>();
            ids = new HashSet<>();
            transcriptCompHetIdsMap = new HashMap<>();
            homIds = new HashSet<>();
            hetIds = new HashSet<>();
            delOverlapIds = new HashSet<>();

            query = ParamUtils.defaultObject(query, Query::new);
            variantIdQuery.addAll(query.getAsStringList(RgaQueryParams.VARIANTS.key()));
            dbSnpQuery.addAll(query.getAsStringList(RgaQueryParams.DB_SNPS.key()));
            knockoutTypeQuery.addAll(query.getAsStringList(RgaQueryParams.KNOCKOUT.key()));
            typeQuery.addAll(query.getAsStringList(RgaQueryParams.TYPE.key()));
            clinicalSignificanceQuery.addAll(query.getAsStringList(RgaQueryParams.CLINICAL_SIGNIFICANCE.key()));
            consequenceTypeQuery.addAll(query.getAsStringList(RgaQueryParams.CONSEQUENCE_TYPE.key())
                    .stream()
                    .map(VariantQueryUtils::parseConsequenceType)
                    .map(String::valueOf)
                    .collect(Collectors.toList()));
            List<String> popFreqs = query.getAsStringList(RgaQueryParams.POPULATION_FREQUENCY.key(), ";");
            if (!popFreqs.isEmpty()) {
                Map<String, List<String>> popFreqList = RgaUtils.parsePopulationFrequencyQuery(popFreqs);
                for (List<String> values : popFreqList.values()) {
                    popFreqQuery.add(new HashSet<>(values));
                }
            }
        }

        public void processFeature(RgaUtils.CodedFeature codedFeature) {
            if (codedFeature instanceof RgaUtils.CodedVariant) {
                // Special checks for CodedVariants
                RgaUtils.CodedVariant codedVariant = (CodedVariant) codedFeature;
                if (!variantIdQuery.isEmpty() && !variantIdQuery.contains(codedVariant.getId())) {
                    return;
                }
                if (!dbSnpQuery.isEmpty() && !dbSnpQuery.contains(codedVariant.getDbSnp())) {
                    return;
                }
                if (!clinicalSignificanceQuery.isEmpty()
                        && codedVariant.getClinicalSignificances().stream().noneMatch((cs) -> clinicalSignificanceQuery.contains(cs))) {
                    return;
                }
            }

            // Common filters
            if (!knockoutTypeQuery.isEmpty() && !knockoutTypeQuery.contains(codedFeature.getKnockoutType())) {
                return;
            }
            if (!popFreqQuery.isEmpty()) {
                for (Set<String> popFreq : popFreqQuery) {
                    if (codedFeature.getPopulationFrequencies().stream().noneMatch(popFreq::contains)) {
                        return;
                    }
                }
            }
            if (!typeQuery.isEmpty() && !typeQuery.contains(codedFeature.getType())) {
                return;
            }
            if (!consequenceTypeQuery.isEmpty()
                    && codedFeature.getConsequenceType().stream().noneMatch((ct) -> consequenceTypeQuery.contains(ct))) {
                return;
            }

            ids.add(codedFeature.getId());
            KnockoutVariant.KnockoutType knockoutType = KnockoutVariant.KnockoutType.valueOf(codedFeature.getKnockoutType());
            switch (knockoutType) {
                case HOM_ALT:
                    homIds.add(codedFeature.getId());
                    break;
                case COMP_HET:
                    if (!transcriptCompHetIdsMap.containsKey(codedFeature.getTranscriptId())) {
                        transcriptCompHetIdsMap.put(codedFeature.getTranscriptId(), new HashSet<>());
                    }
                    transcriptCompHetIdsMap.get(codedFeature.getTranscriptId()).add(codedFeature.getId());
                    break;
                case HET_ALT:
                    hetIds.add(codedFeature.getId());
                    break;
                case DELETION_OVERLAP:
                    delOverlapIds.add(codedFeature.getId());
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + codedFeature.getKnockoutType());
            }
        }

        public Set<String> getIds() {
            return ids;
        }

        public int getNumIds() {
            return ids.size();
        }

        public int getNumCompHetIds() {
            return (int) transcriptCompHetIdsMap.values().stream().flatMap(Set::stream).distinct().count();
        }

        public int getNumPairedCompHetIds() {
            Set<String> chPairs = new HashSet<>();
            for (Set<String> chSet : transcriptCompHetIdsMap.values()) {
                if (chSet.size() > 1) {
                    if (chSet.size() > 50) {
                        // Don't calculate this if the number of possible pairs is bigger than 1000
                        return -1000;
                    }
                    ArrayList<String> chList = new ArrayList<>(chSet);
                    for (int i = 0; i < chList.size() - 1; i++) {
                        for (int j = i + 1; j < chList.size(); j++) {
                            String variant1 = chList.get(i);
                            String variant2 = chList.get(j);
                            if (variant2.compareTo(variant1) < 0) {
                                // Invert positions
                                String aux = variant1;
                                variant1 = variant2;
                                variant2 = aux;
                            }
                            chPairs.add(variant1 + "-" + variant2);
                        }
                    }
                }
            }
            return chPairs.size();
        }

        public int getNumHomIds() {
            return homIds.size();
        }

        public int getNumHetIds() {
            return hetIds.size();
        }

        public int getNumDelOverlapIds() {
            return delOverlapIds.size();
        }

        public Map<String, List<String>> getTranscriptCompHetIdsMap() {
            Map<String, List<String>> compHetMap = new HashMap<>();
            for (Map.Entry<String, Set<String>> entry : transcriptCompHetIdsMap.entrySet()) {
                if (entry.getValue().size() > 1) {
                    compHetMap.put(entry.getKey(), new ArrayList<>(entry.getValue()));
                }
            }
            return compHetMap;
        }
    }

}
