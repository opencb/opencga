package org.opencb.opencga.storage.core.rga;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.biodata.models.variant.avro.PopulationFrequency;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutTranscript;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutVariant;
import org.opencb.opencga.storage.core.exceptions.RgaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class IndividualRgaConverter implements ComplexTypeConverter<List<KnockoutByIndividual>, List<RgaDataModel>> {

    private Logger logger;

    public IndividualRgaConverter() {
        this.logger = LoggerFactory.getLogger(IndividualRgaConverter.class);
    }

    @Override
    public List<KnockoutByIndividual> convertToDataModelType(List<RgaDataModel> rgaDataModelList) {
        // In this list, we will store the keys of result in the order they have been processed so order is kept
        List<String> knockoutByIndividualOrder = new LinkedList<>();
        Map<String, KnockoutByIndividual> result = new HashMap<>();
        for (RgaDataModel rgaDataModel : rgaDataModelList) {
            if (!result.containsKey(rgaDataModel.getIndividualId())) {
                knockoutByIndividualOrder.add(rgaDataModel.getIndividualId());

                KnockoutByIndividual knockoutByIndividual = new KnockoutByIndividual();
                knockoutByIndividual.setId(rgaDataModel.getIndividualId());
                knockoutByIndividual.setSampleId(rgaDataModel.getSampleId());
                knockoutByIndividual.setSex(IndividualProperty.Sex.valueOf(rgaDataModel.getSex()));
                List<Phenotype> phenotypes = new ArrayList<>(rgaDataModel.getPhenotypeJson().size());
                for (String phenotype : rgaDataModel.getPhenotypeJson()) {
                    try {
                        phenotypes.add(JacksonUtils.getDefaultObjectMapper().readValue(phenotype, Phenotype.class));
                    } catch (JsonProcessingException e) {
                        logger.warn("Could not parse Phenotypes: {}", e.getMessage(), e);
                    }
                }
                knockoutByIndividual.setPhenotypes(phenotypes);

                List<Disorder> disorders = new ArrayList<>(rgaDataModel.getDisorderJson().size());
                for (String disorder : rgaDataModel.getDisorderJson()) {
                    try {
                        disorders.add(JacksonUtils.getDefaultObjectMapper().readValue(disorder, Disorder.class));
                    } catch (JsonProcessingException e) {
                        logger.warn("Could not parse Disorders: {}", e.getMessage(), e);
                    }
                }
                knockoutByIndividual.setDisorders(disorders);

                List<KnockoutByIndividual.KnockoutGene> geneList = new LinkedList<>();
                KnockoutByIndividual.KnockoutGene knockoutGene = new KnockoutByIndividual.KnockoutGene();
                knockoutGene.setId(rgaDataModel.getGeneId());
                knockoutGene.setName(rgaDataModel.getGeneName());
                knockoutGene.setTranscripts(new LinkedList<>());
                geneList.add(knockoutGene);

                knockoutByIndividual.setGenes(geneList);

                result.put(rgaDataModel.getIndividualId(), knockoutByIndividual);
            }

            KnockoutByIndividual knockoutByIndividual = result.get(rgaDataModel.getIndividualId());
            KnockoutByIndividual.KnockoutGene knockoutGene = null;
            for (KnockoutByIndividual.KnockoutGene gene : knockoutByIndividual.getGenes()) {
                if (gene.getId().equals(rgaDataModel.getGeneId())) {
                    knockoutGene = gene;
                }
            }
            if (knockoutGene == null) {
                knockoutGene = new KnockoutByIndividual.KnockoutGene();
                knockoutGene.setId(rgaDataModel.getGeneId());
                knockoutGene.setName(rgaDataModel.getGeneName());
                knockoutGene.setTranscripts(new LinkedList<>());

                knockoutByIndividual.addGene(knockoutGene);
            }

            // Add new transcript
            KnockoutTranscript knockoutTranscript = new KnockoutTranscript(rgaDataModel.getTranscriptId());
            knockoutGene.addTranscripts(Collections.singletonList(knockoutTranscript));

            knockoutTranscript.setBiotype(rgaDataModel.getBiotype());
            List<KnockoutVariant> knockoutVariantList = new LinkedList<>();
            for (String variantJson : rgaDataModel.getVariantJson()) {
                try {
                    KnockoutVariant knockoutVariant = JacksonUtils.getDefaultObjectMapper().readValue(variantJson,
                            KnockoutVariant.class);
                    knockoutVariantList.add(knockoutVariant);
                } catch (JsonProcessingException e) {
                    logger.warn("Could not parse KnockoutVariants: {}", e.getMessage(), e);
                }
            }
            knockoutTranscript.setVariants(knockoutVariantList);
        }

        List<KnockoutByIndividual> knockoutByIndividualList = new ArrayList<>(knockoutByIndividualOrder.size());
        for (String id : knockoutByIndividualOrder) {
            knockoutByIndividualList.add(result.get(id));
        }
        return knockoutByIndividualList;
    }

    @Override
    public List<RgaDataModel> convertToStorageType(List<KnockoutByIndividual> knockoutByIndividualList) {
        List<RgaDataModel> result = new LinkedList<>();
        for (KnockoutByIndividual knockoutByIndividual : knockoutByIndividualList) {
            try {
                result.addAll(convertToStorageType(knockoutByIndividual));
            } catch (RgaException | JsonProcessingException e) {
                logger.warn("Could not parse KnockoutByIndividualList: {}", e.getMessage(), e);
            }
        }
        return result;
    }

    private List<RgaDataModel> convertToStorageType(KnockoutByIndividual knockoutByIndividual)
            throws RgaException, JsonProcessingException {
        List<RgaDataModel> result = new LinkedList<>();

        if (knockoutByIndividual.getGenes() != null) {
            for (KnockoutByIndividual.KnockoutGene gene : knockoutByIndividual.getGenes()) {
                for (KnockoutTranscript transcript : gene.getTranscripts()) {
                    List<String> compoundFilters = processFilters(transcript);
                    List<String> variantJson = new ArrayList<>(transcript.getVariants().size());
                    for (KnockoutVariant variant : transcript.getVariants()) {
                        variantJson.add(JacksonUtils.getDefaultObjectMapper().writeValueAsString(variant));
                    }

                    List<String> phenotypes = populatePhenotypes(knockoutByIndividual.getPhenotypes());
                    List<String> disorders = populateDisorders(knockoutByIndividual.getDisorders());
                    List<String> phenotypeJson = new ArrayList<>(knockoutByIndividual.getPhenotypes().size());
                    for (Phenotype phenotype : knockoutByIndividual.getPhenotypes()) {
                        phenotypeJson.add(JacksonUtils.getDefaultObjectMapper().writeValueAsString(phenotype));
                    }
                    List<String> disorderJson = new ArrayList<>(knockoutByIndividual.getDisorders().size());
                    for (Disorder disorder : knockoutByIndividual.getDisorders()) {
                        disorderJson.add(JacksonUtils.getDefaultObjectMapper().writeValueAsString(disorder));
                    }

                    List<String> variantIds = transcript.getVariants().stream().map(KnockoutVariant::getId)
                            .collect(Collectors.toList());

                    List<String> knockoutTypes = transcript.getVariants().stream()
                            .map(KnockoutVariant::getKnockoutType)
                            .map(Enum::name)
                            .distinct()
                            .collect(Collectors.toList());
                    List<String> consequenceTypes = transcript.getVariants().stream()
                            .flatMap(kv -> kv.getSequenceOntologyTerms().stream())
                            .map(SequenceOntologyTerm::getAccession)
                            .distinct()
                            .collect(Collectors.toList());
                    List<String> filters = transcript.getVariants().stream()
                            .map(KnockoutVariant::getFilter)
                            .distinct().collect(Collectors.toList());
                    Map<String, List<Float>> popFreqs = getPopulationFrequencies(transcript);

                    String id = knockoutByIndividual.getId() + "_" + gene.getId() + "_" + transcript.getId();
                    RgaDataModel model = new RgaDataModel(id, knockoutByIndividual.getSampleId(), knockoutByIndividual.getId(),
                            knockoutByIndividual.getSex().name(), phenotypes, disorders, gene.getId(), gene.getName(), transcript.getId(),
                            transcript.getBiotype(), variantIds, knockoutTypes, filters, consequenceTypes, popFreqs, compoundFilters,
                            phenotypeJson, disorderJson, variantJson);
                    result.add(model);
                }
            }
        }

        return result;
    }

    private Map<String, List<Float>> getPopulationFrequencies(KnockoutTranscript transcript) {
        Map<String, List<Float>> popFreqs = new HashMap<>();

        String thousandGenomeKey = "popFreqs" + RgaUtils.SEPARATOR + RgaUtils.THOUSAND_GENOMES_STUDY;
        String gnomadGenomeKey = "popFreqs" + RgaUtils.SEPARATOR + RgaUtils.GNOMAD_GENOMES_STUDY;

        if (!transcript.getVariants().isEmpty()) {
            popFreqs.put(thousandGenomeKey, new ArrayList<>(transcript.getVariants().size()));
            popFreqs.put(gnomadGenomeKey, new ArrayList<>(transcript.getVariants().size()));
        }

        for (KnockoutVariant variant : transcript.getVariants()) {
            if (variant.getPopulationFrequencies() != null) {
                boolean gnomad = false;
                boolean thousandG = false;
                for (PopulationFrequency populationFrequency : variant.getPopulationFrequencies()) {
                    if (populationFrequency.getPopulation().equals("ALL")) {
                        if (RgaUtils.THOUSAND_GENOMES_STUDY.toUpperCase().equals(populationFrequency.getStudy().toUpperCase())) {
                            popFreqs.get(thousandGenomeKey).add(populationFrequency.getAltAlleleFreq());
                            thousandG = true;
                        } else if (RgaUtils.GNOMAD_GENOMES_STUDY.toUpperCase().equals(populationFrequency.getStudy().toUpperCase())) {
                            popFreqs.get(gnomadGenomeKey).add(populationFrequency.getAltAlleleFreq());
                            gnomad = true;
                        }
                    }
                }
                if (!thousandG) {
                    popFreqs.get(thousandGenomeKey).add(0f);
                }
                if (!gnomad) {
                    popFreqs.get(gnomadGenomeKey).add(0f);
                }
            }
        }

        return popFreqs;
    }

    private List<String> processFilters(KnockoutTranscript transcript) throws RgaException {
        Set<String> results = new HashSet<>();

        List<List<List<String>>> compoundHeterozygousVariantList = new LinkedList<>();
        for (KnockoutVariant variant : transcript.getVariants()) {
            List<List<String>> independentTerms = new LinkedList<>();

            // KO - Knockout types
            if (variant.getKnockoutType() != null) {
                independentTerms.add(Collections.singletonList(RgaUtils.encode(variant.getKnockoutType().name())));
            }
            // F - Filters
            if (StringUtils.isNotEmpty(variant.getFilter())) {
                independentTerms.add(Collections.singletonList(RgaUtils.encode(variant.getFilter())));
            }
            // CT - Consequence types
            if (variant.getSequenceOntologyTerms() != null) {
                List<String> ct = new ArrayList<>(variant.getSequenceOntologyTerms().size());
                for (SequenceOntologyTerm sequenceOntologyTerm : variant.getSequenceOntologyTerms()) {
                    ct.add(RgaUtils.encode(sequenceOntologyTerm.getName()));
                }
                independentTerms.add(ct);
            }
            // PF - Population frequencies
            if (variant.getPopulationFrequencies() != null) {
                List<String> pf = new ArrayList<>(variant.getPopulationFrequencies().size());
                boolean gnomad = false;
                boolean thousandG = false;
                for (PopulationFrequency populationFrequency : variant.getPopulationFrequencies()) {
                    if (populationFrequency.getPopulation().equals("ALL")) {
                        if (RgaUtils.THOUSAND_GENOMES_STUDY.toUpperCase().equals(populationFrequency.getStudy().toUpperCase())) {
                            String populationFrequencyKey = RgaUtils.getPopulationFrequencyKey(populationFrequency.getAltAlleleFreq());
                            pf.add(RgaUtils.encode(RgaUtils.THOUSAND_GENOMES_STUDY.toUpperCase() + RgaUtils.SEPARATOR
                                    + populationFrequencyKey));
                            thousandG = true;
                        } else if (RgaUtils.GNOMAD_GENOMES_STUDY.toUpperCase().equals(populationFrequency.getStudy().toUpperCase())) {
                            String populationFrequencyKey = RgaUtils.getPopulationFrequencyKey(populationFrequency.getAltAlleleFreq());
                            pf.add(RgaUtils.encode(RgaUtils.GNOMAD_GENOMES_STUDY.toUpperCase() + RgaUtils.SEPARATOR
                                    + populationFrequencyKey));
                            gnomad = true;
                        }
                    }
                }
                if (!thousandG) {
                    pf.add(RgaUtils.encode(RgaUtils.THOUSAND_GENOMES_STUDY.toUpperCase() + RgaUtils.SEPARATOR + 0));
                }
                if (!gnomad) {
                    pf.add(RgaUtils.encode(RgaUtils.GNOMAD_GENOMES_STUDY.toUpperCase() + RgaUtils.SEPARATOR + 0));
                }
                independentTerms.add(pf);
            }

            if (variant.getKnockoutType() == KnockoutVariant.KnockoutType.COMP_HET) {
                compoundHeterozygousVariantList.add(independentTerms);
            }

            results.addAll(generateCombinations(independentTerms));
        }

        if (!compoundHeterozygousVariantList.isEmpty()) {
            results.addAll(generateCompoundHeterozygousCombinations(compoundHeterozygousVariantList));
        }

        return new ArrayList<>(results);
    }

    private Set<String> generateCompoundHeterozygousCombinations(List<List<List<String>>> compoundHeterozygousVariantList)
            throws RgaException {
        if (compoundHeterozygousVariantList.size() < 2) {
            return Collections.emptySet();
        }
        Set<String> result = new HashSet<>();
        for (int i = 0; i < compoundHeterozygousVariantList.size() - 1; i++) {
            for (int j = i + 1; j < compoundHeterozygousVariantList.size(); j++) {
                result.addAll(generateCompoundHeterozygousPairCombination(compoundHeterozygousVariantList.get(i),
                        compoundHeterozygousVariantList.get(j)));
            }
        }
        return result;
    }

    private Set<String> generateCompoundHeterozygousPairCombination(List<List<String>> variant1, List<List<String>> variant2)
            throws RgaException {
        List<List<String>> result = new LinkedList<>();
        /* Compound heterozygous combinations:
             * KO - F1 - F2
             * KO - F1 - F2 - CT1 - CT2
             * KO - F1 - F2 - CT1 - CT2 - PF1 - PF2
             * KO - F1 - F2 - PF' ; where PF' is equivalent to the highest PF of both variants (to easily respond to PF<=x)
        */
        String knockout = RgaUtils.encode(KnockoutVariant.KnockoutType.COMP_HET.name());
        result.add(Collections.singletonList(knockout));

        // Generate combinations: KO - F1 - F2; KO - F1 - F2 - CT1 - CT2; KO - F1 - F2 - CT1 - CT2 - PF1 - PF2
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
     * Given two lists containing frequencies for the same populations, it will return a unified frequency for each population containing
     * the least restrictive value. Example: [P1-12, P2-6] - [P1-15, P2-2] will generate [P1-15, P2-6]
     *
     * @param list1 List containing the population frequencies of variant1.
     * @param list2 List containing the population frequencies of variant2.
     * @return A list containing the least restrictive population frequencies.
     * @throws RgaException If there is an issue with the provided lists.
     */
    private List<String> generateSimplifiedPopulationFrequencyList(List<String> list1, List<String> list2) throws RgaException {
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
     * Given a list of [[KO], [FILTER1, FILTER2], [CTs], [PFs]], it will return all possible String combinations merging those values.
     * [KO - FILTER1, KO - FILTER2]
     * [KO - FILTER1 - CT', KO - FILTER2 - CT']
     * [KO - FILTER1 - CT' - PF', KO - FILTER2 - CT' - PF']
     * [KO - FILTER1 - PF', KO - FILTER2 - PF']
     * @param values List of size 4 containing values for the 4 filters.
     * @return a list containing all possible merge combinations to store in the DB.
     */
    private List<String> generateCombinations(List<List<String>> values) {
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

    private List<String> populatePhenotypes(List<Phenotype> phenotypes) {
        Set<String> phenotypesIds = new HashSet<>();
        if (phenotypes != null) {
            for (Phenotype phenotype : phenotypes) {
                phenotypesIds.add(phenotype.getId());
                phenotypesIds.add(phenotype.getName());
            }
        }
        return new ArrayList(phenotypesIds);
    }

    private List<String> populateDisorders(List<Disorder> disorders) {
        Set<String> disorderIds = new HashSet<>();
        if (disorders != null) {
            for (Disorder disorder : disorders) {
                disorderIds.add(disorder.getId());
                disorderIds.add(disorder.getName());
            }
        }
        return new ArrayList(disorderIds);
    }
}
