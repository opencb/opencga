package org.opencb.opencga.analysis.rga;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ClinicalSignificance;
import org.opencb.biodata.models.variant.avro.PopulationFrequency;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.opencga.analysis.rga.exceptions.RgaException;
import org.opencb.opencga.analysis.rga.iterators.RgaIterator;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutTranscript;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutVariant;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.slf4j.LoggerFactory;

import java.util.*;

public class IndividualRgaConverter extends AbstractRgaConverter {

    // This object contains the list of solr fields that are required in order to fully build each of the KnockoutByIndividual fields
    private static final Map<String, List<String>> CONVERTER_MAP;

    static {
        CONVERTER_MAP = new HashMap<>();
        CONVERTER_MAP.put("id", Collections.singletonList(RgaDataModel.INDIVIDUAL_ID));
        CONVERTER_MAP.put("sampleId", Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.SAMPLE_ID));
        CONVERTER_MAP.put("sex", Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.SEX));
        CONVERTER_MAP.put("motherId", Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.MOTHER_ID));
        CONVERTER_MAP.put("fatherId", Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.FATHER_ID));
        CONVERTER_MAP.put("fatherSampleId", Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.FATHER_SAMPLE_ID));
        CONVERTER_MAP.put("motherSampleId", Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.MOTHER_SAMPLE_ID));
        CONVERTER_MAP.put("phenotypes", Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.PHENOTYPES, RgaDataModel.PHENOTYPE_JSON));
        CONVERTER_MAP.put("disorders", Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.DISORDERS, RgaDataModel.DISORDER_JSON));
        CONVERTER_MAP.put("numParents", Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.NUM_PARENTS));
        CONVERTER_MAP.put("stats", Collections.emptyList());
        CONVERTER_MAP.put("genes.id", Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID));
        CONVERTER_MAP.put("genes.name", Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.GENE_NAME));
        CONVERTER_MAP.put("genes.chromosome", Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.CHROMOSOME));
        CONVERTER_MAP.put("genes.biotype", Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.GENE_BIOTYPE));
        CONVERTER_MAP.put("genes.transcripts.id", Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID,
                RgaDataModel.TRANSCRIPT_ID));
        CONVERTER_MAP.put("genes.transcripts.chromosome",
                Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.CHROMOSOME));
        CONVERTER_MAP.put("genes.transcripts.start",
                Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.START));
        CONVERTER_MAP.put("genes.transcripts.end", Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID,
                RgaDataModel.TRANSCRIPT_ID, RgaDataModel.END));
        CONVERTER_MAP.put("genes.transcripts.biotype", Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID,
                RgaDataModel.TRANSCRIPT_ID, RgaDataModel.TRANSCRIPT_BIOTYPE));
        CONVERTER_MAP.put("genes.transcripts.strand",
                Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.STRAND));
        CONVERTER_MAP.put("genes.transcripts.variants.id", Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID,
                RgaDataModel.TRANSCRIPT_ID, RgaDataModel.VARIANTS, RgaDataModel.VARIANT_SUMMARY));
        CONVERTER_MAP.put("genes.transcripts.variants.filter", Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID,
                RgaDataModel.TRANSCRIPT_ID, RgaDataModel.FILTERS, RgaDataModel.VARIANTS, RgaDataModel.VARIANT_SUMMARY));
        CONVERTER_MAP.put("genes.transcripts.variants.type", Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID,
                RgaDataModel.TRANSCRIPT_ID, RgaDataModel.TYPES, RgaDataModel.VARIANTS, RgaDataModel.VARIANT_SUMMARY));
        CONVERTER_MAP.put("genes.transcripts.variants.knockoutType", Arrays.asList(RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID,
                RgaDataModel.TRANSCRIPT_ID, RgaDataModel.KNOCKOUT_TYPES, RgaDataModel.VARIANTS, RgaDataModel.VARIANT_SUMMARY));
        CONVERTER_MAP.put("genes.transcripts.variants.populationFrequencies", Arrays.asList(RgaDataModel.INDIVIDUAL_ID,
                RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.POPULATION_FREQUENCIES, RgaDataModel.VARIANTS,
                RgaDataModel.VARIANT_SUMMARY));
        CONVERTER_MAP.put("genes.transcripts.variants.clinicalSignificance", Arrays.asList(RgaDataModel.INDIVIDUAL_ID,
                RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.CLINICAL_SIGNIFICANCES, RgaDataModel.VARIANTS,
                RgaDataModel.VARIANT_SUMMARY));
        CONVERTER_MAP.put("genes.transcripts.variants.sequenceOntologyTerms", Arrays.asList(RgaDataModel.INDIVIDUAL_ID,
                RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.CONSEQUENCE_TYPES, RgaDataModel.VARIANTS,
                RgaDataModel.VARIANT_SUMMARY));

        logger = LoggerFactory.getLogger(IndividualRgaConverter.class);
    }

    public IndividualRgaConverter() {
    }

    public List<KnockoutByIndividual> convertToDataModelType(RgaIterator rgaIterator) {
        // In this list, we will store the keys of result in the order they have been processed so order is kept
        List<String> knockoutByIndividualOrder = new LinkedList<>();
        Map<String, KnockoutByIndividual> result = new HashMap<>();

        Map<String, Variant> variantMap = Collections.emptyMap();

        while (rgaIterator.hasNext()) {
            RgaDataModel rgaDataModel = rgaIterator.next();

            if (!result.containsKey(rgaDataModel.getIndividualId())) {
                knockoutByIndividualOrder.add(rgaDataModel.getIndividualId());
            }
            extractKnockoutByIndividualMap(rgaDataModel, variantMap, result);
        }

        List<KnockoutByIndividual> knockoutByIndividualList = new ArrayList<>(knockoutByIndividualOrder.size());
        for (String id : knockoutByIndividualOrder) {
            knockoutByIndividualList.add(result.get(id));
        }
        return knockoutByIndividualList;
    }

    public static void extractKnockoutByIndividualMap(RgaDataModel rgaDataModel, Map<String, Variant> variantMap,
                                                      Map<String, KnockoutByIndividual> result) {
        extractKnockoutByIndividualMap(rgaDataModel, variantMap, new HashSet<>(), result);
    }

    /**
     * Extract a map containing the processed KnockoutByindividuals.
     *
     * @param rgaDataModel RgaDataModel instance.
     * @param variantMap Map of variants.
     * @param variantIds Set of variant ids to be included in the result. If empty, include all of them.
     * @param result Map containing the KnockoutByIndividual's processed.
     */
    public static void extractKnockoutByIndividualMap(RgaDataModel rgaDataModel, Map<String, Variant> variantMap, Set<String> variantIds,
                                                      Map<String, KnockoutByIndividual> result) {
        if (!result.containsKey(rgaDataModel.getIndividualId())) {
            KnockoutByIndividual knockoutByIndividual = fillIndividualInfo(rgaDataModel);

            List<KnockoutByIndividual.KnockoutGene> geneList = new LinkedList<>();
            knockoutByIndividual.setGenes(geneList);
            result.put(rgaDataModel.getIndividualId(), knockoutByIndividual);
        }

        KnockoutByIndividual knockoutByIndividual = result.get(rgaDataModel.getIndividualId());
        KnockoutByIndividual.KnockoutGene knockoutGene = null;
        for (KnockoutByIndividual.KnockoutGene gene : knockoutByIndividual.getGenes()) {
            if (StringUtils.isNotEmpty(gene.getId()) && gene.getId().equals(rgaDataModel.getGeneId())) {
                knockoutGene = gene;
            }
        }
        if (knockoutGene == null) {
            knockoutGene = new KnockoutByIndividual.KnockoutGene();
            knockoutGene.setId(rgaDataModel.getGeneId());
            knockoutGene.setName(rgaDataModel.getGeneName());
            knockoutGene.setStrand(rgaDataModel.getStrand());
            knockoutGene.setBiotype(rgaDataModel.getGeneBiotype());
            knockoutGene.setStart(rgaDataModel.getStart());
            knockoutGene.setEnd(rgaDataModel.getEnd());
            knockoutGene.setTranscripts(new LinkedList<>());

            knockoutByIndividual.addGene(knockoutGene);
        }

        if (StringUtils.isNotEmpty(rgaDataModel.getTranscriptId())) {
            List<KnockoutVariant> knockoutVariantList = RgaUtils.extractKnockoutVariants(rgaDataModel, variantMap, variantIds);

            // Add new transcript
            KnockoutTranscript knockoutTranscript = new KnockoutTranscript(rgaDataModel.getTranscriptId(), rgaDataModel.getChromosome(),
                    rgaDataModel.getStart(), rgaDataModel.getEnd(), rgaDataModel.getTranscriptBiotype(), rgaDataModel.getStrand(),
                    knockoutVariantList);
            knockoutGene.addTranscripts(Collections.singletonList(knockoutTranscript));
        }
    }

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
        if (StringUtils.isEmpty(knockoutByIndividual.getId())) {
            throw new RgaException("Missing mandatory field 'id'");
        }
        if (StringUtils.isEmpty(knockoutByIndividual.getSampleId())) {
            throw new RgaException("Missing mandatory field 'sampleId'");
        }

        List<RgaDataModel> result = new LinkedList<>();

        if (knockoutByIndividual.getGenes() != null) {
            for (KnockoutByIndividual.KnockoutGene gene : knockoutByIndividual.getGenes()) {
                for (KnockoutTranscript transcript : gene.getTranscripts()) {
                    List<String> compoundFilters = processFilters(transcript);

                    List<String> phenotypes = populatePhenotypes(knockoutByIndividual.getPhenotypes());
                    List<String> disorders = populateDisorders(knockoutByIndividual.getDisorders());
                    List<String> phenotypeJson;
                    if (knockoutByIndividual.getPhenotypes() != null) {
                        phenotypeJson = new ArrayList<>(knockoutByIndividual.getPhenotypes().size());
                        for (Phenotype phenotype : knockoutByIndividual.getPhenotypes()) {
                            phenotypeJson.add(JacksonUtils.getDefaultObjectMapper().writeValueAsString(phenotype));
                        }
                    } else {
                        phenotypeJson = Collections.emptyList();
                    }
                    List<String> disorderJson;
                    if (knockoutByIndividual.getDisorders() != null) {
                        disorderJson = new ArrayList<>(knockoutByIndividual.getDisorders().size());
                        for (Disorder disorder : knockoutByIndividual.getDisorders()) {
                            disorderJson.add(JacksonUtils.getDefaultObjectMapper().writeValueAsString(disorder));
                        }
                    } else {
                        disorderJson = Collections.emptyList();
                    }

                    String id = knockoutByIndividual.getSampleId() + "_" + gene.getId() + "_" + transcript.getId();
                    String individualId = knockoutByIndividual.getId();

                    int numParents = 0;
                    if (StringUtils.isNotEmpty(knockoutByIndividual.getFatherSampleId())) {
                        numParents++;
                    }
                    if (StringUtils.isNotEmpty(knockoutByIndividual.getMotherSampleId())) {
                        numParents++;
                    }

                    Set<String> individualKnockoutSet = new HashSet<>();
                    List<String> variantIds = new ArrayList<>(transcript.getVariants().size());
                    List<String> knockoutTypes = new ArrayList<>(transcript.getVariants().size());
                    List<String> variantKnockoutList = new ArrayList<>(transcript.getVariants().size());
                    Set<String> types = new HashSet<>();
                    Set<String> consequenceTypes = new HashSet<>();
                    Set<String> clinicalSignificances = new HashSet<>();
                    Set<String> filters = new HashSet<>();

                    Map<String, List<String>> popFreqs = getPopulationFrequencies(transcript);
                    List<KnockoutVariant> variants = transcript.getVariants();
                    for (KnockoutVariant variant : variants) {
                        variantIds.add(variant.getId());
                        String knockoutType = variant.getKnockoutType() != null ? variant.getKnockoutType().name() : "";
                        knockoutTypes.add(knockoutType);
                        if (variant.getType() != null) {
                            types.add(variant.getType().name());
                        }
                        List<String> variantConsequenceTypes = new ArrayList<>();
                        if (variant.getSequenceOntologyTerms() != null) {
                            for (SequenceOntologyTerm sequenceOntologyTerm : variant.getSequenceOntologyTerms()) {
                                if (sequenceOntologyTerm.getAccession() != null) {
                                    variantConsequenceTypes.add(sequenceOntologyTerm.getAccession());
                                }
                            }
                        }
                        consequenceTypes.addAll(variantConsequenceTypes);

                        if (variant.getClinicalSignificance() != null) {
                            for (ClinicalSignificance clinicalSignificance : variant.getClinicalSignificance()) {
                                if (clinicalSignificance != null) {
                                    clinicalSignificances.add(clinicalSignificance.name());
                                }
                            }
                        }

                        if (StringUtils.isNotEmpty(variant.getFilter())) {
                            filters.add(variant.getFilter());
                        }

                        Map<String, String> variantPopFreq = getPopulationFrequencies(variant);
                        RgaUtils.CodedFeature codedFeature = new RgaUtils.CodedFeature(variant.getId(), variant.getType().name(),
                                variant.getKnockoutType().name(), variantConsequenceTypes,
                                variantPopFreq.get(RgaUtils.THOUSAND_GENOMES_STUDY), variantPopFreq.get(RgaUtils.GNOMAD_GENOMES_STUDY));
                        variantKnockoutList.add(codedFeature.getEncodedId());

                        RgaUtils.CodedIndividual codedIndividual = new RgaUtils.CodedIndividual(individualId, variant.getType().name(),
                                variant.getKnockoutType().name(), variantConsequenceTypes,
                                variantPopFreq.get(RgaUtils.THOUSAND_GENOMES_STUDY), variantPopFreq.get(RgaUtils.GNOMAD_GENOMES_STUDY),
                                numParents);
                        individualKnockoutSet.add(codedIndividual.getEncodedId());
                    }

                    String sex = knockoutByIndividual.getSex() != null
                            ? knockoutByIndividual.getSex().name()
                            : IndividualProperty.Sex.UNKNOWN.name();

                    RgaDataModel model = new RgaDataModel()
                            .setId(id)
                            .setIndividualId(individualId)
                            .setSampleId(knockoutByIndividual.getSampleId())
                            .setSex(sex)
                            .setPhenotypes(phenotypes)
                            .setDisorders(disorders)
                            .setFatherId(knockoutByIndividual.getFatherId())
                            .setMotherId(knockoutByIndividual.getMotherId())
                            .setFatherSampleId(knockoutByIndividual.getFatherSampleId())
                            .setMotherSampleId(knockoutByIndividual.getMotherSampleId())
                            .setNumParents(numParents)
                            .setGeneId(gene.getId())
                            .setGeneName(gene.getName())
                            .setGeneBiotype(gene.getBiotype())
                            .setChromosome(gene.getChromosome())
                            .setStrand(gene.getStrand())
                            .setStart(gene.getStart())
                            .setEnd(gene.getEnd())
                            .setTranscriptId(transcript.getId())
                            .setTranscriptBiotype(transcript.getBiotype())
                            .setVariants(variantIds)
                            .setTypes(new ArrayList<>(types))
                            .setKnockoutTypes(knockoutTypes)
                            .setIndividualSummary(new ArrayList<>(individualKnockoutSet))
                            .setVariantSummary(variantKnockoutList)
                            .setFilters(new ArrayList<>(filters))
                            .setConsequenceTypes(new ArrayList<>(consequenceTypes))
                            .setClinicalSignificances(new ArrayList<>(clinicalSignificances))
                            .setPopulationFrequencies(popFreqs)
                            .setCompoundFilters(compoundFilters)
                            .setPhenotypeJson(phenotypeJson)
                            .setDisorderJson(disorderJson);
                    result.add(model);
                }
            }
        }

        return result;
    }

    private float getPopulationFrequency(int variantPosition, String population, Map<String, List<Float>> popFreqMap) throws RgaException {
        if (!population.equals(RgaUtils.THOUSAND_GENOMES_STUDY) && !population.equals(RgaUtils.GNOMAD_GENOMES_STUDY)) {
            throw new RgaException("Unexpected population '" + population + "'");
        }

        String pfKey = RgaDataModel.POPULATION_FREQUENCIES.replace("*", "");
        String mapKey = pfKey + population;

        if (!popFreqMap.containsKey(mapKey)) {
            throw new RgaException("Unexpected map key '" + mapKey + "'");
        }
        List<Float> popFreqList = popFreqMap.get(mapKey);
        if (variantPosition < 0 || variantPosition >= popFreqList.size()) {
            throw new RgaException("Variant position not found");
        }

        return popFreqList.get(variantPosition);
    }

    private Map<String, List<Float>> getPopulationFrequenciesOld(KnockoutTranscript transcript) {
        Map<String, List<Float>> popFreqs = new HashMap<>();

        String pfKey = RgaDataModel.POPULATION_FREQUENCIES.replace("*", "");

        String thousandGenomeKey = pfKey + RgaUtils.THOUSAND_GENOMES_STUDY;
        String gnomadGenomeKey = pfKey + RgaUtils.GNOMAD_GENOMES_STUDY;

        if (!transcript.getVariants().isEmpty()) {
            popFreqs.put(thousandGenomeKey, new ArrayList<>(transcript.getVariants().size()));
            popFreqs.put(gnomadGenomeKey, new ArrayList<>(transcript.getVariants().size()));
        }

        for (KnockoutVariant variant : transcript.getVariants()) {
            boolean gnomad = false;
            boolean thousandG = false;
            if (variant.getPopulationFrequencies() != null) {
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
            }
            if (!thousandG) {
                popFreqs.get(thousandGenomeKey).add(0f);
            }
            if (!gnomad) {
                popFreqs.get(gnomadGenomeKey).add(0f);
            }
        }

        return popFreqs;
    }

    private Map<String, List<String>> getPopulationFrequencies(KnockoutTranscript transcript) throws RgaException {
        String pfKey = RgaDataModel.POPULATION_FREQUENCIES.replace("*", "");

        String thousandGenomeKey = pfKey + RgaUtils.THOUSAND_GENOMES_STUDY;
        String gnomadGenomeKey = pfKey + RgaUtils.GNOMAD_GENOMES_STUDY;

        Set<String> thousandPopFreqs = new HashSet<>();
        Set<String> gnomadPopFreqs = new HashSet<>();

        for (KnockoutVariant variant : transcript.getVariants()) {
            Map<String, String> variantFreqs = getPopulationFrequencies(variant);
            thousandPopFreqs.add(variantFreqs.get(RgaUtils.THOUSAND_GENOMES_STUDY));
            gnomadPopFreqs.add(variantFreqs.get(RgaUtils.GNOMAD_GENOMES_STUDY));
        }

        Map<String, List<String>> popFreqs = new HashMap<>();
        if (!transcript.getVariants().isEmpty()) {
            popFreqs.put(thousandGenomeKey, new ArrayList<>(thousandPopFreqs));
            popFreqs.put(gnomadGenomeKey, new ArrayList<>(gnomadPopFreqs));
        }

        return popFreqs;
    }

    private Map<String, String> getPopulationFrequencies(KnockoutVariant variant) throws RgaException {
        Map<String, String> variantFreqMap = new HashMap<>();

        if (variant.getPopulationFrequencies() != null) {
            for (PopulationFrequency populationFrequency : variant.getPopulationFrequencies()) {
                if (populationFrequency.getPopulation().equals("ALL")) {
                    if (RgaUtils.THOUSAND_GENOMES_STUDY.toUpperCase().equals(populationFrequency.getStudy().toUpperCase())) {
                        String encodedPopFrequency = getEncodedPopFrequency(RgaUtils.THOUSAND_GENOMES_STUDY,
                                populationFrequency.getAltAlleleFreq());
                        variantFreqMap.put(RgaUtils.THOUSAND_GENOMES_STUDY, encodedPopFrequency);
                    } else if (RgaUtils.GNOMAD_GENOMES_STUDY.toUpperCase().equals(populationFrequency.getStudy().toUpperCase())) {
                        String encodedPopFrequency = getEncodedPopFrequency(RgaUtils.GNOMAD_GENOMES_STUDY,
                                populationFrequency.getAltAlleleFreq());
                        variantFreqMap.put(RgaUtils.GNOMAD_GENOMES_STUDY, encodedPopFrequency);
                    }
                }
            }
        }
        if (!variantFreqMap.containsKey(RgaUtils.THOUSAND_GENOMES_STUDY)) {
            String encodedPopFrequency = getEncodedPopFrequency(RgaUtils.THOUSAND_GENOMES_STUDY, 0f);
            variantFreqMap.put(RgaUtils.THOUSAND_GENOMES_STUDY, encodedPopFrequency);
        }
        if (!variantFreqMap.containsKey(RgaUtils.GNOMAD_GENOMES_STUDY)) {
            String encodedPopFrequency = getEncodedPopFrequency(RgaUtils.GNOMAD_GENOMES_STUDY, 0f);
            variantFreqMap.put(RgaUtils.GNOMAD_GENOMES_STUDY, encodedPopFrequency);
        }

        return variantFreqMap;
    }

    private String getEncodedPopFrequency(String population, float value) throws RgaException {
        String populationFrequencyKey = RgaUtils.getPopulationFrequencyKey(value);
        return RgaUtils.encode(population.toUpperCase() + RgaUtils.SEPARATOR + populationFrequencyKey);
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
                if (variant.getFilter().equalsIgnoreCase(RgaUtils.PASS)) {
                    independentTerms.add(Collections.singletonList(RgaUtils.encode(RgaUtils.PASS)));
                } else {
                    independentTerms.add(Collections.singletonList(RgaUtils.encode(RgaUtils.NOT_PASS)));
                }
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
            List<String> pf = new LinkedList<>();
            boolean gnomad = false;
            boolean thousandG = false;
            if (variant.getPopulationFrequencies() != null) {
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
            }
            if (!thousandG) {
                pf.add(RgaUtils.encode(RgaUtils.THOUSAND_GENOMES_STUDY.toUpperCase() + RgaUtils.SEPARATOR + 0f));
            }
            if (!gnomad) {
                pf.add(RgaUtils.encode(RgaUtils.GNOMAD_GENOMES_STUDY.toUpperCase() + RgaUtils.SEPARATOR + 0f));
            }
            independentTerms.add(pf);

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

    @Override
    public List<String> getIncludeFields(List<String> includeFields) {
        Set<String> toInclude = new HashSet<>();
        for (String includeField : includeFields) {
            for (String fieldKey : CONVERTER_MAP.keySet()) {
                if (fieldKey.startsWith(includeField)) {
                    toInclude.addAll(CONVERTER_MAP.get(fieldKey));
                }
            }
        }
        return new ArrayList<>(toInclude);
    }

    @Override
    public List<String> getIncludeFromExcludeFields(List<String> excludeFields) {
        Set<String> excludedFields = new HashSet<>();

        for (String excludeField : excludeFields) {
            for (String fieldKey : CONVERTER_MAP.keySet()) {
                if (fieldKey.startsWith(excludeField)) {
                    excludedFields.add(fieldKey);
                }
            }
        }

        // Add everything that was not excluded
        Set<String> toInclude = new HashSet<>();
        for (String field : CONVERTER_MAP.keySet()) {
            if (!excludedFields.contains(field)) {
                toInclude.addAll(CONVERTER_MAP.get(field));
            }
        }

        return new ArrayList<>(toInclude);
    }
}
