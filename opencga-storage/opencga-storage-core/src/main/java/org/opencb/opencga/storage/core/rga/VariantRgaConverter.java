package org.opencb.opencga.storage.core.rga;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByVariant;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutTranscript;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutVariant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class VariantRgaConverter implements ComplexTypeConverter<List<KnockoutByVariant>, List<RgaDataModel>> {

    private Logger logger;
    private IndividualRgaConverter individualRgaConverter;

    // This object contains the list of solr fields that are required in order to fully build each of the KnockoutByIndividual fields
    private static final Map<String, List<String>> CONVERTER_MAP;

    static {
        // We always include individual id in the response because we always want to return the numIndividuals populated
        CONVERTER_MAP = new HashMap<>();
        CONVERTER_MAP.put("id", Arrays.asList(RgaDataModel.VARIANT_JSON, RgaDataModel.VARIANTS, RgaDataModel.INDIVIDUAL_ID));
        CONVERTER_MAP.put("individuals.id", Arrays.asList(RgaDataModel.VARIANT_JSON, RgaDataModel.VARIANTS, RgaDataModel.INDIVIDUAL_ID));
        CONVERTER_MAP.put("individuals.sampleId", Arrays.asList(RgaDataModel.VARIANT_JSON, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.SAMPLE_ID));
        CONVERTER_MAP.put("individuals.sex", Arrays.asList(RgaDataModel.VARIANT_JSON, RgaDataModel.VARIANTS, RgaDataModel.INDIVIDUAL_ID,
                RgaDataModel.SEX));
        CONVERTER_MAP.put("individuals.numParents", Arrays.asList(RgaDataModel.VARIANT_JSON, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.NUM_PARENTS));
        CONVERTER_MAP.put("individuals.motherId", Arrays.asList(RgaDataModel.VARIANT_JSON, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.MOTHER_ID));
        CONVERTER_MAP.put("individuals.fatherId", Arrays.asList(RgaDataModel.VARIANT_JSON, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.FATHER_ID));
        CONVERTER_MAP.put("individuals.phenotypes", Arrays.asList(RgaDataModel.VARIANT_JSON, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.PHENOTYPES, RgaDataModel.PHENOTYPE_JSON));
        CONVERTER_MAP.put("individuals.disorders", Arrays.asList(RgaDataModel.VARIANT_JSON, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.DISORDERS, RgaDataModel.DISORDER_JSON));
        CONVERTER_MAP.put("individuals.stats", Collections.emptyList());
        CONVERTER_MAP.put("individuals.genes.id", Arrays.asList(RgaDataModel.VARIANT_JSON, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID));
        CONVERTER_MAP.put("individuals.genes.name", Arrays.asList(RgaDataModel.VARIANT_JSON, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.GENE_NAME));
        CONVERTER_MAP.put("individuals.genes.chromosome", Arrays.asList(RgaDataModel.VARIANT_JSON, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.CHROMOSOME));
        CONVERTER_MAP.put("individuals.genes.biotype", Arrays.asList(RgaDataModel.VARIANT_JSON, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.GENE_BIOTYPE));
        CONVERTER_MAP.put("individuals.genes.transcripts.id", Arrays.asList(RgaDataModel.VARIANT_JSON, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID));
        CONVERTER_MAP.put("individuals.genes.transcripts.chromosome", Arrays.asList(RgaDataModel.VARIANT_JSON, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.CHROMOSOME));
        CONVERTER_MAP.put("individuals.genes.transcripts.start", Arrays.asList(RgaDataModel.VARIANT_JSON, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.START));
        CONVERTER_MAP.put("individuals.genes.transcripts.end", Arrays.asList(RgaDataModel.VARIANT_JSON, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.END));
        CONVERTER_MAP.put("individuals.genes.transcripts.biotype", Arrays.asList(RgaDataModel.VARIANT_JSON, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.TRANSCRIPT_BIOTYPE));
        CONVERTER_MAP.put("individuals.genes.transcripts.strand", Arrays.asList(RgaDataModel.VARIANT_JSON, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.STRAND));
        CONVERTER_MAP.put("individuals.genes.transcripts.variants.id", Arrays.asList(RgaDataModel.VARIANT_JSON, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.VARIANTS));
        CONVERTER_MAP.put("individuals.genes.transcripts.variants.filter", Arrays.asList(RgaDataModel.VARIANT_JSON,
                RgaDataModel.VARIANTS, RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.FILTERS));
        CONVERTER_MAP.put("individuals.genes.transcripts.variants.type", Arrays.asList(RgaDataModel.VARIANT_JSON,
                RgaDataModel.VARIANTS, RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.TYPES));
        CONVERTER_MAP.put("individuals.genes.transcripts.variants.knockoutType", Arrays.asList(RgaDataModel.VARIANT_JSON,
                RgaDataModel.VARIANTS, RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID,
                RgaDataModel.KNOCKOUT_TYPES));
        CONVERTER_MAP.put("individuals.genes.transcripts.variants.populationFrequencies", Arrays.asList(RgaDataModel.VARIANT_JSON,
                RgaDataModel.VARIANTS, RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID,
                RgaDataModel.POPULATION_FREQUENCIES));
        CONVERTER_MAP.put("individuals.genes.transcripts.variants.sequenceOntologyTerms", Arrays.asList(RgaDataModel.VARIANT_JSON,
                RgaDataModel.VARIANTS, RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID,
                RgaDataModel.CONSEQUENCE_TYPES));
        CONVERTER_MAP.put("individuals.genes.transcripts.variants.clinicalSignificance", Arrays.asList(RgaDataModel.VARIANT_JSON,
                RgaDataModel.VARIANTS, RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID,
                RgaDataModel.CLINICAL_SIGNIFICANCES));
    }

    public VariantRgaConverter() {
        this.logger = LoggerFactory.getLogger(VariantRgaConverter.class);
        this.individualRgaConverter = new IndividualRgaConverter();
    }

    @Override
    public List<KnockoutByVariant> convertToDataModelType(List<RgaDataModel> rgaDataModelList) {
        return convertToDataModelType(rgaDataModelList, Collections.emptyList());
    }

    public List<KnockoutByVariant> convertToDataModelType(List<RgaDataModel> rgaDataModelList, List<String> variantList) {
        Set<String> variantIds = new HashSet<>(variantList);

        // In this list, we will store the keys of result in the order they have been processed so order is kept
        List<String> variantOrder = new LinkedList<>();
        Map<String, Set<String>> result = new HashMap<>();

        Map<String, KnockoutByIndividual> individualMap = new HashMap<>();
        for (RgaDataModel rgaDataModel : rgaDataModelList) {
            List<String> auxVariantIds = new LinkedList<>();
            for (String variant : rgaDataModel.getVariants()) {
                if (variantIds.isEmpty() || variantIds.contains(variant)) {
                    auxVariantIds.add(variant);
                    if (!result.containsKey(variant)) {
                        result.put(variant, new HashSet<>());

                        // The variant will be processed, so we add it to the order list
                        variantOrder.add(variant);
                    }
                }
            }
            if (!auxVariantIds.isEmpty()) {
                IndividualRgaConverter.extractKnockoutByIndividualMap(rgaDataModel, variantIds, individualMap);

                for (String auxVariantId : auxVariantIds) {
                    if (StringUtils.isNotEmpty(rgaDataModel.getIndividualId())) {
                        result.get(auxVariantId).add(rgaDataModel.getIndividualId());
                    }
                }
            }
        }

        List<KnockoutByVariant> knockoutVariantList = new ArrayList<>(variantOrder.size());
        for (String variantId : variantOrder) {
            List<KnockoutByIndividual> individualList = new ArrayList<>(result.get(variantId).size());
            for (String individualId : result.get(variantId)) {
                individualList.add(individualMap.get(individualId));
            }
            Variant variant = new Variant(variantId);
            knockoutVariantList.add(new KnockoutByVariant(variantId, variant.getChromosome(), variant.getStart(), variant.getEnd(),
                    variant.getLength(), variant.getReference(), variant.getAlternate(), individualList));
        }
        return knockoutVariantList;
    }

    public List<KnockoutByVariant> convertToDataModelaType(List<RgaDataModel> rgaDataModelList, List<String> variantList) {
        Set<String> variantIds = new HashSet<>(variantList);

        List<KnockoutByIndividual> knockoutByIndividuals = individualRgaConverter.convertToDataModelType(rgaDataModelList);
        Map<String, List<KnockoutByIndividual>> variantMap = new HashMap<>();

        // Extract variants to the root to generate the KnockoutByVariant data model
        for (KnockoutByIndividual knockoutByIndividual : knockoutByIndividuals) {
            Set<String> tmpVariantIds = new HashSet<>();
            Map<String, Map<String, List<KnockoutVariant>>> transcriptVariantMapList = new HashMap<>();

            for (KnockoutByIndividual.KnockoutGene gene : knockoutByIndividual.getGenes()) {
                for (KnockoutTranscript transcript : gene.getTranscripts()) {
                    Set<String> passedVariants = new HashSet<>();
                    Map<String, KnockoutVariant> tmpVariantMap = new HashMap<>();
                    Set<String> chVariants = new HashSet<>();

                    transcriptVariantMapList.put(transcript.getId(), new HashMap<>());

                    for (KnockoutVariant variant : transcript.getVariants()) {
                        tmpVariantMap.put(variant.getId(), variant);

                        if (variant.getKnockoutType() == KnockoutVariant.KnockoutType.COMP_HET) {
                            chVariants.add(variant.getId());
                        }
                        // Include variant only if the variant is in the query or if user did not filter by any variant
                        if (variantIds.isEmpty() || variantIds.contains(variant.getId())) {
                            passedVariants.add(variant.getId());
                        }
                    }

                    tmpVariantIds.addAll(passedVariants);

                    // Generate variant groups according to the variantList list
                    for (String variantId : passedVariants) {
                        List<KnockoutVariant> tmpVariantList = new LinkedList<>();
                        KnockoutVariant knockoutVariant = tmpVariantMap.get(variantId);
                        tmpVariantList.add(knockoutVariant);

                        if (knockoutVariant.getKnockoutType() == KnockoutVariant.KnockoutType.COMP_HET && chVariants.size() > 1) {
                            // Add all CH variants except itself
                            for (String chVariant : chVariants) {
                                if (!chVariant.equals(variantId)) {
                                    tmpVariantList.add(tmpVariantMap.get(chVariant));
                                }
                            }
                        }

                        transcriptVariantMapList.get(transcript.getId()).put(variantId, tmpVariantList);
                    }
                }
            }

            // For each of the variants that need to be in the root, we add the corresponding filtered KnockoutByIndividual objects
            for (String variantId : tmpVariantIds) {
                if (!variantMap.containsKey(variantId)) {
                    variantMap.put(variantId, new LinkedList<>());
                }

                KnockoutByIndividual tmpKnockoutByIndividual = getFilteredKnockoutByIndividual(knockoutByIndividual,
                        transcriptVariantMapList, variantId);
                variantMap.get(variantId).add(tmpKnockoutByIndividual);
            }
        }

        List<KnockoutByVariant> result = new ArrayList<>(variantMap.size());
        for (String variantId : variantMap.keySet()) {
            Variant variant = new Variant(variantId);
            result.add(new KnockoutByVariant(variantId, variant.getChromosome(), variant.getStart(), variant.getEnd(),
                    variant.getLength(), variant.getReference(), variant.getAlternate(), variantMap.get(variantId)));
        }

        return result;
    }

    private KnockoutByIndividual getFilteredKnockoutByIndividual(KnockoutByIndividual knockoutByIndividual,
                                                                 Map<String, Map<String, List<KnockoutVariant>>> transcriptVariantMapList,
                                                                 String variantId) {
        KnockoutByIndividual newKnockoutByIndividual = new KnockoutByIndividual(knockoutByIndividual.getId(),
                knockoutByIndividual.getSampleId(), knockoutByIndividual.getSex(), knockoutByIndividual.getPhenotypes(),
                knockoutByIndividual.getDisorders(), knockoutByIndividual.getStats());

        List<KnockoutByIndividual.KnockoutGene> geneList = new ArrayList<>(knockoutByIndividual.getGenes().size());
        for (KnockoutByIndividual.KnockoutGene gene : knockoutByIndividual.getGenes()) {
            List<KnockoutTranscript> transcriptList = new ArrayList<>(gene.getTranscripts().size());
            for (KnockoutTranscript transcript : gene.getTranscripts()) {
                Map<String, List<KnockoutVariant>> variantMap = transcriptVariantMapList.get(transcript.getId());
                List<KnockoutVariant> knockoutVariantList = variantMap.get(variantId);
                if (knockoutVariantList != null && !knockoutVariantList.isEmpty()) {
                    transcriptList.add(new KnockoutTranscript(transcript.getId(), transcript.getChromosome(), transcript.getStart(),
                            transcript.getEnd(), transcript.getBiotype(), transcript.getStrand(), knockoutVariantList));
                }
            }

            if (!transcriptList.isEmpty()) {
                geneList.add(new KnockoutByIndividual.KnockoutGene(gene.getId(), gene.getName(), gene.getChromosome(), gene.getStart(),
                        gene.getEnd(), gene.getBiotype(), gene.getStrand()).addTranscripts(transcriptList));
            }
        }

        newKnockoutByIndividual.setGenes(geneList);

        return newKnockoutByIndividual;
    }

    @Override
    public List<RgaDataModel> convertToStorageType(List<KnockoutByVariant> knockoutByVariants) {
        return null;
    }


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
