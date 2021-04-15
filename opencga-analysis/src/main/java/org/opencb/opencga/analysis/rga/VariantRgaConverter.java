package org.opencb.opencga.analysis.rga;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.analysis.rga.iterators.RgaIterator;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByVariant;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutTranscript;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutVariant;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.slf4j.LoggerFactory;

import java.util.*;

public class VariantRgaConverter extends AbstractRgaConverter {

    // This object contains the list of solr fields that are required in order to fully build each of the KnockoutByIndividual fields
    private static final Map<String, List<String>> CONVERTER_MAP;

    static {
        // We always include individual id in the response because we always want to return the numIndividuals populated
        CONVERTER_MAP = new HashMap<>();
        CONVERTER_MAP.put("id", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS, RgaDataModel.INDIVIDUAL_ID));
        CONVERTER_MAP.put("individuals.id", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS, RgaDataModel.INDIVIDUAL_ID));
        CONVERTER_MAP.put("individuals.sampleId", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS, RgaDataModel.INDIVIDUAL_ID));
        CONVERTER_MAP.put("individuals.sex", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS, RgaDataModel.INDIVIDUAL_ID,
                RgaDataModel.SEX));
        CONVERTER_MAP.put("individuals.numParents", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.NUM_PARENTS));
        CONVERTER_MAP.put("individuals.motherId", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.MOTHER_ID));
        CONVERTER_MAP.put("individuals.fatherId", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.FATHER_ID));
        CONVERTER_MAP.put("individuals.motherSampleId", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.MOTHER_SAMPLE_ID));
        CONVERTER_MAP.put("individuals.fatherSampleId", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.FATHER_SAMPLE_ID));
        CONVERTER_MAP.put("individuals.phenotypes", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.PHENOTYPES, RgaDataModel.PHENOTYPE_JSON));
        CONVERTER_MAP.put("individuals.disorders", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.DISORDERS, RgaDataModel.DISORDER_JSON));
        CONVERTER_MAP.put("individuals.stats", Collections.emptyList());
        CONVERTER_MAP.put("individuals.genes.id", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID));
        CONVERTER_MAP.put("individuals.genes.name", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.GENE_NAME));
        CONVERTER_MAP.put("individuals.genes.chromosome", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.CHROMOSOME));
        CONVERTER_MAP.put("individuals.genes.biotype", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.GENE_BIOTYPE));
        CONVERTER_MAP.put("individuals.genes.transcripts.id", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID));
        CONVERTER_MAP.put("individuals.genes.transcripts.chromosome", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.CHROMOSOME));
        CONVERTER_MAP.put("individuals.genes.transcripts.start", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.START));
        CONVERTER_MAP.put("individuals.genes.transcripts.end", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.END));
        CONVERTER_MAP.put("individuals.genes.transcripts.biotype", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.TRANSCRIPT_BIOTYPE));
        CONVERTER_MAP.put("individuals.genes.transcripts.strand", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.STRAND));
        CONVERTER_MAP.put("individuals.genes.transcripts.variants.id", Arrays.asList(RgaDataModel.SAMPLE_ID, RgaDataModel.VARIANTS,
                RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.VARIANTS));
        CONVERTER_MAP.put("individuals.genes.transcripts.variants.filter", Arrays.asList(RgaDataModel.SAMPLE_ID,
                RgaDataModel.VARIANTS, RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.FILTERS));
        CONVERTER_MAP.put("individuals.genes.transcripts.variants.type", Arrays.asList(RgaDataModel.SAMPLE_ID,
                RgaDataModel.VARIANTS, RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID, RgaDataModel.TYPES));
        CONVERTER_MAP.put("individuals.genes.transcripts.variants.knockoutType", Arrays.asList(RgaDataModel.SAMPLE_ID,
                RgaDataModel.VARIANTS, RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID,
                RgaDataModel.KNOCKOUT_TYPES));
        CONVERTER_MAP.put("individuals.genes.transcripts.variants.populationFrequencies", Arrays.asList(RgaDataModel.SAMPLE_ID,
                RgaDataModel.VARIANTS, RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID,
                RgaDataModel.POPULATION_FREQUENCIES));
        CONVERTER_MAP.put("individuals.genes.transcripts.variants.sequenceOntologyTerms", Arrays.asList(RgaDataModel.SAMPLE_ID,
                RgaDataModel.VARIANTS, RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID,
                RgaDataModel.CONSEQUENCE_TYPES));
        CONVERTER_MAP.put("individuals.genes.transcripts.variants.clinicalSignificance", Arrays.asList(RgaDataModel.SAMPLE_ID,
                RgaDataModel.VARIANTS, RgaDataModel.INDIVIDUAL_ID, RgaDataModel.GENE_ID, RgaDataModel.TRANSCRIPT_ID,
                RgaDataModel.CLINICAL_SIGNIFICANCES));

        logger = LoggerFactory.getLogger(VariantRgaConverter.class);
    }

    public VariantRgaConverter() {
    }


    public List<KnockoutByVariant> convertToDataModelType(RgaIterator rgaIterator, VariantDBIterator variantDBIterator,
                                                          List<String> includeVariants) {
        return convertToDataModelType(rgaIterator, variantDBIterator, includeVariants, 0, RgaQueryParams.DEFAULT_INDIVIDUAL_LIMIT);
    }

    public List<KnockoutByVariant> convertToDataModelType(RgaIterator rgaIterator, VariantDBIterator variantDBIterator,
                                                          List<String> includeVariants, int skipIndividuals, int limitIndividuals) {
        Set<String> includeVariantIds = new HashSet<>(includeVariants);

        Map<String, Variant> variantMap = new HashMap<>();
        while (variantDBIterator.hasNext()) {
            Variant variant = variantDBIterator.next();
            variantMap.put(variant.getId(), variant);
        }

        Map<String, ProcessedIndividuals> variantIndividualMap = new HashMap<>();

        // In this list, we will store the keys of result in the order they have been processed so order is kept
        List<String> variantOrder = new LinkedList<>();
        Map<String, Set<String>> result = new HashMap<>();

        Map<String, KnockoutByIndividual> individualMap = new HashMap<>();
        while (rgaIterator.hasNext()) {
            RgaDataModel rgaDataModel = rgaIterator.next();

            List<String> auxVariantIds = new LinkedList<>();
            for (String variant : rgaDataModel.getVariants()) {
                if (includeVariantIds.isEmpty() || includeVariantIds.contains(variant)) {
                    auxVariantIds.add(variant);
                    if (!result.containsKey(variant)) {
                        result.put(variant, new HashSet<>());

                        // The variant will be processed, so we add it to the order list
                        variantOrder.add(variant);

                        // Add also to the map to control the nested individuals that should actually be processed
                        variantIndividualMap.put(variant, new ProcessedIndividuals(limitIndividuals, skipIndividuals));
                    }
                }
            }
            if (!auxVariantIds.isEmpty()) {
                boolean processNestedIndividual = false;
                for (String variant : auxVariantIds) {
                    ProcessedIndividuals processedIndividuals = variantIndividualMap.get(variant);
                    if (processedIndividuals.processIndividual(rgaDataModel.getIndividualId())) {
                        processNestedIndividual = true;
                        break;
                    }
                }
                if (processNestedIndividual) {
                    IndividualRgaConverter.extractKnockoutByIndividualMap(rgaDataModel, variantMap, individualMap);

                    for (String auxVariantId : auxVariantIds) {
                        ProcessedIndividuals processedIndividuals = variantIndividualMap.get(auxVariantId);
                        if (processedIndividuals.processIndividual(rgaDataModel.getIndividualId())) {
                            result.get(auxVariantId).add(rgaDataModel.getIndividualId());
                        }
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

            List<KnockoutByIndividual> filteredIndividualList = new ArrayList<>(individualList.size());
            KnockoutVariant knockoutVariant = null;
            for (KnockoutByIndividual knockoutByIndividual : individualList) {
                List<KnockoutByIndividual.KnockoutGene> geneList = new LinkedList<>();
                // Look for nested variant info
                if (CollectionUtils.isNotEmpty(knockoutByIndividual.getGenes())) {
                    for (KnockoutByIndividual.KnockoutGene gene : knockoutByIndividual.getGenes()) {
                        List<KnockoutTranscript> knockoutTranscriptList = new LinkedList<>();

                        if (CollectionUtils.isNotEmpty(gene.getTranscripts())) {
                            for (KnockoutTranscript transcript : gene.getTranscripts()) {
                                List<KnockoutVariant> filteredVariantList = new LinkedList<>();

                                if (CollectionUtils.isNotEmpty(transcript.getVariants())) {
                                    for (KnockoutVariant transcriptVariant : transcript.getVariants()) {
                                        if (variantId.equals(transcriptVariant.getId())) {
                                            // Only add the expected variant to the list
                                            filteredVariantList.add(transcriptVariant);
                                            knockoutVariant = transcriptVariant;
                                        }
                                    }
                                }
                                if (!filteredVariantList.isEmpty()) {
                                    knockoutTranscriptList.add(new KnockoutTranscript(transcript.getId(), transcript.getChromosome(),
                                            transcript.getStart(), transcript.getEnd(), transcript.getBiotype(), transcript.getStrand(),
                                            filteredVariantList));
                                }
                            }
                        }

                        if (!knockoutTranscriptList.isEmpty()) {
                            KnockoutByIndividual.KnockoutGene knockoutGene = new KnockoutByIndividual.KnockoutGene(gene.getId(), gene.getName(),
                                    gene.getChromosome(), gene.getStart(), gene.getEnd(), gene.getBiotype(), gene.getStrand());
                            knockoutGene.setTranscripts(knockoutTranscriptList);
                            geneList.add(knockoutGene);
                        }
                    }
                }
                if (!geneList.isEmpty()) {
                    KnockoutByIndividual filteredIndividual = new KnockoutByIndividual(knockoutByIndividual.getId(),
                            knockoutByIndividual.getSampleId(), knockoutByIndividual.getMotherId(), knockoutByIndividual.getMotherSampleId(),
                            knockoutByIndividual.getFatherId(), knockoutByIndividual.getFatherSampleId(), knockoutByIndividual.getSex(),
                            knockoutByIndividual.getPhenotypes(), knockoutByIndividual.getDisorders(), knockoutByIndividual.getStats());
                    filteredIndividual.setGenes(geneList);

                    filteredIndividualList.add(filteredIndividual);
                }
            }

            KnockoutByVariant knockoutByVariant = new KnockoutByVariant(variantId, filteredIndividualList);
            if (knockoutVariant == null) {
                // It hasn't been processed because the skipIndividuals is higher than the actual number of individuals having that variant
                Variant variant = variantMap.get(variantId);
                knockoutVariant = RgaUtils.convertToKnockoutVariant(variant);
            }
            knockoutByVariant.setVariantFields(knockoutVariant);

            ProcessedIndividuals processedIndividuals = variantIndividualMap.get(variantId);
            knockoutByVariant.setNumIndividuals(processedIndividuals.getNumIndividuals());
            knockoutByVariant.setHasNextIndividual(processedIndividuals.getNumIndividuals() > skipIndividuals + limitIndividuals);

            knockoutVariantList.add(knockoutByVariant);
        }
        return knockoutVariantList;
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
