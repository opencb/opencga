package org.opencb.opencga.storage.core.rga;

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
            result.add(new KnockoutByVariant(variantId, variantMap.get(variantId)));
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
}
