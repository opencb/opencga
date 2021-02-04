package org.opencb.opencga.storage.core.rga;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByGene;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutTranscript;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutVariant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class GeneRgaConverter implements ComplexTypeConverter<List<KnockoutByGene>, List<RgaDataModel>> {

    private Logger logger;

    public GeneRgaConverter() {
        this.logger = LoggerFactory.getLogger(GeneRgaConverter.class);
    }

    @Override
    public List<KnockoutByGene> convertToDataModelType(List<RgaDataModel> rgaDataModelList) {
        Map<String, KnockoutByGene> result = new HashMap<>();
        for (RgaDataModel rgaDataModel : rgaDataModelList) {
            if (!result.containsKey(rgaDataModel.getGeneId())) {
                KnockoutByGene knockoutByGene = new KnockoutByGene();
                knockoutByGene.setId(rgaDataModel.getGeneId());
                knockoutByGene.setName(rgaDataModel.getGeneName());
//                knockoutByGene.setChromosome(xxxxx);
//                knockoutByGene.setStart(xxxx);
//                knockoutByGene.setEnd(xxxx);
//                knockoutByGene.setStrand(xxxx);
//                knockoutByGene.setBiotype(xxxx);
//                knockoutByGene.setAnnotation(xxxx);

                knockoutByGene.setIndividuals(new LinkedList<>());
                result.put(rgaDataModel.getGeneId(), knockoutByGene);
            }

            KnockoutByGene knockoutByGene = result.get(rgaDataModel.getGeneId());
            KnockoutByGene.KnockoutIndividual knockoutIndividual = null;
            if (StringUtils.isNotEmpty(rgaDataModel.getIndividualId())) {
                for (KnockoutByGene.KnockoutIndividual individual : knockoutByGene.getIndividuals()) {
                    if (rgaDataModel.getIndividualId().equals(individual.getId())) {
                        knockoutIndividual = individual;
                    }
                }
            }
            if (knockoutIndividual == null) {
                knockoutIndividual = new KnockoutByGene.KnockoutIndividual();
                knockoutIndividual.setId(rgaDataModel.getIndividualId());
                knockoutIndividual.setSampleId(rgaDataModel.getSampleId());
                knockoutIndividual.setTranscripts(new LinkedList<>());

                knockoutByGene.addIndividual(knockoutIndividual);
            }

            // Add new transcript
            KnockoutTranscript knockoutTranscript = new KnockoutTranscript(rgaDataModel.getTranscriptId());
            knockoutTranscript.setBiotype(rgaDataModel.getTranscriptBiotype());

            knockoutIndividual.addTranscripts(Collections.singletonList(knockoutTranscript));

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

        return new ArrayList<>(result.values());
    }

    @Override
    public List<RgaDataModel> convertToStorageType(List<KnockoutByGene> object) {
        return null;
    }

}
