package org.opencb.opencga.clinical.rga;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class AbstractRgaConverter {

    protected static Logger logger;

    protected static KnockoutByIndividual fillIndividualInfo(RgaDataModel rgaDataModel) {
        KnockoutByIndividual knockoutByIndividual = new KnockoutByIndividual();
        knockoutByIndividual.setId(rgaDataModel.getIndividualId());
        knockoutByIndividual.setSampleId(rgaDataModel.getSampleId());
        knockoutByIndividual.setFatherId(rgaDataModel.getFatherId());
        knockoutByIndividual.setMotherId(rgaDataModel.getMotherId());
        knockoutByIndividual.setMotherSampleId(rgaDataModel.getMotherSampleId());
        knockoutByIndividual.setFatherSampleId(rgaDataModel.getFatherSampleId());
        knockoutByIndividual.setSex(rgaDataModel.getSex() != null ? IndividualProperty.Sex.valueOf(rgaDataModel.getSex()) : null);
        if (rgaDataModel.getPhenotypeJson() != null) {
            List<Phenotype> phenotypes = new ArrayList<>(rgaDataModel.getPhenotypeJson().size());
            for (String phenotype : rgaDataModel.getPhenotypeJson()) {
                try {
                    phenotypes.add(JacksonUtils.getDefaultObjectMapper().readValue(phenotype, Phenotype.class));
                } catch (JsonProcessingException e) {
                    logger.warn("Could not parse Phenotypes: {}", e.getMessage(), e);
                }
            }
            knockoutByIndividual.setPhenotypes(phenotypes);
        } else {
            knockoutByIndividual.setPhenotypes(Collections.emptyList());
        }

        if (rgaDataModel.getDisorderJson() != null) {
            List<Disorder> disorders = new ArrayList<>(rgaDataModel.getDisorderJson().size());
            for (String disorder : rgaDataModel.getDisorderJson()) {
                try {
                    disorders.add(JacksonUtils.getDefaultObjectMapper().readValue(disorder, Disorder.class));
                } catch (JsonProcessingException e) {
                    logger.warn("Could not parse Disorders: {}", e.getMessage(), e);
                }
            }
            knockoutByIndividual.setDisorders(disorders);
        } else {
            knockoutByIndividual.setDisorders(Collections.emptyList());
        }

        return knockoutByIndividual;
    }

}
