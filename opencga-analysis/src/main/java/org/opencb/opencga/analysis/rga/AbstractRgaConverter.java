package org.opencb.opencga.analysis.rga;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;
import org.slf4j.Logger;

import java.util.*;

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

    protected class ProcessedIndividuals {
        // User parameters
        private int limit;
        private int skip;
        private Set<String> includeIndividuals;

        private Set<String> excludedIndividuals;
        private Set<String> includedIndividuals;

        public ProcessedIndividuals(int limit, int skip, List<String> includeIndividuals) {
            this.limit = limit;
            this.skip = skip;
            this.includeIndividuals = new HashSet<>(includeIndividuals);

            this.excludedIndividuals = new HashSet<>();
            this.includedIndividuals = new HashSet<>();
        }

        private void addIndividual(String individual) {
            if (!excludedIndividuals.contains(individual) && !includedIndividuals.contains(individual)) {
                if (limit > includedIndividuals.size()) {
                    if ((!includeIndividuals.isEmpty() && includeIndividuals.contains(individual)) || skip <= excludedIndividuals.size()) {
                        includedIndividuals.add(individual);
                    } else {
                        excludedIndividuals.add(individual);
                    }
                } else {
                    excludedIndividuals.add(individual);
                }
            }
        }

        public boolean processIndividual(String individual) {
            addIndividual(individual);
            return includedIndividuals.contains(individual);
        }

        public int getNumIndividuals() {
            return includedIndividuals.size() + excludedIndividuals.size();
        }
    }

    public abstract List<String> getIncludeFields(List<String> includeFields);

    public abstract List<String> getIncludeFromExcludeFields(List<String> excludeFields);

}
