package org.opencb.opencga.catalog.stats.solr.converters;

import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.catalog.stats.solr.IndividualSolrModel;
import org.opencb.opencga.core.models.Individual;

/**
 * Created by wasim on 04/07/18.
 */
public class CatalogIndividualToSolrIndividualConverter implements ComplexTypeConverter<Individual, IndividualSolrModel> {
    @Override
    public Individual convertToDataModelType(IndividualSolrModel object) {
        try {
            throw new Exception("Not supported operation!!");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public IndividualSolrModel convertToStorageType(Individual individual) {
        IndividualSolrModel individualSolrModel = new IndividualSolrModel();

        individualSolrModel.setUid(individual.getUid());
        if (individual.getMultiples() != null) {
            individualSolrModel.setMultipleTypeName(individual.getMultiples().getType());
        }

        if (individual.getSex() != null) {
            individualSolrModel.setSex(individual.getSex().name());
        }

        if (individual.getKaryotypicSex() != null) {
            individualSolrModel.setKaryotypicSex(individual.getKaryotypicSex().name());
        }

        individualSolrModel.setEthnicity(individual.getEthnicity());

        if (individual.getPopulation() != null) {
            individualSolrModel.setPopulation(individual.getPopulation().getName());
        }
        individualSolrModel.setRelease(individual.getRelease());
        individualSolrModel.setVersion(individual.getVersion());
        individualSolrModel.setCreationDate(individual.getCreationDate());
        if (individual.getStatus() != null) {
            individualSolrModel.setStatus(individual.getStatus().getName());
        }
        if (individual.getLifeStatus() != null) {
            individualSolrModel.setLifeStatus(individual.getLifeStatus().name());
        }
        if (individual.getAffectationStatus() != null) {
            individualSolrModel.setAffectationStatus(individual.getAffectationStatus().name());
        }
        individualSolrModel.setPhenotypes(SolrConverterUtil.populatePhenotypes(individual.getPhenotypes()));
        if (individual.getSamples() != null) {
            individualSolrModel.setSamples(individual.getSamples().size());
        }

        individualSolrModel.setParentalConsanguinity(individual.isParentalConsanguinity());
        individualSolrModel.setAnnotations(SolrConverterUtil.populateAnnotations(individual.getAnnotationSets()));

        return individualSolrModel;
    }
}
