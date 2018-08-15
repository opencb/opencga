package org.opencb.opencga.catalog.stats.solr.converters;

import org.apache.commons.lang3.NotImplementedException;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.catalog.stats.solr.IndividualSolrModel;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.core.models.Study;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by wasim on 04/07/18.
 */
public class CatalogIndividualToSolrIndividualConverter implements ComplexTypeConverter<Individual, IndividualSolrModel> {

    private Study study;

    public CatalogIndividualToSolrIndividualConverter(Study study) {
        this.study = study;
    }

    @Override
    public Individual convertToDataModelType(IndividualSolrModel object) {
        throw new NotImplementedException("Operation not supported");
    }

    @Override
    public IndividualSolrModel convertToStorageType(Individual individual) {
        IndividualSolrModel individualSolrModel = new IndividualSolrModel();

        individualSolrModel.setUid(individual.getUid());
        individualSolrModel.setStudyId(study.getFqn().replace(":", "__"));
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

        // Extract the permissions
        Map<String, Set<String>> individualAcl =
                SolrConverterUtil.parseInternalOpenCGAAcls((List<Map<String, Object>>) individual.getAttributes().get("OPENCGA_ACL"));
        List<String> effectivePermissions =
                SolrConverterUtil.getEffectivePermissions((Map<String, Set<String>>) study.getAttributes().get("OPENCGA_ACL"),
                        individualAcl, "INDIVIDUAL");
        individualSolrModel.setAcl(effectivePermissions);

        return individualSolrModel;
    }
}
