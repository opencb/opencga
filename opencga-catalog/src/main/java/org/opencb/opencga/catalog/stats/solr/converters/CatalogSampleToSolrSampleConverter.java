package org.opencb.opencga.catalog.stats.solr.converters;

import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.catalog.stats.solr.SampleSolrModel;
import org.opencb.opencga.core.models.AnnotationSet;
import org.opencb.opencga.core.models.OntologyTerm;
import org.opencb.opencga.core.models.Sample;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wasim on 27/06/18.
 */
public class CatalogSampleToSolrSampleConverter implements ComplexTypeConverter<Sample, SampleSolrModel> {

    // DONT NEED
    @Override
    public Sample convertToDataModelType(SampleSolrModel sampleSolrModel) {
        try {
            throw new Exception("Not supported operation!!");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public SampleSolrModel convertToStorageType(Sample sample) {

        SampleSolrModel sampleSolrModel = new SampleSolrModel();

        sampleSolrModel.setUid(sample.getUid());
        sampleSolrModel.setSource(sample.getSource());

        sampleSolrModel.setIndividualUuid(sample.getIndividual().getUuid());
        sampleSolrModel.setIndividualEthnicity(sample.getIndividual().getEthnicity());
        sampleSolrModel.setIndividualKaryotypicSex(sample.getIndividual().getKaryotypicSex().name());
        sampleSolrModel.setIndividualPopulation(sample.getIndividual().getPopulation().getName());

        sampleSolrModel.setRelease(sample.getRelease());
        sampleSolrModel.setVersion(sample.getVersion());
        sampleSolrModel.setCreationDate(sample.getCreationDate());
        sampleSolrModel.setStatus(sample.getStatus().getName());
        sampleSolrModel.setType(sample.getType());
        sampleSolrModel.setSomatic(sample.isSomatic());

        sampleSolrModel.setPhenotypes(populatePhenotypes(sample.getPhenotypes()));

        for (AnnotationSet annotationSet : sample.getAnnotationSets()) {
            sampleSolrModel.setAnnotations(annotationSet.getAnnotations());
        }
        return sampleSolrModel;
    }

    public CatalogSampleToSolrSampleConverter() {
    }

    private List<String> populatePhenotypes(List<OntologyTerm> phenotypes) {
        List<String> phenotypesIds = new ArrayList<>();
        for (OntologyTerm ontologyTerm : phenotypes) {
            phenotypesIds.add(ontologyTerm.getId());
        }
        return phenotypesIds;
    }
}
