package org.opencb.opencga.catalog.stats.solr.converters;

import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.catalog.stats.solr.SampleSolrModel;
import org.opencb.opencga.core.models.AnnotationSet;
import org.opencb.opencga.core.models.OntologyTerm;
import org.opencb.opencga.core.models.Sample;

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

        sampleSolrModel.setId(sample.getId());
        sampleSolrModel.setName(sample.getName());
        sampleSolrModel.setSource(sample.getSource());
        sampleSolrModel.setIndividual(sample.getIndividual().getUuid());  // ??? want some trick to store name ?
        sampleSolrModel.setRelease(sample.getRelease());
        sampleSolrModel.setCreationDate(sample.getCreationDate());
        sampleSolrModel.setStatus(sample.getStatus().getName());
        sampleSolrModel.setDescription(sample.getDescription());
        sampleSolrModel.setType(sample.getType());
        sampleSolrModel.setSomatic(sample.isSomatic());
        ///TODO ??? what to store here id, name or

        //sampleSolrModel.setPhenotypes(sample
        for (OntologyTerm phenotype : sample.getPhenotypes()) {
            // add phenotype
        }
        //TODO ?????????????
        // what to store from annotations
        for (AnnotationSet annotationSet : sample.getAnnotationSets()) {
            sampleSolrModel.setAnnotations(annotationSet.getAnnotations());
        }
        return sampleSolrModel;
    }

    public CatalogSampleToSolrSampleConverter() {
    }
}
