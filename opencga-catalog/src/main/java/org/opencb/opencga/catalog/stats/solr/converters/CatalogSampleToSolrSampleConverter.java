package org.opencb.opencga.catalog.stats.solr.converters;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.catalog.stats.solr.SampleSolrModel;
import org.opencb.opencga.core.models.Sample;

/**
 * Created by wasim on 27/06/18.
 */
public class CatalogSampleToSolrSampleConverter implements ComplexTypeConverter<Sample, SampleSolrModel> {

    @Override
    public Sample convertToDataModelType(SampleSolrModel sampleSolrModel) {
        // DONT NEED
        return null;
    }

    @Override
    public SampleSolrModel convertToStorageType(Sample sample) {

        return null;
    }
}
