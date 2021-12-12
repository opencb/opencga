package org.opencb.opencga.storage.core.variant.search;

import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.common.SolrInputDocument;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.commons.Converter;

public class VariantToSolrBeanConverterTask implements Converter<Variant, SolrInputDocument> {

    private final VariantSearchToVariantConverter converter;
    private final DocumentObjectBinder binder;

    public VariantToSolrBeanConverterTask(DocumentObjectBinder binder) {
        this.binder = binder;
        this.converter = new VariantSearchToVariantConverter();
    }

    @Override
    public SolrInputDocument convert(Variant variant) {
        VariantSearchModel variantSearchModel = converter.convertToStorageType(variant);
        return binder.toSolrInputDocument(variantSearchModel);
    }

}
