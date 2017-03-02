package org.opencb.opencga.storage.core.search.solr;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.search.VariantSearchModel;
import org.opencb.opencga.storage.core.search.VariantSearchToVariantConverter;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;

import java.util.Iterator;

/**
 * Created by jtarraga on 01/03/17.
 */
public class SolrVariantIterator extends VariantDBIterator {

    private Iterator<VariantSearchModel> solrIterator;
    private VariantSearchToVariantConverter variantSearchToVariantConverter;

    public SolrVariantIterator(Iterator<VariantSearchModel> solrIterator) {
        this.solrIterator = solrIterator;
        variantSearchToVariantConverter = new VariantSearchToVariantConverter();
    }

    @Override
    public boolean hasNext() {
        return solrIterator.hasNext();
    }

    @Override
    public Variant next() {
        return variantSearchToVariantConverter.convertToDataModelType(solrIterator.next());
    }

    @Override
    public void close() throws Exception {
        // nothing to do
    }
}
