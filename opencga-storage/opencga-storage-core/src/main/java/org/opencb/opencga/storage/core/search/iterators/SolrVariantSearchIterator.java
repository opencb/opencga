package org.opencb.opencga.storage.core.search.iterators;

import org.opencb.opencga.storage.core.search.VariantSearch;

import java.util.Iterator;

/**
 * Created by wasim on 14/11/16.
 */
public class SolrVariantSearchIterator extends VariantSearchIterator {

    private Iterator<VariantSearch> solrIterator;

    public SolrVariantSearchIterator(Iterator<VariantSearch> solrIterator) {
        this.solrIterator = solrIterator;
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public boolean hasNext() {
        return solrIterator.hasNext();
    }

    @Override
    public VariantSearch next() {
        return solrIterator.next();
    }
}
