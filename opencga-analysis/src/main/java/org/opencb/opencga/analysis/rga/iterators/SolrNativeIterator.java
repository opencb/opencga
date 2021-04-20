package org.opencb.opencga.analysis.rga.iterators;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CursorMarkParams;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.search.VariantSearchModel;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class SolrNativeIterator<E> implements Iterator<E>, AutoCloseable {

    private SolrClient solrClient;
    private String collection;
    private SolrQuery solrQuery;
    private QueryResponse solrResponse;
    private String cursorMark;
    private String nextCursorMark;

    private Iterator<E> solrIterator;

    private int remaining;
    private static final int BATCH_SIZE = 100;

    private Class<E> clazz;

    public SolrNativeIterator(SolrClient solrClient, String collection, SolrQuery solrQuery, Class<E> clazz) throws SolrServerException {
        this.solrClient = solrClient;
        this.collection = collection;
        this.solrQuery = solrQuery;
        this.clazz = clazz;

        // Make sure that query is sorted
        this.solrQuery.setSort(SolrQuery.SortClause.asc("id"));

        // This is the limit of the user, or the default limit if it is not passed
        this.remaining = (solrQuery.getRows() == null || solrQuery.getRows() < 0)
                ? Integer.MAX_VALUE
                : solrQuery.getRows();

        // We the set cursor at the beginning
        this.cursorMark = CursorMarkParams.CURSOR_MARK_START;

        // We create an empty iterator, this will return false in the first hasNext call
        this.solrIterator = Collections.emptyIterator();

        // Current Solr iterator (aka cursorMarks) implementation does not support skip.
        // A simple solution is to waste these records and remove the Start from the solrQuery
        if (solrQuery.getStart() != null && solrQuery.getStart() >= 0) {
            // Do not change the order or position of the next two lines of code
            Integer skip = solrQuery.getStart();
            // We need to increment remaining with skip to allow the decrement in the hasNext method
            this.remaining = (this.remaining < Integer.MAX_VALUE - skip) ? this.remaining + skip : Integer.MAX_VALUE;
            solrQuery.setStart(null);
            for (int i = 0; i < skip && hasNext(); i++) {
                next();
            }
        }
    }

    @Override
    public boolean hasNext() {
        // This is always false the first time with the empty iterator
        if (solrIterator.hasNext()) {
            return true;
        } else {
            // This only happens when there are no more records in Solr
            if (cursorMark.equals(nextCursorMark) || remaining == 0) {
                return false;
            }

            // We need to fetch another batch from Solr
            try {
                if (nextCursorMark != null) {
                    cursorMark = nextCursorMark;
                }
                solrQuery.setRows(remaining > BATCH_SIZE ? BATCH_SIZE : remaining);
                solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);

                // Execute the query and fetch setRows records, we will iterate over this list
                solrResponse = solrClient.query(collection, solrQuery);

                // When the number of returned elements is less than setRows it means there are no enough elements in the server
                if (solrResponse.getResults().size() < BATCH_SIZE) {
                    remaining = 0;
                } else {
                    // We decrement the number of elements found
                    remaining -= solrResponse.getResults().size();
                }
                nextCursorMark = solrResponse.getNextCursorMark();
                solrIterator = solrResponse.getBeans(clazz).iterator();
                return solrIterator.hasNext();
            } catch (SolrServerException | IOException e) {
                throw new VariantQueryException("Error searching more variants", e);
            }
        }
    }

    @Override
    public E next() {
        // Sanity check
        if (hasNext()) {
            return solrIterator.next();
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void close() throws Exception {
        // nothing to do
    }

    public long getNumFound() {
        // Sanity check
        if (solrResponse == null) {
            hasNext();
        }
        return solrResponse == null ? 0 : solrResponse.getResults().getNumFound();
    }
}
