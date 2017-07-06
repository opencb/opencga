/*
 * Copyright 2015-2016 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.search.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CursorMarkParams;
import org.opencb.opencga.storage.core.search.VariantSearchModel;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by wasim on 14/11/16.
 */
public class VariantSearchIterator implements Iterator<VariantSearchModel>, AutoCloseable {

    //private long numFound = 0;
    private Iterator<VariantSearchModel> solrIterator;

    private SolrClient solrClient;
    private String collection;
    private SolrQuery solrQuery;
    private QueryResponse solrResponse;
    private String cursorMark, nextCursorMark;

    @Deprecated
    public VariantSearchIterator(Iterator<VariantSearchModel> solrIterator) {
        this.solrIterator = solrIterator;
    }

    public VariantSearchIterator(SolrClient solrClient, String collection, SolrQuery solrQuery)
            throws IOException, SolrServerException {
        this.solrClient = solrClient;
        this.collection = collection;
        this.solrQuery = solrQuery;

        init();
    }

    private void init() throws IOException, SolrServerException {
        solrQuery.setSort(SolrQuery.SortClause.asc("id"));
        solrQuery.setRows(100);
        cursorMark = CursorMarkParams.CURSOR_MARK_START;
        solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
        solrResponse = solrClient.query(collection, solrQuery);
        nextCursorMark = solrResponse.getNextCursorMark();
        solrIterator = solrResponse.getBeans(VariantSearchModel.class).iterator();
    }

    @Override
    public boolean hasNext() {
        if (solrIterator.hasNext()) {
            return true;
        } else {
            if (cursorMark.equals(nextCursorMark)) {
                return false;
            }
            try {
                cursorMark = nextCursorMark;
                solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
                solrResponse = solrClient.query(collection, solrQuery);
                nextCursorMark = solrResponse.getNextCursorMark();
                solrIterator = solrResponse.getBeans(VariantSearchModel.class).iterator();
                return solrIterator.hasNext();
            } catch (SolrServerException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return false;
        }
    }

    /*
    // Old function (without using Solr cursor) !!
    @Override
    public boolean hasNext() {
        return solrIterator.hasNext();
    }
    */

    @Override
    public VariantSearchModel next() {
        return solrIterator.next();
    }

    @Override
    public void close() throws Exception {
        // nothing to do
    }

    public long getNumFound() {
        return solrResponse.getResults().getNumFound();
    }

/*
    public long getNumFound() {
        return numFound;
    }

    public void setNumFound(long numFound) {
        this.numFound = numFound;
    }
*/
/*
    private Iterator<VariantSearchModel> solrIterator;

    public VariantSearchIterator(Iterator<VariantSearchModel> solrIterator) {
        this.solrIterator = solrIterator;
    }

    @Override
    public boolean hasNext() {
        return solrIterator.hasNext();
    }

    @Override
    public VariantSearchModel next() {
        return solrIterator.next();
    }

    @Override
    public void close() throws Exception {
    }
*/
}
