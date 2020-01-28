/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.storage.core.variant.search.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter;

/**
 * Created by jtarraga on 01/03/17.
 */
public class SolrVariantDBIterator extends VariantDBIterator {

    private SolrNativeIterator solrNativeIterator;
    private VariantSearchToVariantConverter variantSearchToVariantConverter;
    private int count = 0;

    public SolrVariantDBIterator(SolrClient solrClient, String collection, SolrQuery solrQuery, VariantSearchToVariantConverter converter)
            throws SolrServerException {
        solrNativeIterator = new SolrNativeIterator(solrClient, collection, solrQuery);
        this.variantSearchToVariantConverter = converter;
    }

    @Override
    public boolean hasNext() {
        return solrNativeIterator.hasNext();
    }

    @Override
    public Variant next() {
        count++;
        return variantSearchToVariantConverter.convertToDataModelType(solrNativeIterator.next());
    }

    @Override
    public void close() throws Exception {
        // nothing to do
    }

    @Override
    public int getCount() {
        return count;
    }

    public long getNumFound() {
        return solrNativeIterator.getNumFound();
    }

}
