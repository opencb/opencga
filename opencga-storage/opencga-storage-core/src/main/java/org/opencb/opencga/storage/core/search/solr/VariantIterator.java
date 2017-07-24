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

package org.opencb.opencga.storage.core.search.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.search.VariantSearchToVariantConverter;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;

import java.io.IOException;

/**
 * Created by jtarraga on 01/03/17.
 */
public class VariantIterator extends VariantDBIterator {

    private VariantSearchIterator variantSearchIterator;
    private VariantSearchToVariantConverter variantSearchToVariantConverter;

    public VariantIterator(SolrClient solrClient, String collection, SolrQuery solrQuery) throws IOException, SolrServerException {
        variantSearchIterator = new VariantSearchIterator(solrClient, collection, solrQuery);
        variantSearchToVariantConverter = new VariantSearchToVariantConverter();
    }

    @Override
    public boolean hasNext() {
        return variantSearchIterator.hasNext();
    }

    @Override
    public Variant next() {
        return variantSearchToVariantConverter.convertToDataModelType(variantSearchIterator.next());
    }

    @Override
    public void close() throws Exception {
        // nothing to do
    }

    public long getNumFound() {
        return variantSearchIterator.getNumFound();
    }

}
