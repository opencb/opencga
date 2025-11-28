/*
 * Copyright 2015-2020 OpenCB
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

package com.zettagenomics.opencga.enterprise.cvdb.iterators;

import com.zettagenomics.opencga.enterprise.cvdb.converters.SearchConverter;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.opencb.commons.datastore.core.QueryOptions;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;

/**
 * Created by jtarraga on 01/03/17.
 */
public class ClinicalIterator<M,N,C extends SearchConverter<M, N>> extends ClinicalIncludeHandler implements Iterator<M>, AutoCloseable {

    private ClinicalSolrIterator<N> nativeSolrIterator;
    private C converter;
    private Class<C> converterType;

    public ClinicalIterator(SolrClient solrClient, String collection, SolrQuery solrQuery, QueryOptions queryOptions,
                            Class<N> nativeType, Class<C> converterType)
            throws IOException, SolrServerException, NoSuchMethodException, InvocationTargetException, InstantiationException,
            IllegalAccessException {
        super(queryOptions);
        nativeSolrIterator = new ClinicalSolrIterator<N>(solrClient, collection, solrQuery, nativeType);
        converter = converterType.getConstructor().newInstance();
    }

    @Override
    public boolean hasNext() {
        return nativeSolrIterator.hasNext();
    }

    @Override
    public M next() {
        try {
            return applyInclude(converter.toModel(nativeSolrIterator.next()));
        } catch (CvdbException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void close() throws Exception {
        // nothing to do
    }

    public long getNumFound() {
        return nativeSolrIterator.getNumFound();
    }

    public C getConverter() {
        return converter;
    }
}
