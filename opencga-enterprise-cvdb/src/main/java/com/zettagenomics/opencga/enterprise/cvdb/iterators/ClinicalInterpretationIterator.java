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

import com.zettagenomics.opencga.enterprise.cvdb.converters.ClinicalInterpretationConverter;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.models.ClinicalInterpretationSearch;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.opencb.opencga.core.models.clinical.Interpretation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jtarraga on 01/03/17.
 */
public class ClinicalInterpretationIterator extends ClinicalIncludeHandler implements Iterator<Interpretation>, AutoCloseable {

    private ClinicalSolrterator<ClinicalInterpretationSearch> nativeSolrIterator;
    private ClinicalInterpretationConverter converter;

    public ClinicalInterpretationIterator(SolrClient solrClient, String collection, SolrQuery solrQuery)
            throws IOException, SolrServerException {
        this(solrClient, collection, solrQuery, new ArrayList<>());
    }

    public ClinicalInterpretationIterator(SolrClient solrClient, String collection, SolrQuery solrQuery, List<String> includeList)
            throws IOException, SolrServerException {
        super(includeList);
        nativeSolrIterator = new ClinicalSolrterator<ClinicalInterpretationSearch>(solrClient, collection, solrQuery,
                ClinicalInterpretationSearch.class);
        converter = new ClinicalInterpretationConverter();
    }

    @Override
    public boolean hasNext() {
        return nativeSolrIterator.hasNext();
    }

    @Override
    public Interpretation next() {
        try {
            return applyInclude(converter.toInterpretation(nativeSolrIterator.next()));
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
}
