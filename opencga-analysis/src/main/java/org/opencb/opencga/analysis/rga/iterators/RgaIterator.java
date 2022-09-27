package org.opencb.opencga.analysis.rga.iterators;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.opencb.opencga.analysis.rga.RgaDataModel;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

public class RgaIterator extends SolrNativeIterator<RgaDataModel> {

    public RgaIterator(SolrClient solrClient, String collection, SolrQuery solrQuery) throws SolrServerException {
        super(solrClient, collection, solrQuery, Collections.emptyList(), RgaDataModel.class);
    }

    public RgaIterator(SolrClient solrClient, String collection, List<Predicate<RgaDataModel>> filters, SolrQuery solrQuery)
            throws SolrServerException {
        super(solrClient, collection, solrQuery, filters, RgaDataModel.class);
    }

}
