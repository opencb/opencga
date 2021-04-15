package org.opencb.opencga.analysis.rga.iterators;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.opencb.opencga.analysis.rga.RgaDataModel;

public class RgaIterator extends SolrNativeIterator<RgaDataModel> {

    public RgaIterator(SolrClient solrClient, String collection, SolrQuery solrQuery) throws SolrServerException {
        super(solrClient, collection, solrQuery, RgaDataModel.class);
    }

}
