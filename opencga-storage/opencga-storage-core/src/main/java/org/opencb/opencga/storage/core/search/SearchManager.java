package org.opencb.opencga.storage.core.search;

import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.config.SearchConfiguration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.search.iterators.SolrVariantSearchIterator;

import java.io.IOException;
import java.util.List;

/**
 * Created by wasim on 09/11/16.
 */
public class SearchManager {

    private SearchConfiguration searchConfiguration;
    private static VariantSearchFactory variantSearchFactory;
    private static HttpSolrClient solrServer;

    public SearchManager() {
        //TODO remove testing constructor
        if (this.solrServer == null) {
            this.solrServer = new HttpSolrClient("http://localhost:8983/solr/variants");
            solrServer.setRequestWriter(new BinaryRequestWriter());
        }
    }

    public SearchManager(StorageConfiguration storageConfiguration) {

        this.searchConfiguration = storageConfiguration.getSearch();

        if (searchConfiguration.getHost() != null && searchConfiguration.getCollection() != null && solrServer == null) {
            solrServer = new HttpSolrClient(searchConfiguration.getHost() + searchConfiguration.getCollection());
            solrServer.setRequestWriter(new BinaryRequestWriter());
            HttpClientUtil.setBasicAuth((DefaultHttpClient) solrServer.getHttpClient(), searchConfiguration.getUser(),
                    searchConfiguration.getPassword());
        }

        if (variantSearchFactory == null) {
            variantSearchFactory = new VariantSearchFactory();
        }
    }

    public void insert(List<Variant> variants) {

        List<VariantSearch> variantSearches = variantSearchFactory.create(variants);

        if (!variantSearches.isEmpty()) {
            try {
                UpdateResponse updateResponse = solrServer.addBeans(variantSearches);
                if (0 == updateResponse.getStatus()) {
                    solrServer.commit();
                }
            } catch (SolrServerException | IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void insert(Variant variant) {

        VariantSearch variantSearche = variantSearchFactory.create(variant);

        if (variantSearche != null && variantSearche.getId() != null) {
            try {
                UpdateResponse updateResponse = solrServer.addBean(variantSearche);
                if (0 == updateResponse.getStatus()) {
                    solrServer.commit();
                }
            } catch (SolrServerException | IOException e) {
                e.printStackTrace();
            }
        }
    }

    public SolrVariantSearchIterator iterator(Query query, QueryOptions queryOptions) {

        SolrQuery solrQuery = SearchUtil.createSolrQuery(query, queryOptions);
        QueryResponse response = null;

        try {
            response = solrServer.query(solrQuery);
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }

        return new SolrVariantSearchIterator(response.getBeans(VariantSearch.class).iterator());
    }

    public VariantSearchFacet facet(Query query, QueryOptions queryOptions) {

        SolrQuery solrQuery = SearchUtil.createSolrQuery(query, queryOptions);
        QueryResponse response = null;

        try {
            response = solrServer.query(solrQuery);
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }

        return getFacets(response);
    }

    private VariantSearchFacet getFacets(QueryResponse response) {

        VariantSearchFacet variantSearchFacet = new VariantSearchFacet();

        if (response.getFacetFields() != null) {
            variantSearchFacet.setFacetFields(response.getFacetFields());
        } else if (response.getFacetQuery() != null) {
            variantSearchFacet.setFacetQueries(response.getFacetQuery());
        } else if (response.getFacetRanges() != null) {
            variantSearchFacet.setFacetRanges(response.getFacetRanges());
        } else if (response.getIntervalFacets() != null) {
            variantSearchFacet.setFacetIntervales(response.getIntervalFacets());
        }

        return variantSearchFacet;
    }
}
