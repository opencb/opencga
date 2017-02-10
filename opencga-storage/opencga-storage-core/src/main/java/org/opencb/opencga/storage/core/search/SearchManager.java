package org.opencb.opencga.storage.core.search;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.config.SearchConfiguration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.search.solr.ParseSolrQuery;
import org.opencb.opencga.storage.core.search.solr.SolrVariantSearchIterator;

import java.io.IOException;
import java.util.List;

/**
 * Created by wasim on 09/11/16.
 */
public class SearchManager {

    private SearchConfiguration searchConfiguration;
//    private SolrClient solrClient;
    private HttpSolrClient solrClient;
    private static VariantSearchFactory variantSearchFactory;


    public SearchManager() {
        //TODO remove testing constructor
        if (this.solrClient == null) {
//            this.solrClient = new HttpSolrClient("http://localhost:8983/solr/variants");
            this.solrClient = new HttpSolrClient.Builder("http://localhost:8983/solr/variants").build();
            solrClient.setRequestWriter(new BinaryRequestWriter());

        }
        if (variantSearchFactory == null) {
            variantSearchFactory = new VariantSearchFactory();
        }
    }

    public SearchManager(String host, String collection) {
        if (this.solrClient == null) {
//            this.solrClient = new HttpSolrClient(host + collection);
            this.solrClient = new HttpSolrClient.Builder(host + collection).build();
            solrClient.setRequestWriter(new BinaryRequestWriter());
//            this.solrClient = new ConcurrentUpdateSolrClient.Builder(host+ collection).build();
        }
        if (variantSearchFactory == null) {
            variantSearchFactory = new VariantSearchFactory();
        }
    }

    public SearchManager(StorageConfiguration storageConfiguration) {

        this.searchConfiguration = storageConfiguration.getSearch();

        if (searchConfiguration.getHost() != null && searchConfiguration.getCollection() != null && solrClient == null) {
//            solrClient = new HttpSolrClient(searchConfiguration.getHost() + searchConfiguration.getCollection());
            this.solrClient = new HttpSolrClient.Builder(searchConfiguration.getHost() + searchConfiguration.getCollection()).build();
//            solrClient.setRequestWriter(new BinaryRequestWriter());
//            HttpClientUtil.setBasicAuth((DefaultHttpClient) solrClient.getHttpClient(), searchConfiguration.getUser(),
//                    searchConfiguration.getPassword());
        }

        if (variantSearchFactory == null) {
            variantSearchFactory = new VariantSearchFactory();
        }
    }

    public void insert(List<Variant> variants) {

        List<VariantSearch> variantSearches = variantSearchFactory.create(variants);

        if (!variantSearches.isEmpty()) {
            try {
                UpdateResponse updateResponse = solrClient.addBeans(variantSearches);
                if (0 == updateResponse.getStatus()) {
                    solrClient.commit();
                }
            } catch (SolrServerException | IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void insert(Variant variant) {

        VariantSearch variantSearch = variantSearchFactory.create(variant);

        if (variantSearch != null && variantSearch.getId() != null) {
            try {
                UpdateResponse updateResponse = solrClient.addBean(variantSearch);
                if (0 == updateResponse.getStatus()) {
                    solrClient.commit();
                }
            } catch (SolrServerException | IOException e) {
                e.printStackTrace();
            }
        }
    }

    public SolrVariantSearchIterator iterator(Query query, QueryOptions queryOptions) {

        SolrQuery solrQuery = ParseSolrQuery.parse(query, queryOptions);
        QueryResponse response = null;

        try {
            response = solrClient.query(solrQuery);
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }

        return new SolrVariantSearchIterator(response.getBeans(VariantSearch.class).iterator());
    }

    public VariantSearchFacet getFacet(Query query, QueryOptions queryOptions) {

        SolrQuery solrQuery = ParseSolrQuery.parse(query, queryOptions);
        QueryResponse response = null;

        try {
            response = solrClient.query(solrQuery);
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }

        return getFacets(response);
    }

    private VariantSearchFacet getFacets(QueryResponse response) {

        VariantSearchFacet variantSearchFacet = new VariantSearchFacet();

        if (response.getFacetFields() != null) {
            variantSearchFacet.setFacetFields(response.getFacetFields());
        }
        if (response.getFacetQuery() != null) {
            variantSearchFacet.setFacetQueries(response.getFacetQuery());
        }
        if (response.getFacetRanges() != null) {
            variantSearchFacet.setFacetRanges(response.getFacetRanges());
        }
        if (response.getIntervalFacets() != null) {
            variantSearchFacet.setFacetIntervales(response.getIntervalFacets());
        }

        return variantSearchFacet;
    }

    public static VariantSearchFactory getVariantSearchFactory() {
        return variantSearchFactory;
    }
}
