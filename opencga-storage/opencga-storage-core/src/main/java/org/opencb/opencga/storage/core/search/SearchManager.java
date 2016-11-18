package org.opencb.opencga.storage.core.search;

import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RangeFacet;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.config.SearchConfiguration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.search.iterators.SolrVariantSearchIterator;

import java.io.IOException;
import java.util.*;

/**
 * Created by wasim on 09/11/16.
 */
public class SearchManager {

    private SearchConfiguration searchConfiguration;
    private Set<String> queryFields;
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
            } catch (SolrServerException e) {
                e.printStackTrace();
            } catch (IOException e) {
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
            } catch (SolrServerException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public SolrVariantSearchIterator iterator(Query query, QueryOptions queryOptions) {

        SolrQuery solrQuery = SearchUtil.createSolrQuery(query, queryOptions);
        QueryResponse response = null;

        try {
            response = solrServer.query(solrQuery);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Iterator it = response.getResults().iterator();
       //System.out.println(response.getFacetRanges().get(0).getCounts().get(0).);


        List<RangeFacet.Count> rangeEntries = response.getFacetRanges().get(0).getCounts();
        if (rangeEntries != null) {
            for (RangeFacet.Count fcount : rangeEntries) {
                System.out.println((fcount.getValue()) + " <===> " + fcount.getCount());
            }
        }
        // System.out.println(response.getFacetFields().get(0).getValueCount());
        System.out.println(response.getFacetRanges().get(1).getName());
        System.out.println(response.getFacetRanges().get(0).getStart());
        System.out.println(response.getFacetRanges().get(0).getEnd());


        System.out.println("\n\n\n" + solrQuery.toString());
       /* Iterator<VariantSearch> varIterator = response.getBeans(VariantSearch.class).iterator();
        return new SolrVariantSearchIterator(varIterator);*/
        return null;
    }

    public VariantSearchFacet facet(Query query, QueryOptions queryOptions) {

        SolrQuery solrQuery = SearchUtil.createSolrQuery(query, queryOptions);
        QueryResponse response = null;

        try {
            response = solrServer.query(solrQuery);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
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


    public static void main(String[] args) {
        String fileName = "/home/wasim/Downloads/variation_chr1.full.json.gz";
        SearchManager searchManager = new SearchManager();
        double startTime = System.currentTimeMillis();
        System.out.println("Start Time Min : " + startTime / 60000.0 % 60.0);

        Query query = new Query();
        // query.append("ids", "*");
        query.append("start", "16050699");
        query.append("chromosome", 22);
        query.append("fl", "start,end,type");
        query.append("fq", "start:[0 TO 16050654]");

        query.append("facet.fields", "type,sift");
        query.append("facet.field", "chromosome");

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.add(QueryOptions.SORT, "end");
        queryOptions.add(QueryOptions.ORDER, QueryOptions.DESCENDING);
        queryOptions.add(QueryOptions.LIMIT, 1000);

        Map<String, Map<String, Double>> rangeFields = new HashMap<>();

        Map<String, Double> polyRField = new HashMap<>();
        polyRField.put("facet.range.start", 0.0);
        polyRField.put("facet.range.end", 10.1);
        polyRField.put("facet.range.gap", 0.1);
        rangeFields.put("polyphen", polyRField);

        Map<String, Double> sift = new HashMap<>();
        sift.put("facet.range.start", 0.0);
        sift.put("facet.range.end", 10.1);
        sift.put("facet.range.gap", 0.1);
        rangeFields.put("sift", sift);

        query.append("facet.range", rangeFields);

        searchManager.iterator(query, queryOptions);

        double endTime = System.currentTimeMillis();
        System.out.println("End Time : " + endTime / 60000.0 % 60.0);
        System.out.println("Total Time : " + (endTime - startTime) / 60000.0 % 60.0);
    }
}
