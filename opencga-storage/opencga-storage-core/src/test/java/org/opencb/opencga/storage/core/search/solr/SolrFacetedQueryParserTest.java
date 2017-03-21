package org.opencb.opencga.storage.core.search.solr;

import org.apache.solr.client.solrj.SolrQuery;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.results.FacetedQueryResultItem;
import org.opencb.opencga.core.results.VariantFacetedQueryResult;
import org.opencb.opencga.storage.core.config.SearchConfiguration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.search.VariantSearchManager;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by jtarraga on 09/03/17.
 */
public class SolrFacetedQueryParserTest {

    //public String host = "http://localhost:8983/solr/";
    public String host = "http://bioinfo.hpc.cam.ac.uk/solr/"; //hgvav1_hgvauser_reference_grch37/select?facet=on&fq=chromosome:22&indent=on&q=*:*&rows=0&wt=json&facet.field=studies&facet.field=type

    //public String collection = "test1";
    public String collection = "hgvav1_hgvauser_reference_grch37";

    public String study = collection;

    public void executeFacetedQuery(Query facetedQuery, Query query, QueryOptions queryOptions) {
        String user = "";
        String password = "";
        boolean active = true;
        int rows = 10;
        StorageConfiguration config = new StorageConfiguration();
        config.setSearch(new SearchConfiguration(host, collection, user, password, active, rows));
        VariantSearchManager searchManager = new VariantSearchManager(null, config);
        try {
            VariantFacetedQueryResult<Variant> result = searchManager.facetedQuery(collection, facetedQuery, query, queryOptions);

            if (result.getResult() != null) {
                for (FacetedQueryResultItem item: result.getResult()) {
                    System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(item));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void facetField() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        // query for chromosome 2
        query.put("region", "22");

        Query facetedQuery = new Query();
        // two facets: 1) by type, and 2) by studies
        facetedQuery.put("facet-field", "type,studies");

        // execute
        executeFacetedQuery(facetedQuery, query, queryOptions);
    }


    public void facetNestedFields() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        // query for chromosome 2
        query.put("region", "22");

        Query facetedQuery = new Query();
        // two facets: 1) by nested fields: studies and soAcc, and 2) by type
        facetedQuery.put("facet-field", "studies:soAcc:type");

        // execute
        executeFacetedQuery(facetedQuery, query, queryOptions);
    }


    public void facetRanges() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        // query for chromosome 2
        query.put("region", "22");

        Query facetedQuery = new Query();
        // two ranges: 1) phylop from 0 to 1 step 0.3, and 2) caddScaled from 0 to 30 step 5
        facetedQuery.put("facet-range", "phylop:0:1:0.3,caddScaled:0:30:5");

        // execute
        executeFacetedQuery(facetedQuery, query, queryOptions);
    }

    public void facetFieldAndRange() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        // query for chromosome 2
        query.put("region", "22");
        System.out.println(query.toString());

        Query facetedQuery = new Query();
        // two facets: 1) by nested fields: studies and type, and 2) by soAcc
        facetedQuery.put("facet-field", "studies:type,soAcc");
        // two ranges: 1) phylop from 0 to 1 step 0.3, and 2) caddScaled from 0 to 30 step 2
        facetedQuery.put("facet-range", "phylop:0:1:0.3,caddScaled:0:30:2");

        // execute
        executeFacetedQuery(facetedQuery, query, queryOptions);
    }

    @Test
    public void testParsing() {
        facetField();
        facetNestedFields();
        facetRanges();
        facetFieldAndRange();
    }

}