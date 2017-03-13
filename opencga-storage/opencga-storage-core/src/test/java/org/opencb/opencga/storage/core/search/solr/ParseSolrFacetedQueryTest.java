package org.opencb.opencga.storage.core.search.solr;

import org.apache.solr.client.solrj.SolrQuery;
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
public class ParseSolrFacetedQueryTest {

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
        VariantSearchManager searchManager = new VariantSearchManager(config);
        try {
            VariantFacetedQueryResult<Variant> result = searchManager.facetedQuery(collection, facetedQuery, query, queryOptions);

            if (result.getResult() != null) {
                for (FacetedQueryResultItem item: result.getResult()) {
                    System.out.println(item);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void facetType() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put("region", "22");

        Query facetedQuery = new Query();
        facetedQuery.put("facet.field", "type");
//        facetedQuery.put("facet.field", "type,studies/genes/type");
        //facetedQuery.put("facet.range", "phylop:0:1:0.3");

        // execute
        executeFacetedQuery(facetedQuery, query, queryOptions);
    }

    @Test
    public void testParsing() {
        facetType();
    }

}