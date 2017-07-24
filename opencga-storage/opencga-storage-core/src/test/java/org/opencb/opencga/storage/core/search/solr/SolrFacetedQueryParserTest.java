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

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.result.FacetedQueryResult;
import org.opencb.opencga.storage.core.config.SearchConfiguration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;

/**
 * Created by jtarraga on 09/03/17.
 */
public class SolrFacetedQueryParserTest {

    //public String host = "http://localhost:8983/solr/";
    public String host = "http://bioinfo.hpc.cam.ac.uk/solr/"; //hgvav1_hgvauser_reference_grch37/select?facet=on&fq=chromosome:22&indent=on&q=*:*&rows=0&wt=json&facet.field=studies&facet.field=type

    //public String collection = "test1";
    public String mode = "cloud";
    public String collection = "hgvav1_hgvauser_reference_grch37";

    public String study = collection;

    public void executeFacetedQuery(Query query, QueryOptions queryOptions) {
        String user = "";
        String password = "";
        boolean active = true;
        int timeout = 300 * 1000; // SearchConfiguration.DEFAULT_TIMEOUT;
        int rows = 0;
        StorageConfiguration config = new StorageConfiguration();
        config.setSearch(new SearchConfiguration(host, mode, user, password, active, timeout, rows));
        VariantSearchManager searchManager = new VariantSearchManager(null, null, config);
        try {
            queryOptions.put(QueryOptions.LIMIT, rows);
            FacetedQueryResult result = searchManager.facetedQuery(collection, query, queryOptions);

            if (result.getResult() != null) {
                System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(result.getResult()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void facetField() {
        QueryOptions queryOptions = new QueryOptions();
        // two facets: 1) by type, and 2) by studies
        queryOptions.put(QueryOptions.FACET, "type;studies:2:2");

        Query query = new Query();
        // query for chromosome 2
        query.put("region", "22");


        // execute
        executeFacetedQuery(query, queryOptions);
    }

    public void facetFieldInclude() {
        QueryOptions queryOptions = new QueryOptions();
        // two facets: 1) by type, and 2) by studies
        //queryOptions.put(QueryOptions.FACET, "studies[EXAC,1kG_phase3]");
        //queryOptions.put(QueryOptions.FACET, "type[INDEL,SNV];genes[CNTN5,CTNNA3]");
        queryOptions.put(QueryOptions.FACET, "genes[CNTN5,CTNNA3]");

        Query query = new Query();
        // query for chromosome 2
        //query.put("region", "22");


        // execute
        executeFacetedQuery(query, queryOptions);
    }


    public void facetNestedFields() {
        QueryOptions queryOptions = new QueryOptions();
        // two facets: 1) by nested fields: studies and soAcc, and 2) by type
        //queryOptions.put(QueryOptions.FACET, "studies[EXAC,GONL]:2:2>>type");
        //queryOptions.put(QueryOptions.FACET, "studies>>type[INDEL,SNV]");
        queryOptions.put(QueryOptions.FACET, "studies[EXAC,GONL]>>type[INDEL,SNV]");
        //queryOptions.put(QueryOptions.FACET, "studies>>genes[BRCA2]");

        Query query = new Query();
        // query for chromosome 2
        query.put("region", "22");


        // execute
        executeFacetedQuery(query, queryOptions);
    }


    public void facetRanges() {
        QueryOptions queryOptions = new QueryOptions();
        // two ranges: 1) phylop from 0 to 1 step 0.3, and 2) caddScaled from 0 to 30 step 5
        queryOptions.put(QueryOptions.FACET_RANGE, "start:1:50000000:1000000;phylop:0:1:0.3;caddScaled:0:30:5");

        Query query = new Query();
        // query for chromosome 2
        query.put("region", "22");

        // execute
        executeFacetedQuery(query, queryOptions);
    }

    public void facetFieldAndRange() {
        QueryOptions queryOptions = new QueryOptions();
        // two facets: 1) by nested fields: studies and type, and 2) by soAcc
        queryOptions.put(QueryOptions.FACET, "studies>>type;soAcc");
        // two ranges: 1) phylop from 0 to 1 step 0.3, and 2) caddScaled from 0 to 30 step 2
        queryOptions.put(QueryOptions.FACET_RANGE, "phylop:0:1:0.3;caddScaled:0:30:2");

        Query query = new Query();
        // query for chromosome 2
        query.put("region", "22");
        System.out.println(query.toString());

        // execute
        executeFacetedQuery(query, queryOptions);
    }

    public void facetIntersection() {
        QueryOptions queryOptions = new QueryOptions();
        // two facets: 1) by nested fields: studies and type, and 2) by soAcc
        queryOptions.put(QueryOptions.FACET_INTERSECTION, "studies:1kG_phase3:EXAC:ESP6500;studies:MGP:GONL:EXAC");

        Query query = new Query();
        // query for chromosome 2
        query.put("region", "22");
        System.out.println(query.toString());

        // execute
        executeFacetedQuery(query, queryOptions);
    }

    @Test
    public void testParsing() {
        //facetFieldInclude();
        //facetField();
        //facetNestedFields();
        //facetRanges();
        //facetFieldAndRange();
        facetIntersection();
    }

}