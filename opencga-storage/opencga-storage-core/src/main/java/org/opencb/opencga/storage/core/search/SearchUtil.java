package org.opencb.opencga.storage.core.search;

import org.apache.solr.client.solrj.SolrQuery;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.util.Map;

/**
 * Created by wasim on 18/11/16.
 */
public class SearchUtil {

    public enum VariantSolrFields {
        dbSNP,
        type,
        chromosome,
        start,
        end,
        gerp,
        caddRaw,
        caddScaled,
        phastCons,
        phylop,
        sift,
        polyphen,
        studies,
        genes,
        accessions
    }

    public static SolrQuery createSolrQuery(Query query, QueryOptions queryOptions) {


        StringBuilder queryString = new StringBuilder();
        SolrQuery solrQuery = new SolrQuery();

        for (VariantSolrFields value : VariantSolrFields.values()) {
            if (query.containsKey(value.name())) {
                queryString.append(value + ":" + query.get(value.name()));
            }
        }

        if (query.containsKey(VariantDBAdaptor.VariantQueryParams.ID.key())) {
            queryString.append("id:" + query.get(VariantDBAdaptor.VariantQueryParams.ID.key()));
        }

        if (query.containsKey("populations")) {
            queryString.append("study_populations:" + query.get("populations"));

        }

        if (query.containsKey("fl")) {
            solrQuery.addField(query.get("fl").toString());
        }

        if (query.containsKey("fq")) {
            solrQuery.addFilterQuery(query.get("fq").toString());
        }

        // QueryOptions
        if (queryOptions.containsKey(QueryOptions.INCLUDE)) {
            solrQuery.setFields(queryOptions.getAsStringList(QueryOptions.INCLUDE).toString());
         }

        if (queryOptions.containsKey(QueryOptions.LIMIT)) {
            solrQuery.setRows(queryOptions.getInt(QueryOptions.LIMIT));
        }

        if (queryOptions.containsKey(QueryOptions.SORT)) {
            solrQuery.addSort(queryOptions.getString(QueryOptions.SORT), getSortOrder(queryOptions));
        }

        //facet
        if (query.containsKey("facet.field")) {
            solrQuery.addFacetField((query.get("facet.field").toString()));
        }

        if (query.containsKey("facet.fields")) {
            solrQuery.addFacetField((query.get("facet.fields").toString().split(",")));
        }

        if (query.containsKey("facet.query")) {
            solrQuery.addFacetQuery(query.get("facet.query").toString());
        }

        if (query.containsKey("facet.prefix")) {
            solrQuery.setFacetPrefix(query.get("facet.prefix").toString());
        }

        //Facet Ranges
        if (query.containsKey("facet.range")) {

            Map<String, Map<String, Number>> rangeFields = (Map<String, Map<String, Number>>) query.get("facet.range");

            for (String key : rangeFields.keySet()) {
                Number rangeStart = rangeFields.get(key).get("facet.range.start");
                Number rangeEnd = rangeFields.get(key).get("facet.range.end");
                Number rangeGap = rangeFields.get(key).get("facet.range.gap");
                solrQuery.addNumericRangeFacet(key, rangeStart, rangeEnd, rangeGap);
            }
        }

        solrQuery.setQuery(queryString.toString());
        return solrQuery;
    }

    private static SolrQuery.ORDER getSortOrder(QueryOptions queryOptions) {

        return queryOptions.getString(QueryOptions.ORDER).equals(QueryOptions.ASCENDING) ? SolrQuery.ORDER.asc : SolrQuery.ORDER.desc;

    }
}
