/*
 * Copyright 2015-2016 OpenCB
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

import org.apache.solr.client.solrj.SolrQuery;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by wasim on 18/11/16.
 */
public class ParseSolrQuery {

    private static final Pattern SCORE_PATTERN = Pattern.compile("^([^=<>]+.*)(<=?|>=?|!=|!?=?~|==?)([^=<>]+.*)$");

    public enum IdFields {
        dbSNP,
        type,
        chromosome
    }

    public enum ScoreFields {
        gerp,
        caddRaw,
        caddScaled,
        phastCons,
        phylop,
        sift,
        polyphen
    }

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

    /**
     * Create a SolrQuery object from Query and QueryOptions.
     *
     * @param query         Query
     * @param queryOptions  Query Options
     * @return              SolrQuery
     */
    public static SolrQuery parse(Query query, QueryOptions queryOptions) {
        List<String> filterList = new ArrayList<>();

//        StringBuilder queryString = new StringBuilder();
        SolrQuery solrQuery = new SolrQuery();

        //-------------------------------------
        // QueryOptions processing
        //-------------------------------------
        if (queryOptions.containsKey(QueryOptions.INCLUDE)) {
            solrQuery.setFields(queryOptions.getAsStringList(QueryOptions.INCLUDE).toString());
        }

        if (queryOptions.containsKey(QueryOptions.LIMIT)) {
            solrQuery.setRows(queryOptions.getInt(QueryOptions.LIMIT));
        }

        if (queryOptions.containsKey(QueryOptions.SORT)) {
            solrQuery.addSort(queryOptions.getString(QueryOptions.SORT), getSortOrder(queryOptions));
        }

        //-------------------------------------
        // Query processing
        //-------------------------------------
//        for (VariantSolrFields value : VariantSolrFields.values()) {
//            if (query.containsKey(value.name())) {
//                queryString.append(value + ":" + query.get(value.name()));
//            }
//        }

        // parse id/name values, e.g.: dbSNP, type, chromosome,...
        filterList.addAll(parseIdValues(query));

        // parse score values, e.g.: gerp, caddRaw, caddScaled,...
        filterList.addAll(parseScoreValues(query));

//        if (query.containsKey(VariantDBAdaptor.VariantQueryParams.ID.key())) {
//            queryString.append("id:" + query.get(VariantDBAdaptor.VariantQueryParams.ID.key()));
//        }
//
//        if (query.containsKey("populations")) {
//            queryString.append("study_populations:" + query.get("populations"));
//        }

//        if (query.containsKey("fl")) {
//            solrQuery.addField(query.get("fl").toString());
//        }
//
//        if (query.containsKey("fq")) {
//            solrQuery.addFilterQuery(query.get("fq").toString());
//        }

        // process facet options
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

        if (query.containsKey("facet.range")) {

            Map<String, Map<String, Number>> rangeFields = (Map<String, Map<String, Number>>) query.get("facet.range");

            for (String key : rangeFields.keySet()) {
                Number rangeStart = rangeFields.get(key).get("facet.range.start");
                Number rangeEnd = rangeFields.get(key).get("facet.range.end");
                Number rangeGap = rangeFields.get(key).get("facet.range.gap");
                solrQuery.addNumericRangeFacet(key, rangeStart, rangeEnd, rangeGap);
            }
        }

//        solrQuery.setQuery(queryString.toString());
        solrQuery.setQuery("*:*");
        filterList.forEach(filter -> solrQuery.addFilterQuery(filter));
        return solrQuery;
    }

    /**
     * Parse string values, e.g.: dbSNP, type, chromosome,... This function takes into account multiple values and
     * the separator between them can be:
     *     "," to apply a "OR condition"
     *     ";" to apply a "AND condition"
     *
     * @param query         Query containing the values to parse
     * @return              A list of strings, each string represents a boolean condition
     */
    private static List<String> parseIdValues(Query query) {
        List<String> filters = new ArrayList<>();

        for (IdFields param: IdFields.values()) {
            if (query.containsKey(param.name())) {
                String value = (String) query.get(param.name());
                if (value != null && !value.isEmpty()) {
                    boolean or = value.contains(",");
                    boolean and = value.contains(";");
                    if (or && and) {
                        throw new IllegalArgumentException("Command and semi-colon cannot be mixed: " + value);
                    }

                    String[] values = value.split("[,;]");
                    StringBuilder filter = new StringBuilder();
                    if (values.length == 1) {
                        filter.append(value).append(":\"").append(value).append("\"");
                    } else {
                        String logicalComparator = or ? " OR " : " AND ";
                        filter.append("(");
                        filter.append(value).append(":\"").append(values[0]).append("\"");
                        for (int i = 1; i < values.length; i++) {
                            filter.append(logicalComparator);
                            filter.append(value).append(":\"").append(values[i]).append("\"");
                        }
                        filter.append(")");
                    }
                    filters.add(filter.toString());
                }
            }
        }

        return filters;
    }

    /**
     * Parse string values, e.g.: gerp, caddRaw,... This function takes into account multiple values and
     * the separator between them can be:
     *     "," to apply a "OR condition"
     *     ";" to apply a "AND condition"
     *
     * @param query         Query containing the values to parse
     * @return              A list of strings, each string represents a boolean condition
     */
    private static List<String> parseScoreValues(Query query) {
        List<String> filters = new ArrayList<>();

        for (IdFields param: IdFields.values()) {
            if (query.containsKey(param.name())) {
                String value = (String) query.get(param.name());
                if (value != null && !value.isEmpty()) {

                    boolean or = value.contains(",");
                    boolean and = value.contains(";");
                    if (or && and) {
                        throw new IllegalArgumentException("Command and semi-colon cannot be mixed: " + value);
                    }
                    String logicalComparator = or ? " OR " : " AND ";

                    String[] values = value.split("[,;]");

                    Matcher matcher;
                    StringBuilder sb = new StringBuilder();

                    if (values == null) {
                        matcher = SCORE_PATTERN.matcher(value);
                        if (matcher.find()) {
                            // concat expresion, e.g.: value:[0 TO 12]
                            int aaa = 0;
                        } else {
                            // error
                            throw new IllegalArgumentException("Invalid expresion " +  value);
                        }
                    } else {
                        matcher = SCORE_PATTERN.matcher(values[0]);
                        if (matcher.find()) {
                            sb.append("(");
                            // concat expresion, e.g.: value:[0 TO 12]
                            for (int i = 1; i < values.length; i++) {
                                matcher = SCORE_PATTERN.matcher(values[i]);
                                if (matcher.find()) {
                                    sb.append(logicalComparator);
                                    // concat expresion, e.g.: value:[0 TO 12]
                                    int aaa = 0;
                                } else {
                                    // error
                                    throw new IllegalArgumentException("Invalid expresion " +  value);
                                }
                            }
                            sb.append(")");
                        } else {
                            // error
                            System.err.format("Error: invalid expresion %s: abort!\n", values[0]);
                        }
                    }
                    filters.add(sb.toString());
                }
            }
        }

        return filters;
    }

    private static SolrQuery.ORDER getSortOrder(QueryOptions queryOptions) {

        return queryOptions.getString(QueryOptions.ORDER).equals(QueryOptions.ASCENDING) ? SolrQuery.ORDER.asc : SolrQuery.ORDER.desc;

    }
}
