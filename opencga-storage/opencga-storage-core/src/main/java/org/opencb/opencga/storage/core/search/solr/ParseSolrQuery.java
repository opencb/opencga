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

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import scala.collection.mutable.StringBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by wasim on 18/11/16.
 */
public class ParseSolrQuery {

    private static final Pattern STUDY_PATTERN = Pattern.compile("^([^=<>]+):([^=<>]+)(<=?|>=?|=?)([^=<>]+.*)$");
    private static final Pattern SCORE_PATTERN = Pattern.compile("^([^=<>]+)(<=?|>=?|=?)([^=<>]+.*)$");

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
        String key;
        List<String> orFilterList = new ArrayList<>();
        System.out.println("query = \n" + query.toJson() + "\n");

        // id, to the OR filter list
        key = VariantDBAdaptor.VariantQueryParams.ID.key();
        orFilterList.addAll(parseTermValue("xrefs", (String) query.get(key)));

        // type (t)
        key = VariantDBAdaptor.VariantQueryParams.TYPE.key();
        filterList.addAll(parseTermValue(key, (String) query.get(key)));

        // consequence type and gene
        key = VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key();
        String geneKey = VariantDBAdaptor.VariantQueryParams.GENE.key();
        if (query.get(key) != null && !((String) query.get(key)).isEmpty()
            && query.get(geneKey) != null && !((String) query.get(geneKey)).isEmpty()) {

            // special case, check geneToSoAcc from the VariantSearchModel
            List<String> genes = Arrays.asList(((String) query.get(geneKey)).split("[,;]"));
            List<String> cts = Arrays.asList(((String) query.get(key)).replace("SO:", "").split("[,;]"));
            for (String gene: genes) {
                for (String ct: cts) {
                    filterList.addAll(parseTermValue("geneToSoAcc", gene + "_" + Integer.parseInt(ct)));
                }
            }

        } else if (query.get(key) != null && !((String) query.get(key)).isEmpty()) {

            // only consequence type (accession)
            filterList.addAll(parseTermValue("soAcc", ((String) query.get(key)).replace("SO:", "")));

        } else if (query.get(geneKey) != null && !((String) query.get(geneKey)).isEmpty()) {

            // only gene, therefore to the OR filter list
            orFilterList.addAll(parseTermValue("xrefs", (String) query.get(geneKey)));
        }

        // region, to the OR filter list
        if (query.containsKey(VariantDBAdaptor.VariantQueryParams.REGION)) {
            StringBuilder sb = new StringBuilder();
            List<Region> regions = new ArrayList<>();
            String[] regionStr = ((String) query.get(VariantDBAdaptor.VariantQueryParams.REGION)).split("[,;]");
            for (String regStr: regionStr) {
                Region region = new Region(regStr);
                sb.setLength(0);
                sb.append("(chromosome:").append(region.getChromosome())
                        .append(" AND start:[").append(region.getStart()).append(" TO *]")
                        .append(" AND end:[* TO ").append(region.getEnd()).append("]");
                orFilterList.add(sb.toString());
            }
        }

        // group all OR filters, i.e.: genes, ids and regions; and add them to the filter list
        if (orFilterList.size() > 0) {
            filterList.add(StringUtils.join(orFilterList, " OR "));
        }

        // clinvar
        key = VariantDBAdaptor.VariantQueryParams.ANNOT_CLINVAR.key();
        filterList.addAll(parseTraitValue("ClinVar", (String) query.get(key)));

        // cosmic
        key = VariantDBAdaptor.VariantQueryParams.ANNOT_COSMIC.key();
        filterList.addAll(parseTraitValue("COSMIC", (String) query.get(key)));

        // hpo
        key = VariantDBAdaptor.VariantQueryParams.ANNOT_HPO.key();
        filterList.addAll(parseTraitValue("HPO", (String) query.get(key)));

        // cadd, functional score
        key = VariantDBAdaptor.VariantQueryParams.ANNOT_FUNCTIONAL_SCORE.key();
        filterList.addAll(parseScoreValue(key, (String) query.get(key)));

        // conservation
        key = VariantDBAdaptor.VariantQueryParams.ANNOT_CONSERVATION.key();
        filterList.addAll(parseScoreValue(key, (String) query.get(key)));

        // protein-substitution
        key = VariantDBAdaptor.VariantQueryParams.ANNOT_PROTEIN_SUBSTITUTION.key();
        filterList.addAll(parseScoreValue(key, (String) query.get(key)));

        // alt population frequency
        // in the model: "popFreq__1kG_phase3__CLM":0.005319148767739534
        key = VariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key();
        filterList.addAll(parsePopValue("popFreq", (String) query.get(key)));

        // stats maf
        // in the model: "stats__1kg_phase3__ALL"=0.02
        key = VariantDBAdaptor.VariantQueryParams.STATS_MAF.key();
        filterList.addAll(parsePopValue("stats", (String) query.get(key)));

        //-------------------------------------
        // Facet processing
        //-------------------------------------

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

            for (String k : rangeFields.keySet()) {
                Number rangeStart = rangeFields.get(k).get("facet.range.start");
                Number rangeEnd = rangeFields.get(k).get("facet.range.end");
                Number rangeGap = rangeFields.get(k).get("facet.range.gap");
                solrQuery.addNumericRangeFacet(k, rangeStart, rangeEnd, rangeGap);
            }
        }

//        solrQuery.setQuery(queryString.toString());
        solrQuery.setQuery("*:*");
        filterList.forEach(filter -> solrQuery.addFilterQuery(filter));
        return solrQuery;
    }

    /**
     *
     * Parse string values, e.g.: dbSNP, type, chromosome,... This function takes into account multiple values and
     * the separator between them can be:
     *     "," to apply a "OR condition"
     *     ";" to apply a "AND condition"
     *
     * @param name         Paramenter name
     * @param value        Paramenter value
     * @return             A list of strings, each string represents a boolean condition
     */
    private static List<String> parseTermValue(String name, String value) {
        List<String> filters = new ArrayList<>();

        if (value != null && !value.isEmpty()) {
            boolean or = value.contains(",");
            boolean and = value.contains(";");
            if (or && and) {
                throw new IllegalArgumentException("Command and semi-colon cannot be mixed: " + value);
            }

            String[] values = value.split("[,;]");
            StringBuilder filter = new StringBuilder();
            if (values.length == 1) {
                filter.append(name).append(":\"").append(value).append("\"");
            } else {
                String logicalComparator = or ? " OR " : " AND ";
                filter.append("(");
                filter.append(name).append(":\"").append(values[0]).append("\"");
                for (int i = 1; i < values.length; i++) {
                    filter.append(logicalComparator);
                    filter.append(name).append(":\"").append(values[i]).append("\"");
                }
                filter.append(")");
            }
            filters.add(filter.toString());
        }
        return filters;
    }

    /**
     *
     * Parse trait values, i.e.: ClinVar, COSMIC or HPO. This function takes into account multiple values and
     * an OR is applied between them.
     *
     * @param type         Paramenter type: ClinVar, COSMIC or HPO
     * @param value        Paramenter value
     * @return             A list of strings, each string represents a boolean condition
     */
    private static List<String> parseTraitValue(String type, String value) {
        List<String> filters = new ArrayList<>();

        if (value != null && !value.isEmpty()) {
            String[] values = value.split("[,;]");
            StringBuilder filter = new StringBuilder();
            if (values.length == 1) {
                filter.append("traits").append(":\"").append(type).append("*").append(value).append("*\"");
            } else {
                filter.append("(");
                filter.append("traits").append(":\"").append(type).append("*").append(values[0]).append("*\"");
                for (int i = 1; i < values.length; i++) {
                    filter.append(" OR ");
                    filter.append("traits").append(":\"").append(type).append("*").append(values[i]).append("*\"");
                }
                filter.append(")");
            }
            filters.add(filter.toString());
        }
        return filters;
    }

    /**
     * Parse string values, e.g.: polyPhen, gerp, caddRaw,... This function takes into account multiple values and
     * the separator between them can be:
     *     "," to apply a "OR condition"
     *     ";" to apply a "AND condition"
     *
     * @param name         Paramenter name
     * @param value        Paramenter value
     * @return              A list of strings, each string represents a boolean condition
     */
    private static List<String> parseScoreValue(String name, String value) {
        List<String> filters = new ArrayList<>();

        // In Solr, range queries can be inclusive or exclusive of the upper and lower bounds:
        //    - Inclusive range queries are denoted by square brackets.
        //    - Exclusive range queries are denoted by curly brackets.

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

            if (values.length == 1) {
                matcher = SCORE_PATTERN.matcher(value);
                if (matcher.find()) {
                    // concat expresion, e.g.: value:[0 TO 12]
                    sb.append(getRange("", matcher.group(1), matcher.group(2), matcher.group(3)));
                } else {
                    // error
                    throw new IllegalArgumentException("Invalid expresion " +  value);
                }
            } else {
                matcher = SCORE_PATTERN.matcher(values[0]);
                if (matcher.find()) {
                    sb.append("(");
                    // concat expresion, e.g.: value:[0 TO 12]
                    sb.append(getRange("", matcher.group(1), matcher.group(2), matcher.group(3)));
                    for (int i = 1; i < values.length; i++) {
                        matcher = SCORE_PATTERN.matcher(values[i]);
                        if (matcher.find()) {
                            sb.append(logicalComparator);
                            // concat expresion, e.g.: value:[0 TO 12]
                            sb.append(getRange("", matcher.group(1), matcher.group(2), matcher.group(3)));
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

        return filters;
    }

    /**
     * Parse population/stats values, e.g.: 1000g:all>0.4 or 1Kg_phase3:JPN<0.00982. This function takes into account
     * multiple values and the separator between them can be:
     *     "," to apply a "OR condition"
     *     ";" to apply a "AND condition"
     *
     * @param name         Paramenter type: propFreq or stats
     * @param value        Paramenter value
     * @return             A list of strings, each string represents a boolean condition
     */
    private static List<String> parsePopValue(String name, String value) {
        List<String> filters = new ArrayList<>();

        // In Solr, range queries can be inclusive or exclusive of the upper and lower bounds:
        //    - Inclusive range queries are denoted by square brackets.
        //    - Exclusive range queries are denoted by curly brackets.

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

            if (values.length == 1) {
                matcher = STUDY_PATTERN.matcher(value);
                if (matcher.find()) {
                    // concat expresion, e.g.: value:[0 TO 12]
                    sb.append(getRange(name + "__" + matcher.group(1) + "__", matcher.group(2),
                            matcher.group(3), matcher.group(4)));
                } else {
                    // error
                    throw new IllegalArgumentException("Invalid expresion " +  value);
                }
            } else {
                matcher = SCORE_PATTERN.matcher(values[0]);
                if (matcher.find()) {
                    sb.append("(");
                    // concat expresion, e.g.: value:[0 TO 12]
                    sb.append(getRange(name + "__" + matcher.group(1) + "__", matcher.group(2),
                            matcher.group(3), matcher.group(4)));
                    for (int i = 1; i < values.length; i++) {
                        matcher = SCORE_PATTERN.matcher(values[i]);
                        if (matcher.find()) {
                            sb.append(logicalComparator);
                            // concat expresion, e.g.: value:[0 TO 12]
                            sb.append(getRange(name + "__" + matcher.group(1) + "__", matcher.group(2),
                                    matcher.group(3), matcher.group(4)));
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

        return filters;
    }

    /**
     * Get the name in the SearchVariantModel from the command line parameter name.
     *
     * @param name  Command line parameter name
     * @return      Name in the model
     */
    private static String getScoreName(String name) {
        switch (name) {
            case "cadd_scaled":
            case "caddScaled":
                return "caddScaled";

            case "cadd_raw":
            case "caddRaw":
                return "caddRaw";

            default: {
                return name;
//                System.err.format("Error: invalid score name %s. Use cadd_scaled, cadd_raw, gerp,...!\n", name);
//                return null;
            }
        }
    }

    /**
     * Build Solr query range, e.g.: query range [0 TO 23}.
     *
     * @param prefix    Prefix
     * @param name      Parameter name
     * @param op        Operator
     * @param value     Parameter value
     * @return          Solr query range
     */
    private static String getRange(String prefix, String name, String op, String value) {
        StringBuilder sb = new StringBuilder();
        switch (op) {
            case "=": {
                sb.append(prefix).append(getScoreName(name)).append(":[").append(value)
                        .append(" TO ").append(value).append("]");
                break;
            }
            case ">": {
                sb.append(prefix).append(getScoreName(name)).append(":{").append(value)
                        .append(" TO *]");
                break;
            }
            case ">=": {
                sb.append(prefix).append(getScoreName(name)).append(":[").append(value)
                        .append(" TO *]");
                break;
            }
            case "<": {
                sb.append(prefix).append(getScoreName(name)).append(":[* TO ").append(value)
                        .append("}");
                break;
            }
            case "<=": {
                sb.append(prefix).append(getScoreName(name)).append(":[* TO ").append(value)
                        .append("]");
                break;
            }
            default: {
                break;
            }
        }
        return sb.toString();
    }

    private static SolrQuery.ORDER getSortOrder(QueryOptions queryOptions) {
        return queryOptions.getString(QueryOptions.ORDER).equals(QueryOptions.ASCENDING)
                ? SolrQuery.ORDER.asc : SolrQuery.ORDER.desc;
    }
}
