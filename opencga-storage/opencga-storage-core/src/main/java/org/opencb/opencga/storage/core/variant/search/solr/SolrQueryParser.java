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

package org.opencb.opencga.storage.core.variant.search.solr;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;

/**
 * Created by imedina on 18/11/16.
 * Created by jtarraga on 18/11/16.
 * Created by wasim on 18/11/16.
 */
public class SolrQueryParser {

    private final StudyConfigurationManager studyConfigurationManager;

    private static Map<String, String> includeMap;

    private static final Pattern STUDY_PATTERN = Pattern.compile("^([^=<>!]+):([^=<>!]+)(!=?|<=?|>=?|<<=?|>>=?|==?|=?)([^=<>!]+.*)$");
    private static final Pattern SCORE_PATTERN = Pattern.compile("^([^=<>!]+)(!=?|<=?|>=?|<<=?|>>=?|==?|=?)([^=<>!]+.*)$");

    protected static Logger logger = LoggerFactory.getLogger(SolrQueryParser.class);

    static {
        includeMap = new HashMap<>();

        includeMap.put("id", "id,variantId");
        includeMap.put("chromosome", "chromosome");
        includeMap.put("start", "start");
        includeMap.put("end", "end");
        includeMap.put("type", "type");

        includeMap.put("studies", "studies,stats__*");
        includeMap.put("studies.stats", "studies,stats__*");

        includeMap.put("annotation", "genes,soAcc,geneToSoAcc,biotypes,sift,siftDesc,polyphen,polyphenDesc,popFreq__*,xrefs,"
                + "phastCons,phylop,gerp,caddRaw,caddScaled,traits");
        includeMap.put("annotation.consequenceTypes", "genes,soAcc,geneToSoAcc,biotypes,sift,siftDesc,polyphen,polyphenDesc");
        includeMap.put("annotation.populationFrequencies", "popFreq__*");
        includeMap.put("annotation.xrefs", "xrefs");
        includeMap.put("annotation.conservation", "phastCons,phylop,gerp");
        includeMap.put("annotation.functionalScore", "caddRaw,caddScaled");
        includeMap.put("annotation.traitAssociation", "traits");
    }

    public SolrQueryParser(StudyConfigurationManager studyConfigurationManager) {
        this.studyConfigurationManager = studyConfigurationManager;
    }

    /**
     * Create a SolrQuery object from Query and QueryOptions.
     *
     * @param query         Query
     * @param queryOptions  Query Options
     * @return              SolrQuery
     */
    public SolrQuery parse(Query query, QueryOptions queryOptions) {
        List<String> filterList = new ArrayList<>();

        SolrQuery solrQuery = new SolrQuery();

        //-------------------------------------
        // QueryOptions processing
        //-------------------------------------
        // TODO: Use VariantField
        String[] includes = null;
        if (queryOptions.containsKey(QueryOptions.INCLUDE)) {
            includes = solrIncludeFields(queryOptions.getAsStringList(QueryOptions.INCLUDE));
        } else {
            if (queryOptions.containsKey(QueryOptions.EXCLUDE)) {
                includes = getSolrIncludeFromExclude(queryOptions.getAsStringList(QueryOptions.EXCLUDE));
            } else {
                includes = getSolrIncludeFromExclude(Collections.emptyList());
            }
        }
        includes = ArrayUtils.removeAllOccurences(includes, "release");
        includes = includeFieldsWithMandatory(includes);

        solrQuery.setFields(includes);

        if (queryOptions.containsKey(QueryOptions.LIMIT)) {
            solrQuery.setRows(queryOptions.getInt(QueryOptions.LIMIT));
        }

        if (queryOptions.containsKey(QueryOptions.SKIP)) {
            solrQuery.setStart(queryOptions.getInt(QueryOptions.SKIP));
        }

        if (queryOptions.containsKey(QueryOptions.SORT)) {
            solrQuery.addSort(queryOptions.getString(QueryOptions.SORT), getSortOrder(queryOptions));
        }

        //-------------------------------------
        // Facet processing
        //-------------------------------------

        // facet fields (query parameter: facet)
        // multiple faceted fields are separated by ";", they can be:
        //    - non-nested faceted fields, e.g.: biotype
        //    - nested faceted fields (i.e., Solr pivots) are separated by ">>", e.g.: studies>>type
        //    - ranges, field_name:start:end:gap, e.g.: sift:0:1:0.5
        //    - intersections, field_name:value1^value2[^value3], e.g.: studies:1kG^ESP
        if (queryOptions.containsKey(QueryOptions.FACET) && StringUtils.isNotEmpty(queryOptions.getString(QueryOptions.FACET))) {
            parseSolrFacets(queryOptions.get(QueryOptions.FACET).toString(), solrQuery);
        }

        // facet ranges,
        // query parameter name: facetRange
        // multiple facet ranges are separated by ";"
        // query parameter value: field:start:end:gap, e.g.: sift:0:1:0.5
        if (queryOptions.containsKey(QueryOptions.FACET_RANGE)
                && StringUtils.isNotEmpty(queryOptions.getString(QueryOptions.FACET_RANGE))) {
            parseSolrFacetRanges(queryOptions.get(QueryOptions.FACET_RANGE).toString(), solrQuery);
        }

        // facet intersections,
        //if (queryOptions.containsKey(QueryOptions.FACET_INTERSECTION)) {
        //    parseSolrFacetIntersections(queryOptions.get(QueryOptions.FACET_INTERSECTION).toString(), solrQuery);
        //}

        //-------------------------------------
        // Query processing
        //-------------------------------------

        // OR conditions
        // create a list for xrefs (without genes), genes, regions and cts
        // the function classifyIds function differentiates xrefs from genes
        List<String> xrefs = new ArrayList<>();
        List<String> genes = new ArrayList<>();
        List<Region> regions = new ArrayList<>();
        List<String> consequenceTypes = new ArrayList<>();

        // xref
        classifyIds(VariantQueryParam.ANNOT_XREF.key(), query, xrefs, genes);
        classifyIds(VariantQueryParam.ID.key(), query, xrefs, genes);
        classifyIds(VariantQueryParam.GENE.key(), query, xrefs, genes);
        classifyIds(VariantQueryParam.ANNOT_CLINVAR.key(), query, xrefs, genes);
        classifyIds(VariantQueryParam.ANNOT_COSMIC.key(), query, xrefs, genes);
//        classifyIds(VariantQueryParams.ANNOT_HPO.key(), query, xrefs, genes);

        // Convert region string to region objects
        if (query.containsKey(VariantQueryParam.REGION.key())) {
            regions = Region.parseRegions(query.getString(VariantQueryParam.REGION.key()));
        }

        // consequence types (cts)
        if (query.containsKey(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key())
                && StringUtils.isNotEmpty(query.getString(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key()))) {
            consequenceTypes = Arrays.asList(query.getString(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key()).split("[,;]"));
        }

        // goal: [((xrefs OR regions) AND cts) OR (genes AND cts)] AND ... AND ...
        if (CollectionUtils.isNotEmpty(consequenceTypes)) {
            if (CollectionUtils.isNotEmpty(genes)) {
                // consequence types and genes
                String or = buildXrefOrRegionAndConsequenceType(xrefs, regions, consequenceTypes);
                if (xrefs.isEmpty() && regions.isEmpty()) {
                    // no xrefs or regions: genes AND cts
                    filterList.add(buildGeneAndCt(genes, consequenceTypes));
                } else {
                    // otherwise: [((xrefs OR regions) AND cts) OR (genes AND cts)]
                    filterList.add("(" + or + ") OR (" + buildGeneAndCt(genes, consequenceTypes) + ")");
                }
            } else {
                // consequence types but no genes: (xrefs OR regions) AND cts
                // in this case, the resulting string will never be null, because there are some consequence types!!
                filterList.add(buildXrefOrRegionAndConsequenceType(xrefs, regions, consequenceTypes));
            }
        } else {
            // no consequence types: (xrefs OR regions) but we must add "OR genes", i.e.: xrefs OR regions OR genes
            // no consequence types: (xrefs OR regions) but we must add "OR genMINes", i.e.: xrefs OR regions OR genes
            // we must make an OR with xrefs, genes and regions and add it to the "AND" filter list
            String orXrefs = buildXrefOrGeneOrRegion(xrefs, genes, regions);
            if (!orXrefs.isEmpty()) {
                filterList.add(orXrefs);
            }
        }

        // now we continue with the other AND conditions...
        // type (t)
        String key = VariantQueryParam.STUDY.key();
        if (isValidParam(query, VariantQueryParam.STUDY)) {
            try {
                String value = query.getString(key);
                VariantQueryUtils.QueryOperation op = checkOperator(value);
                Set<Integer> studyIds = new HashSet<>(studyConfigurationManager.getStudyIds(splitValue(value, op), queryOptions));
                List<String> studyNames = new ArrayList<>(studyIds.size());
                Map<String, Integer> map = studyConfigurationManager.getStudies(null);
                if (map != null && map.size() > 1) {
                    map.forEach((name, id) -> {
                        if (studyIds.contains(id)) {
                            String[] s = name.split(":");
                            studyNames.add(s[s.length - 1]);
                        }
                    });

                    if (op == null || op == VariantQueryUtils.QueryOperation.OR) {
                        filterList.add(parseCategoryTermValue("studies", StringUtils.join(studyNames, ",")));
                    } else {
                        filterList.add(parseCategoryTermValue("studies", StringUtils.join(studyNames, ";")));
                    }
                }
            } catch (NullPointerException e) {
                logger.error(e.getMessage());
                e.printStackTrace();
            }
        }

        // type (t)
        key = VariantQueryParam.TYPE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parseCategoryTermValue("type", query.getString(key)));
        }

        // Gene biotype
        key = VariantQueryParam.ANNOT_BIOTYPE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parseCategoryTermValue("biotypes", query.getString(key)));
        }

        // protein-substitution
        key = VariantQueryParam.ANNOT_PROTEIN_SUBSTITUTION.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parseScoreValue(query.getString(key)));
        }

        // conservation
        key = VariantQueryParam.ANNOT_CONSERVATION.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parseScoreValue(query.getString(key)));
        }

        // cadd, functional score
        key = VariantQueryParam.ANNOT_FUNCTIONAL_SCORE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parseScoreValue(query.getString(key)));
        }

        // ALT population frequency
        // in the query: 1kG_phase3:CEU<=0.0053191,1kG_phase3:CLM>0.0125319"
        // in the search model: "popFreq__1kG_phase3__CEU":0.0053191,popFreq__1kG_phase3__CLM">0.0125319"
        key = VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parsePopFreqValue("popFreq", query.getString(key), "ALT"));
        }

        // MAF population frequency
        // in the search model: "popFreq__1kG_phase3__CLM":0.005319148767739534
        key = VariantQueryParam.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parsePopFreqValue("popFreq", query.getString(key), "MAF"));
        }

        // REF population frequency
        // in the search model: "popFreq__1kG_phase3__CLM":0.005319148767739534
        key = VariantQueryParam.ANNOT_POPULATION_REFERENCE_FREQUENCY.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parsePopFreqValue("popFreq", query.getString(key), "REF"));
        }

        // stats maf
        // in the model: "stats__1kg_phase3__ALL"=0.02
        key = VariantQueryParam.STATS_MAF.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parsePopFreqValue("stats", query.getString(key), "MAF"));
        }

        // GO
        key = ANNOT_GO_GENES.key();
        if (isValidParam(query, ANNOT_GO_GENES)) {
            List<String> genesByGo = query.getAsStringList(key);
            if (CollectionUtils.isNotEmpty(genesByGo)) {
                filterList.add(parseCategoryTermValue("xrefs", StringUtils.join(genesByGo, ",")));
            }
        }

        // EXPRESSION
        key = ANNOT_EXPRESSION_GENES.key();
        if (isValidParam(query, ANNOT_EXPRESSION_GENES)) {
            List<String> genesByExpression = query.getAsStringList(key);
            if (CollectionUtils.isNotEmpty(genesByExpression)) {
                filterList.add(parseCategoryTermValue("xrefs", StringUtils.join(genesByExpression, ",")));
            }
        }

        // Gene Trait IDs
        key = VariantQueryParam.ANNOT_GENE_TRAIT_ID.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parseCategoryTermValue("traits", query.getString(key)));
        }

        // hpo
        key = VariantQueryParam.ANNOT_HPO.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parseCategoryTermValue("traits", query.getString(key)));
        }

        // clinvar
//        key = VariantQueryParam.ANNOT_CLINVAR.key();
//        if (StringUtils.isNotEmpty(query.getString(key))) {
//            filterList.add(parseCategoryTermValue("traits", query.getString(key)));
//        }

        // traits
        key = VariantQueryParam.ANNOT_TRAIT.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parseCategoryTermValue("traits", query.getString(key)));
        }

        logger.debug("query = {}\n", query.toJson());

        solrQuery.setQuery("*:*");
        filterList.forEach(filter -> {
            solrQuery.addFilterQuery(filter);
            logger.debug("Solr fq: {}\n", filter);
        });

        return solrQuery;
    }

    /**
     * Check if the target xref is a gene.
     *
     * @param xref    Target xref
     * @return        True or false
     */
    private boolean isGene(String xref) {
        // TODO: this function must be completed
        if (xref.isEmpty()) {
            return false;
        }
        if (xref.indexOf(":") == -1) {
            return true;
        }
        return true;
    }

    /**
     * Insert the IDs for this key in the query into the xref or gene list depending on they are or not genes.
     *
     * @param key     Key in the query
     * @param query   Query
     * @param xrefs   List to insert the xrefs (no genes)
     * @param genes   List to insert the genes
     */
    private void classifyIds(String key, Query query, List<String> xrefs, List<String> genes) {
        String value;
        if (query.containsKey(key)) {
            value = query.getString(key);
            if (StringUtils.isNotEmpty(value)) {
                List<String> items = Arrays.asList(value.split("[,;]"));
                for (String item: items) {
                    if (isGene(item)) {
                        genes.add(item);
                    } else {
                        xrefs.add(item);
                    }
                }
            }
        }
    }

    /**
     * Parse string values, e.g.: dbSNP, type, chromosome,... This function takes into account multiple values and
     * the separator between them can be:
     *     "," or ";" to apply a "OR" condition
     *
     * @param name          Parameter name
     * @param value         Parameter value
     * @return             A list of strings, each string represents a boolean condition
     */
    public String parseCategoryTermValue(String name, String value) {
        return parseCategoryTermValue(name, value, false);
    }

    /**
     * Parse string values, e.g.: dbSNP, type, chromosome,... This function takes into account multiple values and
     * the separator between them can be:
     *     "," or ";" to apply a "OR" condition
     *
     * @param name          Parameter name
     * @param value         Parameter value
     * @param partialSearch Flag to partial search
     * @return             A list of strings, each string represents a boolean condition
     */
    public String parseCategoryTermValue(String name, String value, boolean partialSearch) {
        StringBuilder filter = new StringBuilder();
        if (StringUtils.isNotEmpty(value)) {
            boolean or = value.contains(",");
            boolean and = value.contains(";");
            if (or && and) {
                throw new IllegalArgumentException("Command and semi-colon cannot be mixed: " + value);
            }
            String logicalComparator = or ? " OR " : " AND ";
            String wildcard = partialSearch ? "*" : "";

            String[] values = value.split("[,;]");
            if (values.length == 1) {
                filter.append(name).append(":\"").append(wildcard).append(value).append(wildcard).append("\"");
            } else {
                filter.append("(");
                filter.append(name).append(":\"").append(wildcard).append(values[0]).append(wildcard).append("\"");
                for (int i = 1; i < values.length; i++) {
                    filter.append(logicalComparator);
                    filter.append(name).append(":\"").append(wildcard).append(values[i]).append(wildcard).append("\"");
                }
                filter.append(")");
            }
        }
        return filter.toString();
    }

    /**
     * Parse string values, e.g.: polyPhen, gerp, caddRaw,... This function takes into account multiple values and
     * the separator between them can be:
     *     "," to apply a "OR condition"
     *     ";" to apply a "AND condition"
     *
     * @param value        Parameter value
     * @return             The string with the boolean conditions
     */
    public String parseScoreValue(String value) {
        // In Solr, range queries can be inclusive or exclusive of the upper and lower bounds:
        //    - Inclusive range queries are denoted by square brackets.
        //    - Exclusive range queries are denoted by curly brackets.
        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotEmpty(value)) {
            boolean or = value.contains(",");
            boolean and = value.contains(";");
            if (or && and) {
                throw new IllegalArgumentException("Command and semi-colon cannot be mixed: " + value);
            }
            String logicalComparator = or ? " OR " : " AND ";

            Matcher matcher;
            String[] values = value.split("[,;]");
            if (values.length == 1) {
                matcher = SCORE_PATTERN.matcher(value);
                if (matcher.find()) {
                    // concat expression, e.g.: value:[0 TO 12]
                    sb.append(getRange("", matcher.group(1), matcher.group(2), matcher.group(3)));
                } else {
                    logger.debug("Invalid expression: {}", value);
                    throw new IllegalArgumentException("Invalid expression " +  value);
                }
            } else {
                List<String> list = new ArrayList<>(values.length);
                for (String v : values) {
                    matcher = SCORE_PATTERN.matcher(v);
                    if (matcher.find()) {
                        // concat expression, e.g.: value:[0 TO 12]
                        list.add(getRange("", matcher.group(1), matcher.group(2), matcher.group(3)));
                    } else {
                        throw new IllegalArgumentException("Invalid expression " +  value);
                    }
                }
                sb.append("(").append(StringUtils.join(list, logicalComparator)).append(")");
            }
        }
        return sb.toString();
    }

    /**
     * Parse population/stats values, e.g.: 1000g:all>0.4 or 1Kg_phase3:JPN<0.00982. This function takes into account
     * multiple values and the separator between them can be:
     *     "," to apply a "OR condition"
     *     ";" to apply a "AND condition"
     *
     * @param name         Paramenter type: propFreq or stats
     * @param value        Paramenter value
     * @return             The string with the boolean conditions
     */
    private String parsePopFreqValue(String name, String value, String type) {
        // In Solr, range queries can be inclusive or exclusive of the upper and lower bounds:
        //    - Inclusive range queries are denoted by square brackets.
        //    - Exclusive range queries are denoted by curly brackets.
        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotEmpty(value)) {
            // FIXME at the higher level
            value = value.replace("<<", "<");
            value = value.replace("<", "<<");

            boolean or = value.contains(",");
            boolean and = value.contains(";");
            if (or && and) {
                throw new IllegalArgumentException("Command and semi-colon cannot be mixed: " + value);
            }
            String logicalComparator = or ? " OR " : " AND ";

            Matcher matcher;
            String[] values = value.split("[,;]");
            if (values.length == 1) {
                matcher = STUDY_PATTERN.matcher(value);
                if (matcher.find()) {
                    // Solr only stores ALT frequency, we need to calculate the MAF or REF before querying
                    String[] freqValue = getMafOrRefFrequency(type, matcher.group(3), matcher.group(4));

                    // concat expression, e.g.: value:[0 TO 12]
                    sb.append(getRange(name + "__" + matcher.group(1) + "__", matcher.group(2), freqValue[0], freqValue[1]));
                } else {
                    // error
                    throw new IllegalArgumentException("Invalid expression " +  value);
                }
            } else {
                List<String> list = new ArrayList<>(values.length);
                for (String v : values) {
                    matcher = STUDY_PATTERN.matcher(v);
                    if (matcher.find()) {
                        // Solr only stores ALT frequency, we need to calculate the MAF or REF before querying
                        String[] freqValue = getMafOrRefFrequency(type, matcher.group(3), matcher.group(4));

                        // concat expression, e.g.: value:[0 TO 12]
                        list.add(getRange(name + "__" + matcher.group(1) + "__", matcher.group(2), freqValue[0], freqValue[1]));
                    } else {
                        throw new IllegalArgumentException("Invalid expression " +  value);
                    }
                }
                sb.append("(").append(StringUtils.join(list, logicalComparator)).append(")");
            }
        }
        return sb.toString();
    }

    private String[] getMafOrRefFrequency(String type, String operator, String value) {
        String[] opValue = new String[2];
        opValue[0] = operator;
        switch (type.toUpperCase()) {
            case "MAF":
                double d = Double.parseDouble(value);
                if (d > 0.5) {
                    d = 1 - d;

                    if (operator.contains("<")) {
                        opValue[0] = operator.replaceAll("<", ">");
                    } else {
                        if (operator.contains(">")) {
                            opValue[0] = operator.replaceAll(">", "<");
                        }
                    }
                }

                opValue[1] = String.valueOf(d);
                break;
            case "REF":
                if (operator.contains("<")) {
                    opValue[0] = operator.replaceAll("<", ">");
                } else {
                    if (operator.contains(">")) {
                        opValue[0] = operator.replaceAll(">", "<");
                    }
                }
                opValue[1] = String.valueOf(1 - Double.parseDouble(value));
                break;
            case "ALT":
            default:
                opValue[1] = value;
                break;
        }
        return opValue;
    }

    /**
     * Get the name in the SearchVariantModel from the command line parameter name.
     *
     * @param name  Command line parameter name
     * @return      Name in the model
     */
    private String getSolrFieldName(String name) {
        switch (name) {
            case "cadd_scaled":
            case "caddScaled":
                return "caddScaled";
            case "cadd_raw":
            case "caddRaw":
                return "caddRaw";
            default:
                return name;
        }
    }

    /**
     * Build Solr query range, e.g.: query range [0 TO 23}.
     *
     * @param prefix    Prefix, e.g.: popFreq__study__cohort, stats__ or null
     * @param name      Parameter name, e.g.: sift, phylop, gerp, caddRaw,...
     * @param op        Operator, e.g.: =, !=, <, <=, <<, <<=, >,...
     * @param value     Parameter value, e.g.: 0.314, tolerated,...
     * @return          Solr query range
     */
    public String getRange(String prefix, String name, String op, String value) {
        StringBuilder sb = new StringBuilder();
        switch (op) {
            case "=":
            case "==":
                try {
                    Double v = Double.parseDouble(value);
                    // attention: negative values must be escaped
                    sb.append(prefix).append(getSolrFieldName(name)).append(":").append(v < 0 ? "\\" : "").append(value);
                } catch (NumberFormatException e) {
                    switch (name.toLowerCase()) {
                        case "sift":
                            sb.append(prefix).append("siftDesc").append(":\"").append(value).append("\"");
                            break;
                        case "polyphen":
                            sb.append(prefix).append("polyphenDesc").append(":\"").append(value).append("\"");
                            break;
                        default:
                            sb.append(prefix).append(getSolrFieldName(name)).append(":\"").append(value).append("\"");
                            break;
                    }
                }
                break;
            case "!=":
                switch (name.toLowerCase()) {
                    case "sift": {
                        try {
                            Double v = Double.parseDouble(value);
                            // attention: negative values must be escaped
                            sb.append("-").append(prefix).append("sift").append(":").append(v < 0 ? "\\" : "").append(v);
                        } catch (NumberFormatException e) {
                            sb.append("-").append(prefix).append("siftDesc").append(":\"").append(value).append("\"");
                        }
                        break;
                    }
                    case "polyphen": {
                        try {
                            Double v = Double.parseDouble(value);
                            // attention: negative values must be escaped
                            sb.append("-").append(prefix).append("polyphen").append(":").append(v < 0 ? "\\" : "").append(v);
                        } catch (NumberFormatException e) {
                            sb.append("-").append(prefix).append("polyphenDesc").append(":\"").append(value).append("\"");
                        }
                        break;
                    }
                    default: {
                        sb.append("-").append(prefix).append(getSolrFieldName(name)).append(":").append(value);
                    }
                }
                break;

            case "<":
                sb.append(prefix).append(getSolrFieldName(name)).append(":{")
                        .append(VariantSearchToVariantConverter.MISSING_VALUE).append(" TO ").append(value).append("}");
                break;
            case "<=":
                sb.append(prefix).append(getSolrFieldName(name)).append(":{")
                        .append(VariantSearchToVariantConverter.MISSING_VALUE).append(" TO ").append(value).append("]");
                break;
            case ">":
                sb.append(prefix).append(getSolrFieldName(name)).append(":{").append(value).append(" TO *]");
                break;
            case ">=":
                sb.append(prefix).append(getSolrFieldName(name)).append(":[").append(value).append(" TO *]");
                break;

            case "<<":
            case "<<=":
                String rightCloseOperator = ("<<").equals(op) ? "}" : "]";
                if (StringUtils.isNotEmpty(prefix) && (prefix.startsWith("popFreq_") || prefix.startsWith("stats_"))) {
                    sb.append("(");
                    sb.append("(* -").append(prefix).append(getSolrFieldName(name)).append(":*)");
                    sb.append(" OR ");
                    sb.append(prefix).append(getSolrFieldName(name)).append(":[0 TO ").append(value).append(rightCloseOperator);
                    sb.append(")");
                } else {
                    sb.append(prefix).append(getSolrFieldName(name)).append(":[")
                            .append(VariantSearchToVariantConverter.MISSING_VALUE).append(" TO ").append(value).append(rightCloseOperator);
                }
                break;
            case ">>":
            case ">>=":
                String leftCloseOperator = (">>").equals(op) ? "{" : "[";
                sb.append("(");
                if (StringUtils.isNotEmpty(prefix) && (prefix.startsWith("popFreq_") || prefix.startsWith("stats_"))) {
                    sb.append(prefix).append(getSolrFieldName(name)).append(":").append(leftCloseOperator).append(value).append(" TO *]");
                    sb.append(" OR ");
                    sb.append("(* -").append(prefix).append(getSolrFieldName(name)).append(":*)");
                } else {
                    // attention: negative values must be escaped
                    sb.append(prefix).append(getSolrFieldName(name)).append(":").append(leftCloseOperator).append(value).append(" TO *]");
                    sb.append(" OR ");
                    sb.append(prefix).append(getSolrFieldName(name)).append(":\\").append(VariantSearchToVariantConverter.MISSING_VALUE);
                }
                sb.append(")");
                break;
            default:
                logger.debug("Unknown operator {}", op);
                break;
        }
        return sb.toString();
    }

    public SolrQuery.ORDER getSortOrder(QueryOptions queryOptions) {
        return queryOptions.getString(QueryOptions.ORDER).equals(QueryOptions.ASCENDING)
                ? SolrQuery.ORDER.asc : SolrQuery.ORDER.desc;
    }

    /**
     * Build an OR-condition with all xrefs, genes and regions.
     *
     * @param xrefs     List of xrefs
     * @param genes     List of genes
     * @param regions   List of regions
     * @return          OR-condition string
     */
    private String buildXrefOrGeneOrRegion(List<String> xrefs, List<String> genes, List<Region> regions) {
        StringBuilder sb = new StringBuilder();

        // first, concatenate xrefs and genes in single list
        List<String> ids = new ArrayList<>();
        if (xrefs != null && CollectionUtils.isNotEmpty(xrefs)) {
            ids.addAll(xrefs);
        }
        if (genes != null && CollectionUtils.isNotEmpty(genes)) {
            ids.addAll(genes);
        }
        if (CollectionUtils.isNotEmpty(ids)) {
            for (String id : ids) {
                if (sb.length() > 0) {
                    sb.append(" OR ");
                }
                sb.append("xrefs:\"").append(id).append("\"");
            }
        }

        // and now regions
        for (Region region: regions) {
            if (StringUtils.isNotEmpty(region.getChromosome())) {
                if (sb.length() > 0) {
                    sb.append(" OR ");
                }
                sb.append("(");
                if (region.getStart() == 0 && region.getEnd() == Integer.MAX_VALUE) {
                    sb.append("chromosome:").append(region.getChromosome());
                } else if (region.getEnd() == Integer.MAX_VALUE) {
                    sb.append("chromosome:").append(region.getChromosome())
                            .append(" AND start:").append(region.getStart());
                } else {
                    sb.append("chromosome:").append(region.getChromosome())
                            .append(" AND start:[").append(region.getStart()).append(" TO *]")
                            .append(" AND end:[* TO ").append(region.getEnd()).append("]");
                }
                sb.append(")");
            }
        }

        return sb.toString();
    }

    /**
     * Build an OR-condition with all consequence types from the input list. It uses the VariantDBAdaptorUtils
     * to parse the consequence type (accession or term) into an integer.
     *
     * @param cts    List of consequence types
     * @return       OR-condition string
     */
    private String buildConsequenceTypeOr(List<String> cts) {
        StringBuilder sb = new StringBuilder();
        for (String ct : cts) {
            if (sb.length() > 0) {
                sb.append(" OR ");
            }
            sb.append("soAcc:").append(VariantQueryUtils.parseConsequenceType(ct));
        }
        return sb.toString();
    }

    /**
     * Build the condition: (xrefs OR regions) AND cts.
     *
     * @param xrefs      List of xrefs
     * @param regions    List of regions
     * @param cts        List of consequence types
     * @return           OR/AND condition string
     */
    private String buildXrefOrRegionAndConsequenceType(List<String> xrefs, List<Region> regions, List<String> cts) {
        String orCts = buildConsequenceTypeOr(cts);
        if (xrefs.isEmpty() && regions.isEmpty()) {
            // consequences type but no xrefs, no genes, no regions
            // we must make an OR with all consequences types and add it to the "AND" filter list
            return orCts;
        } else {
            String orXrefs = buildXrefOrGeneOrRegion(xrefs, null, regions);
            return "(" +  orXrefs + ") AND (" + orCts + ")";
        }
    }

    /**
     * Build the condition: genes AND cts.
     *
     * @param genes    List of genes
     * @param cts      List of consequence types
     * @return         OR/AND condition string
     */
    private String buildGeneAndCt(List<String> genes, List<String> cts) {
        // in the VariantSearchModel the (gene AND ct) is modeled in the field: geneToSoAcc:gene_ct
        // and if there are multiple genes and consequence types, we have to build the combination of all of them in a OR expression
        StringBuilder sb = new StringBuilder();
        for (String gene: genes) {
            for (String ct: cts) {
                if (sb.length() > 0) {
                    sb.append(" OR ");
                }
                sb.append("geneToSoAcc:").append(gene).append("_").append(VariantQueryUtils.parseConsequenceType(ct));
            }
        }
        return sb.toString();
    }

    /**
     * Parse facets.
     * Multiple facets are separated by semicolons (;)
     * E.g.:  chromosome[1,2,3,4,5];studies[1kg,exac]>>type[snv,indel];sift:0:1:0.2;gerp:-1:3:0.5;studies:1kG_phase3^EXAC^ESP6500
     *
     * @param strFields   String containing the facet definitions
     * @param solrQuery   Solr query
     */
    public void parseSolrFacets(String strFields, SolrQuery solrQuery) {
        if (StringUtils.isNotEmpty(strFields) && solrQuery != null) {
            String[] fields = strFields.split("[;]");
            for (String field: fields) {
                if (field.contains("^")) {
                    // intersections
                    parseSolrFacetIntersections(field, solrQuery);
                } else if (field.contains(":")) {
                    // ranges
                    parseSolrFacetRanges(field, solrQuery);
                } else {
                    // fields (simple or nested)
                    parseSolrFacetFields(field, solrQuery);
                }
            }
        }
    }

    /**
     * Parse Solr facet fields.
     * This format is: field_name[field_values_1,field_values_2...]:skip:limit
     *
     * @param field   String containing the facet field
     * @param solrQuery   Solr query
     */
    private void parseSolrFacetFields(String field, SolrQuery solrQuery) {
        String[] splits = field.split(">>");
        if (splits.length == 1) {
            // Solr field
            //solrQuery.addFacetField(field);
            parseFacetField(field, solrQuery, false);
        } else {
            // Solr pivots (nested fields)
            StringBuilder sb = new StringBuilder();
            for (String split: splits) {
                String name = parseFacetField(split, solrQuery, true);
                if (sb.length() > 0) {
                    sb.append(",");
                }
                sb.append(name);
            }
            solrQuery.addFacetPivotField(sb.toString());
        }
    }

    /**
     * Parse field string.
     * The expected format is: field_name[field_value_1,field_value_2,...]:skip:limit.
     *
     * @param field    The string to parse
     * @retrun         The field name
     */
    private String parseFacetField(String field, SolrQuery solrQuery, boolean pivot) {
        String name = "";
        String[] splits1 = field.split("[\\[\\]]");
        if (splits1.length == 1) {
            String[] splits2 = field.split(":");
            if (splits2.length >= 1) {
                name = splits2[0];
                if (!pivot) {
                    solrQuery.addFacetField(name);
                }
            }
            if (splits2.length >= 2 && StringUtils.isNotEmpty(splits2[1])) {
                solrQuery.set("f." + name + ".facet.offset", splits2[1]);
            }
            if (splits2.length >= 3 && StringUtils.isNotEmpty(splits2[2])) {
                solrQuery.set("f." + name + ".facet.limit", splits2[2]);
            }
        } else {
            // first, field name
            name = splits1[0];
            if (!pivot) {
                solrQuery.addFacetField(name);
            }

            // second, includes
            // nothing to do, if includes, the other ones will be removed later

            // third, skip and limit
            if (splits1.length >= 3) {
                String[] splits2 = splits1[2].split(":");
                if (splits2.length >= 2 && StringUtils.isNotEmpty(splits2[1])) {
                    solrQuery.set("f." + name + ".facet.offset", splits2[1]);
                }
                if (splits2.length >= 3 && StringUtils.isNotEmpty(splits2[2])) {
                    solrQuery.set("f." + name + ".facet.limit", splits2[2]);
                }
            }
        }
        return name;
    }

    /**
     * Parse Solr facet range.
     * This format is: field_name:start:end:gap, e.g.: sift:0:1:0.2
     *
     * @param range   String containing the facet range definition
     * @param solrQuery   Solr query
     */
    public void parseSolrFacetRanges(String range, SolrQuery solrQuery) {
        String[] split = range.split(":");
        if (split.length != 4) {
            logger.warn("Facet range '" + range + "' malformed. The expected range format is 'name:start:end:gap'");
        } else {
            try {
                Number start, end, gap;
                if (("start").equals(split[0])) {
                    start = Integer.parseInt(split[1]);
                    end = Integer.parseInt(split[2]);
                    gap = Integer.parseInt(split[3]);
                } else {
                    start = Double.parseDouble(split[1]);
                    end = Double.parseDouble(split[2]);
                    gap = Double.parseDouble(split[3]);
                }
                // Solr ranges
                solrQuery.addNumericRangeFacet(split[0], start, end, gap);
            } catch (NumberFormatException e) {
                logger.warn("Facet range '" + range + "' malformed. Range format is 'name:start:end:gap'"
                        + " where start, end and gap values are numbers.");
            }
        }
    }

    /**
     * Parse Solr facet intersection.
     *
     * @param intersection   String containing the facet intersection
     * @param solrQuery   Solr query
     */
    public void parseSolrFacetIntersections(String intersection, SolrQuery solrQuery) {
        boolean error = true;
        String[] splitA = intersection.split(":");
        if (splitA.length == 2) {
            String[] splitB = splitA[1].split("\\^");
            if (splitB.length == 2) {
                error = false;
                solrQuery.addFacetQuery("{!key=" + splitB[0] + "}" + splitA[0] + ":" + splitB[0]);
                solrQuery.addFacetQuery("{!key=" + splitB[1] + "}" + splitA[0] + ":" + splitB[1]);
                solrQuery.addFacetQuery("{!key=" + splitB[0] + "__" + splitB[1] + "}" + splitA[0] + ":" + splitB[0]
                        + " AND " + splitA[0] + ":" + splitB[1]);

            } else if (splitB.length == 3) {
                error = false;
                solrQuery.addFacetQuery("{!key=" + splitB[0] + "}" + splitA[0] + ":" + splitB[0]);
                solrQuery.addFacetQuery("{!key=" + splitB[1] + "}" + splitA[0] + ":" + splitB[1]);
                solrQuery.addFacetQuery("{!key=" + splitB[2] + "}" + splitA[0] + ":" + splitB[2]);
                solrQuery.addFacetQuery("{!key=" + splitB[0] + "__" + splitB[1] + "}" + splitA[0] + ":" + splitB[0]
                        + " AND " + splitA[0] + ":" + splitB[1]);
                solrQuery.addFacetQuery("{!key=" + splitB[0] + "__" + splitB[2] + "}" + splitA[0] + ":" + splitB[0]
                        + " AND " + splitA[0] + ":" + splitB[2]);
                solrQuery.addFacetQuery("{!key=" + splitB[1] + "__" + splitB[2] + "}" + splitA[0] + ":" + splitB[1]
                        + " AND " + splitA[0] + ":" + splitB[2]);
                solrQuery.addFacetQuery("{!key=" + splitB[0] + "__" + splitB[1] + "__" + splitB[2] + "}" + splitA[0]
                        + ":" + splitB[0] + " AND " + splitA[0]
                        + ":" + splitB[1] + " AND " + splitA[0] + ":" + splitB[2]);
            }
        }

        if (error) {
            logger.warn("Facet intersection '" + intersection + "' malformed. The expected intersection format"
                    + " is 'name:value1^value2[^value3]', value3 is optional");
        }
    }

    private String[] solrIncludeFields(List<String> includes) {
        if (includes == null) {
            return new String[0];
        }

        List<String> solrIncludeList = new ArrayList<>();
        // The values of the includeMap can contain commas
        for (String include : includes) {
            if (includeMap.containsKey(include)) {
                solrIncludeList.add(includeMap.get(include));
            }
        }
        return StringUtils.join(solrIncludeList, ",").split(",");
    }

    private String[] getSolrIncludeFromExclude(List<String> excludes) {
        Set<String> solrFieldsToInclude = new HashSet<>(20);
        for (String value : includeMap.values()) {
            solrFieldsToInclude.addAll(Arrays.asList(value.split(",")));
        }

        if (excludes != null) {
            for (String exclude : excludes) {
                List<String> solrFields = Arrays.asList(includeMap.get(exclude).split(","));
                solrFieldsToInclude.removeAll(solrFields);
            }
        }

        List<String> solrFieldsToIncludeList = new ArrayList<>(solrFieldsToInclude);
        String[] solrFieldsToIncludeArr = new String[solrFieldsToIncludeList.size()];
        for (int i = 0; i < solrFieldsToIncludeList.size(); i++) {
            solrFieldsToIncludeArr[i] = solrFieldsToIncludeList.get(i);
        }

        return solrFieldsToIncludeArr;
    }

    private String[] includeFieldsWithMandatory(String[] includes) {
        if (includes == null || includes.length == 0) {
            return new String[0];
        }

        String[] mandatoryIncludeFields  = new String[]{"id", "chromosome", "start", "end", "type"};
        String[] includeWithMandatory = new String[includes.length + mandatoryIncludeFields.length];
        for (int i = 0; i < includes.length; i++) {
            includeWithMandatory[i] = includes[i];
        }
        for (int i = 0; i < mandatoryIncludeFields.length; i++) {
            includeWithMandatory[includes.length + i] = mandatoryIncludeFields[i];
        }
        return includeWithMandatory;
    }
 }
