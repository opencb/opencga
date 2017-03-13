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
import org.opencb.opencga.storage.core.search.VariantSearchToVariantConverter;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by wasim on 18/11/16.
 */
public class ParseSolrQuery {

    private static final Pattern STUDY_PATTERN = Pattern.compile("^([^=<>!]+):([^=<>!]+)(!=?|<=?|>=?|<<=?|>>=?|==?|=?)([^=<>!]+.*)$");
    private static final Pattern SCORE_PATTERN = Pattern.compile("^([^=<>!]+)(!=?|<=?|>=?|<<=?|>>=?|==?|=?)([^=<>!]+.*)$");

    protected static Logger logger = LoggerFactory.getLogger(ParseSolrQuery.class);

    /**
     * Create a SolrQuery object from Query and QueryOptions.
     *
     * @param query         Query
     * @param queryOptions  Query Options
     * @return              SolrQuery
     */
    public static SolrQuery parse(Query query, QueryOptions queryOptions) {
        List<String> filterList = new ArrayList<>();

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

        if (queryOptions.containsKey(QueryOptions.SKIP)) {
            solrQuery.setStart(queryOptions.getInt(QueryOptions.SKIP));
        }

        if (queryOptions.containsKey(QueryOptions.SORT)) {
            solrQuery.addSort(queryOptions.getString(QueryOptions.SORT), getSortOrder(queryOptions));
        }

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
        classifyIds(VariantDBAdaptor.VariantQueryParams.ANNOT_XREF.key(), query, xrefs, genes);
        classifyIds(VariantDBAdaptor.VariantQueryParams.ID.key(), query, xrefs, genes);
        classifyIds(VariantDBAdaptor.VariantQueryParams.GENE.key(), query, xrefs, genes);
        classifyIds(VariantDBAdaptor.VariantQueryParams.ANNOT_CLINVAR.key(), query, xrefs, genes);
        classifyIds(VariantDBAdaptor.VariantQueryParams.ANNOT_COSMIC.key(), query, xrefs, genes);
        classifyIds(VariantDBAdaptor.VariantQueryParams.ANNOT_HPO.key(), query, xrefs, genes);

        // Parse regions
        if (query.containsKey(VariantDBAdaptor.VariantQueryParams.REGION.key())) {
            regions = Region.parseRegions(query.getString(VariantDBAdaptor.VariantQueryParams.REGION.key()));
        }

        // consequence types (cts)
        if (query.containsKey(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key())
                && StringUtils.isNotEmpty(query.getString(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key()))) {
            consequenceTypes = Arrays.asList(query.getString(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key())
                    .split("[,;]"));
        }

        // goal: [((xrefs OR regions) AND cts) OR (genes AND cts)] AND ... AND ...
        if (consequenceTypes.size() > 0) {
            if (genes.size() > 0) {
                // consequence types and genes
                String or = buildXrefOrRegionAndConsequenceType(xrefs, regions, consequenceTypes);
                if (or.isEmpty()) {
                    // no xrefs or regions: genes AND cts
                    filterList.add(buildGeneAndCt(genes, consequenceTypes));
                } else {
                    // otherwise: [((xrefs OR regions) AND cts) OR (genes AND cts)]
                    filterList.add(or + " OR (" + buildGeneAndCt(genes, consequenceTypes) + ")");
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
        String key = VariantDBAdaptor.VariantQueryParams.TYPE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parseCategoryTermValue(key, query.getString(key)));
        }

        // protein-substitution
        key = VariantDBAdaptor.VariantQueryParams.ANNOT_PROTEIN_SUBSTITUTION.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parseScoreValue(query.getString(key)));
        }

        // conservation
        key = VariantDBAdaptor.VariantQueryParams.ANNOT_CONSERVATION.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parseScoreValue(query.getString(key)));
        }

        // cadd, functional score
        key = VariantDBAdaptor.VariantQueryParams.ANNOT_FUNCTIONAL_SCORE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parseScoreValue(query.getString(key)));
        }

        // traits
        key = VariantDBAdaptor.VariantQueryParams.ANNOT_TRAITS.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parseCategoryTermValue("traits", query.getString(key)));
        }

        // maf population frequency
        // in the model: "popFreq__1kG_phase3__CLM":0.005319148767739534
        key = VariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parsePopValue("popFreq", query.getString(key)));
        }

        // stats maf
        // in the model: "stats__1kg_phase3__ALL"=0.02
        key = VariantDBAdaptor.VariantQueryParams.STATS_MAF.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parsePopValue("stats", query.getString(key)));
        }

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
    private static boolean isGene(String xref) {
        // TODO: this function must be completed
        if (xref.isEmpty()) {
            return false;
        }
        if (xref.indexOf(":") == -1) {
            return false;
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
    private static void classifyIds(String key, Query query, List<String> xrefs, List<String> genes) {
        String value;
        if (query.containsKey(key)) {
            value = (String) query.get(key);
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
     *
     * Parse string values, e.g.: dbSNP, type, chromosome,... This function takes into account multiple values and
     * the separator between them can be:
     *     "," or ";" to apply a "OR condition"
     *
     * @param name         Paramenter name
     * @param value        Paramenter value
     * @return             A list of strings, each string represents a boolean condition
     */
    private static String parseCategoryTermValue(String name, String value) {
        StringBuilder filter = new StringBuilder();
        if (StringUtils.isNotEmpty(value)) {
            String[] values = value.split("[,;]");
            if (values.length == 1) {
                filter.append(name).append(":\"").append(value).append("\"");
            } else {
                filter.append("(");
                filter.append(name).append(":\"").append(values[0]).append("\"");
                for (int i = 1; i < values.length; i++) {
                    filter.append(" OR ");
                    filter.append(name).append(":\"").append(values[i]).append("\"");
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
    private static String parseScoreValue(String value) {
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
    private static String parsePopValue(String name, String value) {
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
                List<String> list = new ArrayList<>(values.length);
                for (String v : values) {
                    matcher = STUDY_PATTERN.matcher(v);
                    if (matcher.find()) {
                        // concat expression, e.g.: value:[0 TO 12]
                        list.add(getRange(name + "__" + matcher.group(1) + "__", matcher.group(2),
                                matcher.group(3), matcher.group(4)));
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
    private static String getRange(String prefix, String name, String op, String value) {
        StringBuilder sb = new StringBuilder();
        switch (op) {
            case "=":
            case "==": {
                try {
                    Double v = Double.parseDouble(value);
                    // attention: negative values must be escaped
                    sb.append(prefix).append(getScoreName(name)).append(":").append(v < 0 ? "\\" : "").append(value);
                } catch (NumberFormatException e) {
                    switch (name.toLowerCase()) {
                        case "sift": {
                            sb.append(prefix).append("siftDesc").append(":\"").append(value).append("\"");
                            break;
                        }
                        case "polyphen": {
                            sb.append(prefix).append("polyphenDesc").append(":\"").append(value).append("\"");
                            break;
                        }
                        default: {
                            sb.append(prefix).append(getScoreName(name)).append(":\"").append(value).append("\"");
                        }
                    }
                }
                break;
            }

            case "!=": {
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
                        sb.append("-").append(prefix).append(getScoreName(name)).append(":").append(value);
                    }
                }
                break;
            }

            case ">": {
                sb.append(prefix).append(getScoreName(name)).append(":{").append(value).append(" TO *]");
                break;
            }

            case ">>":  {
                sb.append("(");
                if (StringUtils.isNotEmpty(prefix) && (prefix.startsWith("popFreq_") || prefix.startsWith("stats_"))) {
                    sb.append("(* -").append(prefix).append(getScoreName(name)).append(":*)");
                    sb.append(" OR ");
                    sb.append(prefix).append(getScoreName(name)).append(":{").append(value).append(" TO *]");
                } else {
                    sb.append(prefix).append(getScoreName(name)).append(":\\").append(VariantSearchToVariantConverter.MISSING_VALUE);
                    sb.append(" OR ");
                    sb.append(prefix).append(getScoreName(name)).append(":{").append(value).append(" TO *]");
                }
                sb.append(")");
                break;
            }

            case ">=": {
                sb.append(prefix).append(getScoreName(name)).append(":[").append(value).append(" TO *]");
                break;
            }

            case ">>=": {
                sb.append("(");
                if (StringUtils.isNotEmpty(prefix) && (prefix.startsWith("popFreq_") || prefix.startsWith("stats_"))) {
                    sb.append("(* -").append(prefix).append(getScoreName(name)).append(":*)");
                    sb.append(" OR ");
                    sb.append(prefix).append(getScoreName(name)).append(":[").append(value).append(" TO *]");
                } else {
                    // attention: negative values must be escaped
                    sb.append(prefix).append(getScoreName(name)).append(":\\").append(VariantSearchToVariantConverter.MISSING_VALUE);
                    sb.append(" OR ");
                    sb.append(prefix).append(getScoreName(name)).append(":[").append(value).append(" TO *]");
                }
                sb.append(")");
                break;
            }

            case "<": {
                sb.append(prefix).append(getScoreName(name)).append(":{")
                        .append(VariantSearchToVariantConverter.MISSING_VALUE).append(" TO ").append(value).append("}");
                break;
            }

            case "<<":  {
                if (StringUtils.isNotEmpty(prefix) && (prefix.startsWith("popFreq_") || prefix.startsWith("stats_"))) {
                    sb.append("(");
                    sb.append("(* -").append(prefix).append(getScoreName(name)).append(":*)");
                    sb.append(" OR ");
                    sb.append(prefix).append(getScoreName(name)).append(":[0 TO ").append(value).append("}");
                    sb.append(")");
                } else {
                    sb.append(prefix).append(getScoreName(name)).append(":[")
                            .append(VariantSearchToVariantConverter.MISSING_VALUE).append(" TO ").append(value).append("}");
                }
                break;
            }

            case "<=": {
                sb.append(prefix).append(getScoreName(name)).append(":{")
                        .append(VariantSearchToVariantConverter.MISSING_VALUE).append(" TO ").append(value).append("]");
                break;
            }

            case "<<=": {
                if (StringUtils.isNotEmpty(prefix) && (prefix.startsWith("popFreq_") || prefix.startsWith("stats_"))) {
                    sb.append("(");
                    sb.append("(* -").append(prefix).append(getScoreName(name)).append(":*)");
                    sb.append(" OR ");
                    sb.append(prefix).append(getScoreName(name)).append(":[0 TO ").append(value).append("]");
                    sb.append(")");
                } else {
                    sb.append(prefix).append(getScoreName(name)).append(":[")
                            .append(VariantSearchToVariantConverter.MISSING_VALUE).append(" TO ").append(value).append("]");
                }
                break;
            }
            default: {
                logger.debug("Unknown operator {}", op);
                break;
            }
        }
        return sb.toString();
    }

    private static SolrQuery.ORDER getSortOrder(QueryOptions queryOptions) {
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
    private static String buildXrefOrGeneOrRegion(List<String> xrefs, List<String> genes, List<Region> regions) {
        StringBuilder sb = new StringBuilder();

        // first, concatenate xrefs and genes in single list
        List<String> ids = new ArrayList<>();
        if (xrefs != null && xrefs.size() > 0) {
            ids.addAll(xrefs);
        }
        if (genes != null && genes.size() > 0) {
            ids.addAll(genes);
        }
        if (ids.size() > 0) {
            for (String id : ids) {
                if (sb.length() > 0) {
                    sb.append(" OR ");
                }
                sb.append("xrefs:\"").append(id).append("\"");
            }
        }

        // and now regions
        for (Region region: regions) {
            if (!region.getChromosome().isEmpty()) {
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
    private static String buildConsequenceTypeOr(List<String> cts) {
        StringBuilder sb = new StringBuilder();
        for (String ct : cts) {
            if (sb.length() > 0) {
                sb.append(" OR ");
            }
            sb.append("soAcc:").append(VariantDBAdaptorUtils.parseConsequenceType(ct));
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
    private static String buildXrefOrRegionAndConsequenceType(List<String> xrefs, List<Region> regions, List<String> cts) {
        String orCts = buildConsequenceTypeOr(cts);
        String orXrefs = buildXrefOrGeneOrRegion(xrefs, null, regions);
        if (orXrefs.isEmpty()) {
            // consequences type but no xrefs, no genes, no regions
            // we must make an OR with all consequences types and add it to the "AND" filter list
            return orCts;
        } else {
            return orXrefs + " AND (" + orCts + ")";
        }
    }

    /**
     * Build the condition: genes AND cts.
     *
     * @param genes    List of genes
     * @param cts      List of consequence types
     * @return         OR/AND condition string
     */
    private static String buildGeneAndCt(List<String> genes, List<String> cts) {
        // in the VariantSearchModel the (gene AND ct) is modeled in the field: geneToSoAcc:gene_ct
        // and if there are multiple genes and consequence types, we have to build the combination of all of them in a OR expression
        StringBuilder sb = new StringBuilder();
        for (String gene: genes) {
            for (String ct: cts) {
                if (sb.length() > 0) {
                    sb.append(" OR ");
                }
                sb.append("geneToSoAcc:").append(gene).append("_").append(VariantDBAdaptorUtils.parseConsequenceType(ct));
            }
        }
        return sb.toString();
    }
}
