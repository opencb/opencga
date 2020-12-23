package org.opencb.opencga.storage.core.rga;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrException;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.solr.FacetQueryParser;
import org.opencb.opencga.storage.core.exceptions.RgaException;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

import static org.opencb.opencga.core.models.analysis.knockout.KnockoutVariant.KnockoutType.*;
import static org.opencb.opencga.storage.core.rga.RgaQueryParams.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.printQuery;

public class RgaQueryParser {

    private static final Pattern FACET_RANGE_PATTERN = Pattern.compile("([_a-zA-Z]+)\\[([_a-zA-Z]+):([.a-zA-Z0-9]+)\\]:([.0-9]+)$");
    public static final String SEPARATOR = "__";

    protected static Logger logger = LoggerFactory.getLogger(RgaQueryParser.class);

    /**
     * Create a SolrQuery object from Query and QueryOptions.
     *
     * @param query         Query
     * @param queryOptions  Query Options
     * @return              SolrQuery
     * @throws RgaException RgaException.
     */
    public SolrQuery parse(Query query, QueryOptions queryOptions) throws RgaException {
        SolrQuery solrQuery = new SolrQuery();

        List<String> filterList = new ArrayList<>();

        //-------------------------------------
        // QueryOptions processing
        //-------------------------------------

        // Facet management, (including facet ranges, nested facets and aggregation functions)
        if (queryOptions.containsKey(QueryOptions.FACET) && StringUtils.isNotEmpty(queryOptions.getString(QueryOptions.FACET))) {
            try {
                FacetQueryParser facetQueryParser = new FacetQueryParser();

                String facetQuery = parseFacet(queryOptions.getString(QueryOptions.FACET));
                String jsonFacet = facetQueryParser.parse(facetQuery);

                solrQuery.set("json.facet", jsonFacet);
                solrQuery.setRows(0);
                solrQuery.setStart(0);
                solrQuery.setFields();

                logger.debug(">>>>>> Solr Facet: " + solrQuery.toString());
            } catch (Exception e) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Solr parse exception: " + e.getMessage(), e);
            }
        } else {
            // If the query is not a facet we must set the proper include, limit, skip and sort
            // Get the correct includes
//            String[] includes;
//            if (queryOptions.containsKey(QueryOptions.INCLUDE)) {
//                includes = solrIncludeFields(queryOptions.getAsStringList(QueryOptions.INCLUDE));
//            } else {
//                if (queryOptions.containsKey(QueryOptions.EXCLUDE)) {
//                    includes = getSolrIncludeFromExclude(queryOptions.getAsStringList(QueryOptions.EXCLUDE));
//                } else {
//                    // We want all possible fields
//                    includes = getSolrIncludeFromExclude(Collections.emptyList());
//                }
//            }
//            includes = ArrayUtils.removeAllOccurences(includes, "release");
//            includes = includeFieldsWithMandatory(includes);
//            solrQuery.setFields(includes);
//
//            // Add Solr fields from the variant includes, i.e.: includeSample, includeFormat,...
//            List<String> solrFieldsToInclude = getSolrFieldsFromVariantIncludes(query, queryOptions);
//            for (String solrField : solrFieldsToInclude) {
//                solrQuery.addField(solrField);
//            }

            if (queryOptions.containsKey(QueryOptions.INCLUDE)) {
                for (String include : queryOptions.getAsStringList(QueryOptions.INCLUDE)) {
                    solrQuery.addField(include);
                }
            } else if (queryOptions.containsKey(QueryOptions.EXCLUDE)) {
//                    includes = getSolrIncludeFromExclude(queryOptions.getAsStringList(QueryOptions.EXCLUDE));
            }

            if (queryOptions.containsKey(QueryOptions.LIMIT)) {
                solrQuery.setRows(queryOptions.getInt(QueryOptions.LIMIT));
            }

            if (queryOptions.containsKey(QueryOptions.SKIP)) {
                solrQuery.setStart(queryOptions.getInt(QueryOptions.SKIP));
            }

//            if (queryOptions.containsKey(QueryOptions.SORT)) {
//                solrQuery.addSort(queryOptions.getString(QueryOptions.SORT), getSortOrder(queryOptions));
//            }
        }

        //-------------------------------------
        // Query processing
        //-------------------------------------

        parseStringValue(query, SAMPLE_ID, SAMPLE_ID.key(), filterList);
        parseStringValue(query, INDIVIDUAL_ID, "indId", filterList);
        parseStringValue(query, SEX, SEX.key(), filterList);
        parseStringValue(query, PHENOTYPES, "pheno", filterList);
        parseStringValue(query, DISORDERS, "disor", filterList);
        parseStringValue(query, GENE_ID, GENE_ID.key(), filterList);
        parseStringValue(query, GENE_NAME, GENE_NAME.key(), filterList);
        parseStringValue(query, TRANSCRIPT_ID, "transcId", filterList);
        parseStringValue(query, BIOTYPE, "biotype", filterList);
        parseStringValue(query, VARIANTS, "variants", filterList);
        parseFilterValue(query, filterList);

        // Create Solr query, adding filter queries and fields to show
        solrQuery.setQuery("*:*");
        filterList.forEach(solrQuery::addFilterQuery);

        logger.debug("----------------------");
        logger.debug("query     : " + printQuery(query));
        logger.debug("solrQuery : " + solrQuery);
        return solrQuery;
    }

    private void parseFilterValue(Query query, List<String> filterList) throws RgaException {
        List<String> knockoutValues = query.getAsStringList(KNOCKOUT.key());
        String filterValue = query.getString(FILTER.key());
        List<String> ctValues = query.getAsStringList(CONSEQUENCE_TYPE.key());
        List<String> popFreqValues = query.getAsStringList(POPULATION_FREQUENCY.key());

        if (ctValues.isEmpty() && !popFreqValues.isEmpty()) {
            throw new RgaException("Filtering by '" + CONSEQUENCE_TYPE.key()  + "' is mandatory when filtering by '"
                    + POPULATION_FREQUENCY.key() + "'");
        }

        List<List<String>> filters = new LinkedList<>();

        // Pop. freq
        List<List<String>> popFreqQueryList = RgaUtils.parsePopulationFrequencyQuery(popFreqValues);
        for (List<String> sublist : popFreqQueryList) {
            replicateFilters(filters, sublist.size());
            addFilterValues(filters, sublist);
        }

        // CT
        if (!ctValues.isEmpty()) {
            List<String> encodedCTValues = new ArrayList<>(ctValues.size());
            for (String ctValue : ctValues) {
                String encodedValue = String.valueOf(VariantQueryUtils.parseConsequenceType(ctValue));
                encodedCTValues.add(encodedValue);
            }
            replicateFilters(filters, encodedCTValues.size());
            addFilterValues(filters, encodedCTValues);
        }

        // Filter
        List<String> filterValues;
        if (StringUtils.isNotEmpty(filterValue)) {
            filterValues = Collections.singletonList(filterValue);
        } else {
            if (!filters.isEmpty()) {
                filterValues = Arrays.asList("PASS", "NOT_PASS");
            } else {
                filterValues = Collections.emptyList();
            }
        }
        filterValues = RgaUtils.parseFilterQuery(filterValues);
        replicateFilters(filters, filterValues.size());
        addFilterValues(filters, filterValues);

        // KT
        if (knockoutValues.isEmpty() && !filters.isEmpty()) {
            knockoutValues = Arrays.asList(COMP_HET.name(), DELETION_OVERLAP.name(), HET_ALT.name(), HOM_ALT.name());
        }
        knockoutValues = RgaUtils.parseKnockoutTypeQuery(knockoutValues);
        replicateFilters(filters, knockoutValues.size());
        addFilterValues(filters, knockoutValues);

        if (!filters.isEmpty()) {
            filterList.add("cFilters:" + parseQueryFilter(filters));
        }
    }

    private void addFilterValues(List<List<String>> filters, List<String> values) {
        int size = values.size();
        for (int i = 0; i < filters.size(); i += size) {
            for (int j = 0; j < values.size(); j++) {
                filters.get(i + j).add(0, values.get(j));
            }
        }
    }

    private void replicateFilters(List<List<String>> filters, int size) {
        if (filters.isEmpty()) {
            if (size > 0) {
                for (int i = 0; i < size; i++) {
                    filters.add(new LinkedList<>());
                }
            }
        } else {
            if (size > 1) {
                // Replicate filters as many times as new elements we will need to add
                int numberOfCopies = size - 1;

                List<List<String>> replicatedFilterList = new LinkedList<>();

                for (int i = 0; i < numberOfCopies; i++) {
                    List<List<String>> tmpFilters = new ArrayList<>(filters.size());
                    for (List<String> filter : filters) {
                        tmpFilters.add(new ArrayList<>(filter));
                    }

                    replicatedFilterList.addAll(tmpFilters);
                }

                filters.addAll(replicatedFilterList);
            }
        }
    }

    private void parseStringValue(Query query, RgaQueryParams queryParam, String storageKey, List<String> filterList) {
        if (StringUtils.isNotEmpty(query.getString(queryParam.key()))) {
            filterList.add(storageKey + ":( " + StringUtils.join(query.getAsStringList(queryParam.key()), " || ") + " )");
        }
    }

    private String parseFacet(String facetQuery) {
        StringBuilder sb = new StringBuilder();
        String[] facets = facetQuery.split(FacetQueryParser.FACET_SEPARATOR);

        for (int i = 0; i < facets.length; i++) {
            if (i > 0) {
                sb.append(FacetQueryParser.FACET_SEPARATOR);
            }
            String[] nestedFacets = facets[i].split(FacetQueryParser.NESTED_FACET_SEPARATOR);
            for (int j = 0; j < nestedFacets.length; j++) {
                if (j > 0) {
                    sb.append(FacetQueryParser.NESTED_FACET_SEPARATOR);
                }
                String[] nestedSubfacets = nestedFacets[j].split(FacetQueryParser.NESTED_SUBFACET_SEPARATOR);
                for (int k = 0; k < nestedSubfacets.length; k++) {
                    if (k > 0) {
                        sb.append(FacetQueryParser.NESTED_SUBFACET_SEPARATOR);
                    }
                    // Convert to Solr schema fields, if necessary
                    sb.append(toSolrSchemaFields(nestedSubfacets[k]));
                }
            }
        }

        return sb.toString();
    }

    private String parseQueryFilter(List<List<String>> filters) {
        StringBuilder builder = new StringBuilder();
        if (filters.size() > 1) {
            builder.append("( ");
        }
        for (int i = 0; i < filters.size(); i++) {
            if (i != 0) {
                builder.append("|| ");
            }
            List<String> filter = filters.get(i);
            builder.append(StringUtils.join(filter, SEPARATOR)).append(" ");
        }
        if (filters.size() > 1) {
            builder.append(")");
        }

        return builder.toString();
    }

//    private String parseFacet(String facet, String categoryName) {
//        if (facet.contains("(")) {
//            // Aggregation function
//            return facet.replace(categoryName, "").replace("[", "").replace("]", "");
//        } else if (facet.contains("..")) {
//            // Range
//            Matcher matcher = FACET_RANGE_PATTERN.matcher(facet);
//            if (matcher.find()) {
//                return matcher.group(2) + "[" + matcher.group(3) + "]:" + matcher.group(4);
//            } else {
//                throw VariantQueryException.malformedParam(categoryName, facet, "Invalid syntax for facet range.");
//            }
//        }
//        // Nothing to do
//        return facet;
//    }

    private String toSolrSchemaFields(String facet) {
//        if (facet.contains(CHROM_DENSITY)) {
//            return parseChromDensity(facet);
//        } else if (facet.contains(ANNOT_FUNCTIONAL_SCORE.key())) {
//            return parseFacet(facet, ANNOT_FUNCTIONAL_SCORE.key());
//        } else if (facet.contains(ANNOT_CONSERVATION.key())) {
//            return parseFacet(facet, ANNOT_CONSERVATION.key());
//        } else if (facet.contains(ANNOT_PROTEIN_SUBSTITUTION.key())) {
//            return parseFacet(facet, ANNOT_PROTEIN_SUBSTITUTION.key());
//        } else if (facet.contains(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key())) {
//            return parseFacetWithStudy(facet, "popFreq");
//        } else if (facet.contains(STATS_ALT.key())) {
//            return parseFacetWithStudy(facet, "altStats");
//        } else if (facet.contains(SCORE.key())) {
//            return parseFacetWithStudy(facet, SCORE.key());
//        } else {
            return facet;
//        }
    }

}
