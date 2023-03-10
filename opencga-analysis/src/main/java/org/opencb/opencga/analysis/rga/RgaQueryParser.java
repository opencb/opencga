package org.opencb.opencga.analysis.rga;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.solr.FacetQueryParser;
import org.opencb.opencga.analysis.rga.exceptions.RgaException;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutVariant;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opencb.opencga.analysis.rga.RgaQueryParams.*;
import static org.opencb.opencga.core.models.analysis.knockout.KnockoutVariant.KnockoutType.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.printQuery;

public class RgaQueryParser {

    private static final Pattern FACET_RANGE_PATTERN = Pattern.compile("([_a-zA-Z]+)\\[([_a-zA-Z]+):([.a-zA-Z0-9]+)\\]:([.0-9]+)$");
    private final CompHetQueryMode compHetQueryMode;
    public static final String SEPARATOR = "__";

    protected static Logger logger = LoggerFactory.getLogger(RgaQueryParser.class);

    private static final List<String> ALL_CONSEQUENCE_TYPES;
    private static final List<String> ALL_PAIRED_CONSEQUENCE_TYPES;
    private static final List<String> INCLUDED_DEL_OVERLAP_CONSEQUENCE_TYPES;
    private static final List<String> INCLUDED_DEL_OVERLAP_PAIR_CTS;

    static {
        List<String> excludedDelOverlapCts = getEncodedConsequenceTypes(Arrays.asList("missense_variant", "frameshift_variant",
                "incomplete_terminal_codon_variant", "start_lost", "stop_gained", "stop_lost", "splice_acceptor_variant",
                "splice_donor_variant", "splice_region_variant"));

        // Exclude DELETION_OVERLAP variants with consequence types: missense_variant
        ALL_CONSEQUENCE_TYPES = getEncodedConsequenceTypes(RgaUtils.CONSEQUENCE_TYPE_LIST);
        ALL_PAIRED_CONSEQUENCE_TYPES = generateSortedCombinations(ALL_CONSEQUENCE_TYPES);
        INCLUDED_DEL_OVERLAP_CONSEQUENCE_TYPES = ALL_CONSEQUENCE_TYPES
                .stream()
                .filter(ct -> !excludedDelOverlapCts.contains(ct))
                .collect(Collectors.toList());
        INCLUDED_DEL_OVERLAP_PAIR_CTS = generateSortedCombinations(INCLUDED_DEL_OVERLAP_CONSEQUENCE_TYPES);
    }

    public RgaQueryParser() {
        this(CompHetQueryMode.SINGLE);
    }

    public RgaQueryParser(CompHetQueryMode compHetQueryMode) {
        this.compHetQueryMode = compHetQueryMode != null ? compHetQueryMode : CompHetQueryMode.SINGLE;
    }

    /**
     * Create a SolrQuery object from Query and QueryOptions for the main RGA collection.
     *
     * @param query         Query
     * @return              SolrQuery
     * @throws RgaException RgaException.
     */
    public SolrQuery parseQuery(Query query) throws RgaException {
        SolrQuery solrQuery = new SolrQuery();

        Query finalQuery = new Query(query);
        fixQuery(finalQuery);

        List<String> filterList = new ArrayList<>();
        parseStringValue(finalQuery, SAMPLE_ID, RgaDataModel.SAMPLE_ID, filterList);
        parseStringValue(finalQuery, INDIVIDUAL_ID, RgaDataModel.INDIVIDUAL_ID, filterList);
        parseStringValue(finalQuery, SEX, RgaDataModel.SEX, filterList);
        parseStringValue(finalQuery, PHENOTYPES, RgaDataModel.PHENOTYPES, filterList);
        parseStringValue(finalQuery, DISORDERS, RgaDataModel.DISORDERS, filterList);
        parseStringValue(finalQuery, CHROMOSOME, RgaDataModel.CHROMOSOME, filterList);
        parseStringValue(finalQuery, START, RgaDataModel.START, filterList);
        parseStringValue(finalQuery, END, RgaDataModel.END, filterList);
        parseStringValue(finalQuery, NUM_PARENTS, RgaDataModel.NUM_PARENTS, filterList);
        parseStringValue(finalQuery, GENE_ID, RgaDataModel.GENE_ID, filterList);
        parseStringValue(finalQuery, GENE_NAME, RgaDataModel.GENE_NAME, filterList);
        parseStringValue(finalQuery, TRANSCRIPT_ID, RgaDataModel.TRANSCRIPT_ID, filterList);
        parseStringValue(finalQuery, TYPE, RgaDataModel.TYPES, filterList);
        parseStringValue(finalQuery, CLINICAL_SIGNIFICANCE, RgaDataModel.CLINICAL_SIGNIFICANCES, filterList);
//        parseStringValue(finalQuery, TRANSCRIPT_BIOTYPE, RgaDataModel.TRANSCRIPT_BIOTYPE, filterList);
        parseStringValue(finalQuery, VARIANTS, RgaDataModel.VARIANTS, filterList);
        parseStringValue(finalQuery, DB_SNPS, RgaDataModel.DB_SNPS, filterList);
        parseMainCollCompoundFilters(finalQuery, filterList);

        // Create Solr query, adding filter queries and fields to show
        solrQuery.setQuery("*:*");
        filterList.forEach(solrQuery::addFilterQuery);

        logger.debug("----------------------");
        logger.debug("query     : " + printQuery(finalQuery));
        logger.debug("solrQuery : " + solrQuery);
        return solrQuery;
    }

    /**
     * Create a SolrQuery object from Query and QueryOptions for the auxliar RGA collection.
     *
     * @param query         Query
     * @return              SolrQuery
     * @throws RgaException RgaException.
     */
    public SolrQuery parseAuxQuery(Query query) throws RgaException {
        SolrQuery solrQuery = new SolrQuery();

        Query finalQuery = new Query(query);
        fixQuery(finalQuery);

        List<String> filterList = new ArrayList<>();
        parseStringValue(finalQuery, VARIANTS, AuxiliarRgaDataModel.ID, filterList);
        parseStringValue(finalQuery, DB_SNPS, AuxiliarRgaDataModel.DB_SNP, filterList);
        parseStringValue(finalQuery, TYPE, AuxiliarRgaDataModel.TYPE, filterList);
        parseStringValue(finalQuery, CLINICAL_SIGNIFICANCE, AuxiliarRgaDataModel.CLINICAL_SIGNIFICANCES, filterList);
        parseStringValue(finalQuery, GENE_ID, AuxiliarRgaDataModel.GENE_IDS, filterList);
        parseStringValue(finalQuery, GENE_NAME, AuxiliarRgaDataModel.GENE_NAMES, filterList);
        parseStringValue(finalQuery, TRANSCRIPT_ID, AuxiliarRgaDataModel.TRANSCRIPT_IDS, filterList);
        parseAuxCollCompoundFilters(finalQuery, filterList);

        // Create Solr query, adding filter queries and fields to show
        solrQuery.setQuery("*:*");
        filterList.forEach(solrQuery::addFilterQuery);

        logger.debug("----------------------");
        logger.debug("query     : " + printQuery(finalQuery));
        logger.debug("solrQuery : " + solrQuery);
        return solrQuery;
    }

    public Predicate<RgaDataModel> getCompHetVariantsPostQueryPredicate(Query query) {
        List<String> variants = query.getAsStringList(RgaQueryParams.VARIANTS.key(), ";");
        List<String> knockoutTypes = query.getAsStringList(RgaQueryParams.KNOCKOUT.key());
        if (variants.size() > 1 && knockoutTypes.size() == 1 && knockoutTypes.get(0).equals(KnockoutVariant.KnockoutType.COMP_HET.name())) {
            return rgaDataModel -> {
                if (rgaDataModel == null || rgaDataModel.getVariants() == null || rgaDataModel.getKnockoutTypes() == null) {
                    return false;
                }
                // Get the positions where the variants are found
                List<Integer> positions = new ArrayList<>(variants.size());
                for (String variant : variants) {
                    positions.add(rgaDataModel.getVariants().indexOf(variant));
                }
                // And check that the knockout type corresponding to those variants is COMP_HET
                for (Integer position : positions) {
                    if (position < 0
                            || !rgaDataModel.getKnockoutTypes().get(position).equals(KnockoutVariant.KnockoutType.COMP_HET.name())) {
                        return false;
                    }
                }
                return true;
            };
        } else {
            return null;
        }
    }

    private void parseMainCollCompoundFilters(Query query, List<String> filterList) throws RgaException {
        List<String> knockoutValues = query.getAsStringList(KNOCKOUT.key());
        String filterValue = query.getString(FILTER.key());
        List<String> ctValues = query.getAsStringList(CONSEQUENCE_TYPE.key());
        List<String> popFreqValues = query.getAsStringList(POPULATION_FREQUENCY.key(), ";");

        int count = 0;
        count += knockoutValues.isEmpty() ? 0 : 1;
        count += StringUtils.isEmpty(filterValue) ? 0 : 1;
        count += ctValues.isEmpty() ? 0 : 1;
        count += popFreqValues.isEmpty() ? 0 : 1;

        boolean simpleFilter = !knockoutValues.contains(COMP_HET.name()) && !knockoutValues.contains(DELETION_OVERLAP.name()) && count == 1;

        if (simpleFilter) {
            // Simple filter
            parseStringValue(query, KNOCKOUT, RgaDataModel.KNOCKOUT_TYPES, filterList);
            parseStringValue(query, FILTER, RgaDataModel.FILTERS, filterList);
            parseStringValue(query, CONSEQUENCE_TYPE, RgaDataModel.CONSEQUENCE_TYPES, filterList);

            if (!popFreqValues.isEmpty()) {
                Map<String, List<String>> encodedPopFreqs = RgaUtils.parsePopulationFrequencyQuery(popFreqValues);

                for (Map.Entry<String, List<String>> entry : encodedPopFreqs.entrySet()) {
                    parseStringValue(entry.getValue(), RgaDataModel.POPULATION_FREQUENCIES.replace("*", entry.getKey()), filterList, "||");
                }
            }
        } else {
            buildComplexQueryFilter(filterList, knockoutValues, filterValue, ctValues, popFreqValues);
        }
    }

    private void parseAuxCollCompoundFilters(Query query, List<String> filterList) throws RgaException {
        List<String> knockoutValues = query.getAsStringList(KNOCKOUT.key());
//        String filterValue = query.getString(FILTER.key());
        List<String> ctValues = query.getAsStringList(CONSEQUENCE_TYPE.key());
        List<String> popFreqValues = query.getAsStringList(POPULATION_FREQUENCY.key(), ";");

        int count = 0;
        count += knockoutValues.isEmpty() ? 0 : 1;
//        count += StringUtils.isEmpty(filterValue) ? 0 : 1;
        count += ctValues.isEmpty() ? 0 : 1;
        count += popFreqValues.isEmpty() ? 0 : 1;

        boolean simpleFilter = !knockoutValues.contains(COMP_HET.name()) && !knockoutValues.contains(DELETION_OVERLAP.name()) && count == 1;

        if (simpleFilter) {
            // Simple filters
            if (!ctValues.isEmpty()) {
                List<String> encodedCtValues = getEncodedConsequenceTypes(ctValues);
                query.put(CONSEQUENCE_TYPE.key(), encodedCtValues);
            }

            parseStringValue(query, KNOCKOUT, AuxiliarRgaDataModel.KNOCKOUT_TYPES, filterList);
            parseStringValue(query, CONSEQUENCE_TYPE, AuxiliarRgaDataModel.CONSEQUENCE_TYPES, filterList);
            if (!popFreqValues.isEmpty()) {
                Map<String, List<String>> encodedPopFreqs = RgaUtils.parsePopulationFrequencyQuery(popFreqValues);
                for (Map.Entry<String, List<String>> entry : encodedPopFreqs.entrySet()) {
                    parseStringValue(entry.getValue(),
                            AuxiliarRgaDataModel.POPULATION_FREQUENCIES.replace("*", entry.getKey()), filterList, "||");
                }
            }
        } else {
            buildComplexQueryFilter(filterList, knockoutValues, "", ctValues, popFreqValues);
        }
    }

    private void fixQuery(Query query) {
        if (query.containsKey(CONSEQUENCE_TYPE.key())) {
            // Convert CONSEQUENCE TYPES to full SO Terms so they can be successfully processed
            List<String> orConsequenceTypeList = query.getAsStringList(CONSEQUENCE_TYPE.key(), ",");
            List<String> andConsequenceTypeList = query.getAsStringList(CONSEQUENCE_TYPE.key(), ";");

            List<String> consequenceTypeList;
            String separator;
            if (orConsequenceTypeList.size() >= andConsequenceTypeList.size()) {
                consequenceTypeList = orConsequenceTypeList;
                separator = ",";
            } else {
                consequenceTypeList = andConsequenceTypeList;
                separator = ";";
            }

            List<String> result = new ArrayList<>(consequenceTypeList.size());
            for (String ct : consequenceTypeList) {
                if (ct.startsWith("SO:")) {
                    result.add(ct);
                } else {
                    result.add(ConsequenceTypeMappings.getSoAccessionString(ct));
                }
            }

            query.put(CONSEQUENCE_TYPE.key(), StringUtils.join(result, separator));
        }
    }

    private void buildComplexQueryFilter(List<String> filterList, List<String> knockoutList, String filterValue, List<String> ctList,
                                         List<String> popFreqList) throws RgaException {
        // KT
        List<String> koValues;
        if (knockoutList.isEmpty()) {
            koValues = Arrays.asList(COMP_HET.name(), DELETION_OVERLAP.name(), HET_ALT.name(), HOM_ALT.name());
        } else {
            koValues = knockoutList;
        }
        koValues = RgaUtils.parseKnockoutTypeQuery(koValues);

        // Filter
        List<String> filterValues;
        if (StringUtils.isEmpty(filterValue)) {
            filterValues = Arrays.asList("PASS", "NOT_PASS");
        } else {
            filterValues = Collections.singletonList(filterValue);
        }
        filterValues = RgaUtils.parseFilterQuery(filterValues);

        // CT
        List<String> ctValues = getEncodedConsequenceTypes(ctList);

        // Pop. freq
        Map<String, List<String>> popFreqQueryList = RgaUtils.parsePopulationFrequencyQuery(popFreqList);

        buildComplexQuery(koValues, filterValues, ctValues, popFreqQueryList, filterList);
    }

    private static List<String> getEncodedConsequenceTypes(List<String> originalCtList) {
        if (CollectionUtils.isEmpty(originalCtList)) {
            return Collections.emptyList();
        }
        List<String> ctValues = new ArrayList<>(originalCtList.size());
        for (String ctValue : originalCtList) {
            String encodedValue = String.valueOf(VariantQueryUtils.parseConsequenceType(ctValue));
            ctValues.add(encodedValue);
        }
        return ctValues;
    }

    private void buildComplexQuery(List<String> koValues, List<String> filterValues, List<String> ctValues,
                                   Map<String, List<String>> popFreqQueryList, List<String> filterList) throws RgaException {
        String encodedChString = RgaUtils.encode(COMP_HET.name());
        String delOverlap = RgaUtils.encode(DELETION_OVERLAP.name());

        List<String> chFilterValues = filterValues;
        List<String> chCtValues = ctValues;
        if (compHetQueryMode.equals(CompHetQueryMode.PAIR)) {
            // To generate pairs to query for complete COMP_HET variants
            chFilterValues = generateSortedCombinations(filterValues);
            chCtValues = generateSortedCombinations(ctValues);
        }

        if (ctValues.isEmpty() && popFreqQueryList.isEmpty()) {
            // KT + FILTER
            List<String> orFilterList = new LinkedList<>();
            for (String koValue : koValues) {
                if (compHetQueryMode.equals(CompHetQueryMode.PAIR) && koValue.equals(encodedChString)) {
                    for (String filterVal : chFilterValues) {
                        orFilterList.add(koValue + SEPARATOR + filterVal);
                    }
                } else {
                    for (String filterVal : filterValues) {
                        if (koValue.equals(delOverlap)) {
                            for (String ctValue : INCLUDED_DEL_OVERLAP_CONSEQUENCE_TYPES) {
                                orFilterList.add(koValue + SEPARATOR + filterVal + SEPARATOR + ctValue);
                            }
                        } else {
                            orFilterList.add(koValue + SEPARATOR + filterVal);
                        }
                    }
                }
            }
            parseStringValue(orFilterList, RgaDataModel.COMPOUND_FILTERS, filterList, "||");
        } else if (!ctValues.isEmpty() && !popFreqQueryList.isEmpty()) {
            // KT + FILTER + CT + POP_FREQ
            List<String> andQueryList = new LinkedList<>();
            if (popFreqQueryList.size() == 2) {
                List<String> koQueryList = new LinkedList<>();
                for (String koValue : koValues) {
                    if (compHetQueryMode.equals(CompHetQueryMode.PAIR) && koValue.equals(encodedChString)) {
                        ArrayList<String> popFreqKeys = new ArrayList<>(popFreqQueryList.keySet());
                        List<List<String>> sortedPopFreqs = RgaUtils.generateSortedCombinations(popFreqQueryList.get(popFreqKeys.get(0)),
                                popFreqQueryList.get(popFreqKeys.get(1)));
                        List<String> popFreqAndQueryList = new LinkedList<>();
                        List<String> tmpOrQueryList = new LinkedList<>();
                        for (List<String> sortedPopFreq : sortedPopFreqs) {
                            for (String filterVal : chFilterValues) {
                                for (String ctValue : chCtValues) {
                                    // CH__P__P__1583__1583__P1-1__P2-2
                                    tmpOrQueryList.add(koValue + SEPARATOR + filterVal + SEPARATOR + ctValue + SEPARATOR
                                            + sortedPopFreq.get(0) + SEPARATOR + sortedPopFreq.get(1));
                                }
                            }
                        }
                        parseStringValue(tmpOrQueryList, "", popFreqAndQueryList, "||");

                        List<String> filterValuesOrList = new LinkedList<>();
                        for (String filterVal : chFilterValues) {
                            List<String> tmpAndList = new LinkedList<>();
                            for (List<String> popFreqList : popFreqQueryList.values()) {
                                List<String> popFreqOrQueryList = new LinkedList<>();
                                for (String popFreq : popFreqList) {
                                    popFreqOrQueryList.add(koValue + SEPARATOR + filterVal + SEPARATOR + popFreq);
                                }
                                parseStringValue(popFreqOrQueryList, "", tmpAndList, "||");
                            }
                            parseStringValue(tmpAndList, "", filterValuesOrList, "&&");
                        }
                        parseStringValue(filterValuesOrList, "", popFreqAndQueryList, "||");
                        parseStringValue(popFreqAndQueryList, "", koQueryList, "&&");
                    } else {
                        List<String> orQueryList = new LinkedList<>();
                        for (String ctValue : ctValues) {
                            if (koValue.equals(delOverlap) && !INCLUDED_DEL_OVERLAP_CONSEQUENCE_TYPES.contains(ctValue)) {
                                // Don't process this filter
                                continue;
                            }
                            for (String filterValue : filterValues) {
                                List<String> tmpAndQueryList = new ArrayList<>(popFreqQueryList.size());
                                for (List<String> popFreqs : popFreqQueryList.values()) {
                                    List<String> tmpOrQueryList = new ArrayList<>(popFreqs.size());
                                    for (String popFreq : popFreqs) {
                                        tmpOrQueryList.add(koValue + SEPARATOR + filterValue + SEPARATOR + ctValue + SEPARATOR + popFreq);
                                    }
                                    parseStringValue(tmpOrQueryList, "", tmpAndQueryList, "||");
                                }
                                parseStringValue(tmpAndQueryList, "", orQueryList, "&&");
                            }
                        }
                        parseStringValue(orQueryList, "", koQueryList, "||");
                    }
                }
                parseStringValue(koQueryList, "", andQueryList, "||");
            } else {
                for (List<String> tmpPopFreqList : popFreqQueryList.values()) {
                    List<String> orQueryList = new LinkedList<>();
                    for (String koValue : koValues) {
                        List<String> finalFilterValues = koValue.equals(encodedChString) ? chFilterValues : filterValues;
                        List<String> finalCtValues = koValue.equals(encodedChString) ? chCtValues : ctValues;
                        for (String filterVal : finalFilterValues) {
                            for (String ctValue : finalCtValues) {
                                if (compHetQueryMode.equals(CompHetQueryMode.PAIR) && koValue.equals(encodedChString)) {
                                    if (tmpPopFreqList.size() == 1) {
                                        // Replicate the same value so it filters as a pair
                                        tmpPopFreqList.add(tmpPopFreqList.get(0));
                                    }
                                    List<String> sortedCombinations = generateSortedCombinations(tmpPopFreqList);
                                    for (String popFreqPair : sortedCombinations) {
                                        orQueryList.add(koValue + SEPARATOR + filterVal + SEPARATOR + ctValue + SEPARATOR + popFreqPair);
                                    }
                                } else {
                                    if (koValue.equals(delOverlap) && !INCLUDED_DEL_OVERLAP_CONSEQUENCE_TYPES.contains(ctValue)) {
                                        // Don't process this filter
                                        continue;
                                    }
                                    for (String popFreq : tmpPopFreqList) {
                                        orQueryList.add(koValue + SEPARATOR + filterVal + SEPARATOR + ctValue + SEPARATOR + popFreq);
                                    }
                                }
                            }
                        }
                    }
                    parseStringValue(orQueryList, "", andQueryList, "||");
                }
            }
            parseStringValue(andQueryList, RgaDataModel.COMPOUND_FILTERS, filterList, "&&");
        } else if (!ctValues.isEmpty()) {
            // KT + FILTER + CT
            List<String> orFilterList = new LinkedList<>();
            for (String koValue : koValues) {
                List<String> finalFilterValues = koValue.equals(encodedChString) ? chFilterValues : filterValues;
                List<String> finalCtValues = koValue.equals(encodedChString) ? chCtValues : ctValues;
                for (String filterVal : finalFilterValues) {
                    for (String ctValue : finalCtValues) {
                        if (koValue.equals(delOverlap) && !INCLUDED_DEL_OVERLAP_CONSEQUENCE_TYPES.contains(ctValue)) {
                            // Don't process this filter
                            continue;
                        }
                        orFilterList.add(koValue + SEPARATOR + filterVal + SEPARATOR + ctValue);
                    }
                }
            }
            parseStringValue(orFilterList, RgaDataModel.COMPOUND_FILTERS, filterList, "||");
        } else { // POP_FREQ not empty
            // KT + FILTER + POP_FREQ
            List<String> andQueryList = new ArrayList<>(popFreqQueryList.size());
            if (popFreqQueryList.size() == 2) { // + 2x POP FREQ
                ArrayList<String> popFreqKeys = new ArrayList<>(popFreqQueryList.keySet());
                List<List<String>> sortedPopFreqs = RgaUtils.generateSortedCombinations(popFreqQueryList.get(popFreqKeys.get(0)),
                        popFreqQueryList.get(popFreqKeys.get(1)));
                List<String> orQueryList = new LinkedList<>();
                for (List<String> sortedPopFreq : sortedPopFreqs) {
                    for (String koValue : koValues) {
                        List<String> finalFilterValues = koValue.equals(encodedChString) ? chFilterValues : filterValues;
                        List<String> ctList = koValue.equals(delOverlap) ? INCLUDED_DEL_OVERLAP_PAIR_CTS : ALL_PAIRED_CONSEQUENCE_TYPES;
                        for (String filterVal : finalFilterValues) {
                            // This is how it should be filtered
//                            orQueryList.add(koValue + SEPARATOR + filterVal + SEPARATOR + sortedPopFreq.get(0) + SEPARATOR
//                                    + sortedPopFreq.get(1));
                            if (koValue.equals(delOverlap)) {
                                for (String ctValue : ctList) {
                                    List<String> tmpAndQueryList = new ArrayList<>(2);
                                    tmpAndQueryList.add(koValue + SEPARATOR + filterVal + SEPARATOR + ctValue + SEPARATOR
                                            + sortedPopFreq.get(0));
                                    tmpAndQueryList.add(koValue + SEPARATOR + filterVal + SEPARATOR + ctValue + SEPARATOR
                                            + sortedPopFreq.get(1));
                                    parseStringValue(tmpAndQueryList, "", orQueryList, "&&");
                                }
//                            } else if (koValue.equals(encodedChString)) {
//                                orQueryList.add(koValue + SEPARATOR + filterVal + SEPARATOR + sortedPopFreq.get(0) + SEPARATOR
//                                        + sortedPopFreq.get(1));
//                            } else {
//                                List<String> tmpAndQueryList = new ArrayList<>(2);
//                                tmpAndQueryList.add(koValue + SEPARATOR + filterVal + SEPARATOR + sortedPopFreq.get(0));
//                                tmpAndQueryList.add(koValue + SEPARATOR + filterVal + SEPARATOR + sortedPopFreq.get(1));
//                                parseStringValue(tmpAndQueryList, "", orQueryList, "&&");
//                            }
                            } else {
                                List<String> tmpAndQueryList = new ArrayList<>(2);
                                tmpAndQueryList.add(koValue + SEPARATOR + filterVal + SEPARATOR + sortedPopFreq.get(0));
                                tmpAndQueryList.add(koValue + SEPARATOR + filterVal + SEPARATOR + sortedPopFreq.get(1));
                                parseStringValue(tmpAndQueryList, "", orQueryList, "&&");

                                if (koValue.equals(encodedChString)) {
                                    orQueryList.add(koValue + SEPARATOR + filterVal + SEPARATOR + sortedPopFreq.get(0) + SEPARATOR
                                            + sortedPopFreq.get(1));
                                }
                            }
                        }
                    }
                }
                parseStringValue(orQueryList, "", andQueryList, "||");
            } else { // + 1x POP FREQ
                for (List<String> tmpPopFreqList : popFreqQueryList.values()) {
                    List<String> orQueryList = new LinkedList<>();
                    for (String popFreq : tmpPopFreqList) {
                        for (String koValue : koValues) {
                            List<String> ctList = koValue.equals(delOverlap) ? INCLUDED_DEL_OVERLAP_CONSEQUENCE_TYPES
                                    : ALL_CONSEQUENCE_TYPES;
                            List<String> finalFilterValues = koValue.equals(encodedChString) ? chFilterValues : filterValues;
                            for (String filterVal : finalFilterValues) {
                                // This is how it should be filtered
//                                orQueryList.add(koValue + SEPARATOR + filterVal + SEPARATOR + popFreq);
                                for (String ctValue : ctList) {
                                    orQueryList.add(koValue + SEPARATOR + filterVal + SEPARATOR + ctValue + SEPARATOR + popFreq);
                                }
                            }
                        }
                    }
                    parseStringValue(orQueryList, "", andQueryList, "||");
                }
            }
            parseStringValue(andQueryList, RgaDataModel.COMPOUND_FILTERS, filterList, "&&");
        }

    }

    public static List<String> generateSortedCombinations(List<String> list) {
        if (CollectionUtils.isEmpty(list)) {
            return list;
        }
        Set<String> results = new HashSet<>();
        for (String term1 : list) {
            for (String term2 : list) {
                if (StringUtils.compare(term1, term2) <= 0) {
                    results.add(term1 + SEPARATOR + term2);
                } else {
                    results.add(term2 + SEPARATOR + term1);
                }
            }
        }
        return new ArrayList<>(results);
    }

    private void parseStringValue(Query query, RgaQueryParams queryParam, String storageKey, List<String> filterList) {
        if (StringUtils.isNotEmpty(query.getString(queryParam.key()))) {
            String operator = "||";
            List<String> values = query.getAsStringList(queryParam.key());
            if (values.size() == 1) {
                List<String> andValues = query.getAsStringList(queryParam.key(), ";");
                if (andValues.size() > 1) {
                    values = andValues;
                    operator = "&&";
                }
            }
            List<String> escapedValues = escapeValues(values);
            parseStringValue(escapedValues, storageKey, filterList, operator);
        }
    }

    private void parseStringValue(List<String> values, String storageKey, List<String> filterList, String opSeparator) {
        String separator = " " + opSeparator + " ";

        if (!values.isEmpty()) {
            String value = values.size() == 1 ? values.get(0) : "( " + StringUtils.join(values, separator) + " )";
            if (StringUtils.isNotEmpty(storageKey)) {
                filterList.add(storageKey + ":" + value);
            } else {
                filterList.add(value);
            }
        }
    }

    private List<String> escapeValues(List<String> values) {
        List<String> result = new ArrayList<>(values.size());
        for (String value : values) {
            result.add(value.replace(":", "\\:"));
        }
        return result;
    }

    public String parseFacet(String facetQuery) {
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
