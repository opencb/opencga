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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrException;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.solr.FacetQueryParser;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.KeyOpValue;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.Values;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;
import org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;
import static org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter.*;
import static org.opencb.opencga.storage.core.variant.search.VariantSearchUtils.FIELD_SEPARATOR;

/**
 * Created by imedina on 18/11/16.
 * Created by jtarraga on 18/11/16.
 * Created by wasim on 18/11/16.
 */
public class SolrQueryParser {

    private final VariantStorageMetadataManager variantStorageMetadataManager;
    private final boolean functionQueryStats;
    private final String statsCollectionName;

    private static Map<String, String> includeMap;

    private static Map<String, Integer> chromosomeMap;
    public static final String CHROM_DENSITY = "chromDensity";

    private static final Pattern STUDY_PATTERN = Pattern.compile("^([^=<>!]+):([^=<>!]+)(!=?|<=?|>=?|<<=?|>>=?|==?|=?)([^=<>!]+.*)$");
    private static final Pattern SCORE_PATTERN = Pattern.compile("^([^=<>!]+)(!=?|<=?|>=?|<<=?|>>=?|==?|=?)([^=<>!]+.*)$");
    private static final Pattern NUMERIC_PATTERN = Pattern.compile("(!=?|<=?|>=?|=?)([^=<>!]+.*)$");

    private static final Pattern FACET_RANGE_PATTERN = Pattern.compile("([_a-zA-Z]+)\\[([_a-zA-Z]+):([.a-zA-Z0-9]+)\\]:([.0-9]+)$");
    private static final Pattern FACET_RANGE_STUDY_PATTERN = Pattern.compile("([_a-zA-Z]+)\\[([_a-zA-Z0-9]+):([_a-zA-Z0-9]+):"
            + "([.a-zA-Z0-9]+)\\]:([.0-9]+)$");
    private static final Pattern FACET_FUNCTION_STUDY_PATTERN = Pattern.compile("([_a-zA-Z]+)\\(([a-zA-Z]+)\\[([_a-zA-Z0-9]+):"
            + "([_a-zA-Z0-9]+)\\]\\)$");

    protected static Logger logger = LoggerFactory.getLogger(SolrQueryParser.class);

    static {
        includeMap = new HashMap<>();

        includeMap.put("id", "id,variantId,attr_id");
        includeMap.put("chromosome", "chromosome");
        includeMap.put("start", "start");
        includeMap.put("end", "end");
        includeMap.put("type", "type");

        includeMap.put("studies", "studies,altStats_*,passStats_*,score_*");
        includeMap.put("studies.stats", "studies,altStats_*,passStats_*");
        includeMap.put("studies.scores", "studies,score_*");

        includeMap.put("annotation", "genes,soAcc,geneToSoAcc,biotypes,sift,siftDesc,polyphen,polyphenDesc,popFreq_*,"
                + "xrefs,phastCons,phylop,gerp,caddRaw,caddScaled,traits,other");
        includeMap.put("annotation.consequenceTypes", "genes,soAcc,geneToSoAcc,biotypes,sift,siftDesc,polyphen,"
                + "polyphenDesc,other");
        includeMap.put("annotation.populationFrequencies", "popFreq_*");
        includeMap.put("annotation.xrefs", "xrefs");
        includeMap.put("annotation.conservation", "phastCons,phylop,gerp");
        includeMap.put("annotation.functionalScore", "caddRaw,caddScaled");
        includeMap.put("annotation.traitAssociation", "traits");

        initChromosomeMap();
    }

    public SolrQueryParser(VariantStorageMetadataManager variantStorageMetadataManager) {
        this.variantStorageMetadataManager = variantStorageMetadataManager;
        this.statsCollectionName = null;
        this.functionQueryStats = true;
    }

    public SolrQueryParser(VariantStorageMetadataManager variantStorageMetadataManager, String statsCollectionName) {
        this.variantStorageMetadataManager = variantStorageMetadataManager;
        this.statsCollectionName = statsCollectionName;
        this.functionQueryStats = true;
    }

    public SolrQueryParser(VariantStorageMetadataManager variantStorageMetadataManager, boolean functionQueryStats) {
        this.variantStorageMetadataManager = variantStorageMetadataManager;
        this.functionQueryStats = functionQueryStats;
        this.statsCollectionName = null;
    }

    /**
     * Create a SolrQuery object from Query and QueryOptions.
     *
     * @param query         Query
     * @param queryOptions  Query Options
     * @return              SolrQuery
     */
    public SolrQuery parse(Query query, QueryOptions queryOptions) {
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
            // TODO: Use VariantField
            // Get the correct includes
            String[] includes;
            if (queryOptions.containsKey(QueryOptions.INCLUDE)) {
                includes = solrIncludeFields(queryOptions.getAsStringList(QueryOptions.INCLUDE));
            } else {
                if (queryOptions.containsKey(QueryOptions.EXCLUDE)) {
                    includes = getSolrIncludeFromExclude(queryOptions.getAsStringList(QueryOptions.EXCLUDE));
                } else {
                    // We want all possible fields
                    includes = getSolrIncludeFromExclude(Collections.emptyList());
                }
            }
            includes = ArrayUtils.removeAllOccurences(includes, "release");
            includes = includeFieldsWithMandatory(includes);
            solrQuery.setFields(includes);

            // Add Solr fields from the variant includes
            List<String> solrFieldsToInclude = getSolrFieldsFromVariantIncludes(query, queryOptions);
            for (String solrField : solrFieldsToInclude) {
                solrQuery.addField(solrField);
            }

            if (queryOptions.containsKey(QueryOptions.LIMIT)) {
                solrQuery.setRows(queryOptions.getInt(QueryOptions.LIMIT));
            }

            if (queryOptions.containsKey(QueryOptions.SKIP)) {
                solrQuery.setStart(queryOptions.getInt(QueryOptions.SKIP));
            }

            solrQuery.addSort(new SolrQuery.SortClause("id", SolrQuery.ORDER.asc));
        }

        //-------------------------------------
        // Query processing
        //-------------------------------------

        // OR conditions
        // create a list for xrefs (without genes), genes, regions and cts
        // the function classifyIds function differentiates xrefs from genes
        String geneFilter = parseGenomicFilter(query);
        if (StringUtils.isNotEmpty(geneFilter)) {
            filterList.add(geneFilter);
        }

        // now we continue with the other AND conditions...
        // Study (study)
        @Deprecated
        boolean studiesOr = false;
        StudyMetadata defaultStudy = VariantQueryParser.getDefaultStudy(query, variantStorageMetadataManager);
        String defaultStudyName = (defaultStudy == null)
                ? null
                : defaultStudy.getName();
        String key = STUDY.key();
        if (isValidParam(query, STUDY)) {
            String value = query.getString(key);
            QueryOperation op = checkOperator(value);
            Set<Integer> studyIds = new HashSet<>(variantStorageMetadataManager.getStudyIds(splitValue(value, op)));
            List<String> studyNames = new ArrayList<>(studyIds.size());
            Map<String, Integer> map = variantStorageMetadataManager.getStudies(null);
            if (map != null && map.size() > 1) {
                map.forEach((name, id) -> {
                    if (studyIds.contains(id)) {
                        studyNames.add(studyIdToSearchModel(name));
                    }
                });

                if (op == null || op == QueryOperation.OR) {
                    filterList.add(parseCategoryTermValue("studies", StringUtils.join(studyNames, ",")));
                    studiesOr = true;
                } else {
                    filterList.add(parseCategoryTermValue("studies", StringUtils.join(studyNames, ";")));
                }
            }
        }

        // type
        key = TYPE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parseCategoryTermValue("type", query.getString(key)));
        }

        // protein-substitution
        key = ANNOT_PROTEIN_SUBSTITUTION.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            try {
                filterList.add(parseScoreValue(ANNOT_PROTEIN_SUBSTITUTION, query.getString(key)));
            } catch (Exception e) {
                throw VariantQueryException.malformedParam(ANNOT_PROTEIN_SUBSTITUTION, query.getString(key));
            }
        }

        // conservation
        key = ANNOT_CONSERVATION.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            try {
                filterList.add(parseScoreValue(ANNOT_CONSERVATION, query.getString(key)));
            } catch (Exception e) {
                throw VariantQueryException.malformedParam(ANNOT_CONSERVATION, query.getString(key));
            }
        }

        // cadd, functional score
        key = ANNOT_FUNCTIONAL_SCORE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            try {
                filterList.add(parseScoreValue(ANNOT_FUNCTIONAL_SCORE, query.getString(key)));
            } catch (Exception e) {
                throw VariantQueryException.malformedParam(ANNOT_FUNCTIONAL_SCORE, query.getString(key));
            }
        }

        // ALT population frequency
        // in the query: 1000G:CEU<=0.0053191,1000G:CLM>0.0125319"
        // in the search model: "popFreq__1000G__CEU":0.0053191,popFreq__1000G__CLM">0.0125319"
        key = ANNOT_POPULATION_ALTERNATE_FREQUENCY.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parsePopFreqValue(ANNOT_POPULATION_ALTERNATE_FREQUENCY, "popFreq", query.getString(key), "ALT", null, null));
        }

        // MAF population frequency
        // in the search model: "popFreq__1000G__CLM":0.005319148767739534
        key = ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parsePopFreqValue(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY, "popFreq", query.getString(key), "MAF", null, null));
        }

        // REF population frequency
        // in the search model: "popFreq__1000G__CLM":0.005319148767739534
        key = ANNOT_POPULATION_REFERENCE_FREQUENCY.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parsePopFreqValue(ANNOT_POPULATION_REFERENCE_FREQUENCY, "popFreq", query.getString(key), "REF", null, null));
        }

        // Stats ALT
        // In the model: "altStats__1000G__ALL"=0.02
        key = STATS_ALT.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parsePopFreqValue(STATS_ALT, "altStats", query.getString(key), "ALT", defaultStudyName,
                    query.getString(STUDY.key())));
        }

        // Stats MAF
        // In the model: "altStats__1000G__ALL"=0.02
        key = STATS_MAF.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parsePopFreqValue(STATS_MAF, "altStats", query.getString(key), "MAF", defaultStudyName,
                    query.getString(STUDY.key())));
        }

        // Stats REF
        // In the model: "altStats__1000G__ALL"=0.02
        key = STATS_REF.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parsePopFreqValue(STATS_REF, "altStats", query.getString(key), "REF", defaultStudyName,
                    query.getString(STUDY.key())));
        }

        // Stats PASS filter
        // In the model: "passStats__1000G__ALL">0.2
        key = STATS_PASS_FREQ.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parsePopFreqValue(STATS_PASS_FREQ, "passStats", query.getString(key), "", defaultStudyName,
                    query.getString(STUDY.key())));
        }

        // Variant score
        // In the model: "score__1000G__gwas1>3.12"
        //               "scorePValue__1000G__gwas1<=0.002"
        // where "1000G" is the study ID, and "gwas1" is the score ID
        key = SCORE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.addAll(parseVariantScores(query.getString(key), defaultStudyName));
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
        key = ANNOT_GENE_TRAIT_ID.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parseCategoryTermValue("traits", query.getString(key)));
        }

        // Gene Trait Name
        key = ANNOT_GENE_TRAIT_NAME.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            String traits = parseCategoryTermValue("traits", query.getString(key));
            filterList.add(traits);
        }

        // hpo
        key = ANNOT_HPO.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parseCategoryTermValue("traits", query.getString(key)));
        }

        // traits
        key = ANNOT_TRAIT.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parseCategoryTermValue("traits", query.getString(key)));
        }

        // protein keywords
        key = ANNOT_PROTEIN_KEYWORD.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parseCategoryTermValue("traits", query.getString(key)));
        }

        // clinical significance
        for (List<String> clinicalCombinations : VariantQueryParser.parseClinicalCombination(query)) {
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            for (int i = 0; i < clinicalCombinations.size(); i++) {
                if (i > 0) {
                    sb.append(" OR ");
                }
                sb.append("clinicalSig:\"").append(clinicalCombinations.get(i)).append("\"");
            }
            sb.append(")");
            filterList.add(sb.toString());
        }

        // Create Solr query, adding filter queries and fields to show
        solrQuery.setQuery("*:*");
        filterList.forEach(solrQuery::addFilterQuery);

        logger.debug("----------------------");
        logger.debug("query     : " + printQuery(query));
        logger.debug("solrQuery : " + solrQuery);
        return solrQuery;
    }

    private List<String> parseVariantScores(String queryValue, String defaultStudyName) {
        List<String> filters = new ArrayList<>();

        String[] scores = queryValue.split("[,;]");
        QueryOperation queryOp = parseOrAndFilter(SCORE, queryValue);

        if (queryOp == QueryOperation.OR) {
            // OR
            StringBuilder filter = new StringBuilder();
            for (int i = 0; i < scores.length; i++) {
                filter.append(parseVariantScore(scores[i], defaultStudyName));
                if (i < scores.length - 1) {
                    filter.append(" OR ");
                }
            }
            filters.add(filter.toString());
        } else {
            // AND
            for (String score : scores) {
                filters.add(parseVariantScore(score, defaultStudyName));
            }
        }
        return filters;
    }

    private String parseVariantScore(String score, String defaultStudyName) {
        KeyOpValue<String, String> keyOpValue = parseKeyOpValue(score);
        if (StringUtils.isEmpty(keyOpValue.getKey())) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid Solr variant score query: " + score);
        }

        String scoreField = "score";
        if (keyOpValue.getKey().endsWith(":pvalue")) {
            scoreField = "scorePValue";
            keyOpValue.setKey(StringUtils.removeEnd(keyOpValue.getKey(), ":pvalue"));
        } else if (keyOpValue.getKey().endsWith(":score")) {
            keyOpValue.setKey(StringUtils.removeEnd(keyOpValue.getKey(), ":score"));
        }

        String[] fields = splitStudyResource(keyOpValue.getKey());
        String study;
        String scoreName;
        if (fields.length == 1) {
            checkMissingStudy(defaultStudyName, "variant score", score);
            study = studyIdToSearchModel(defaultStudyName);
            scoreName = fields[0];
        } else {
            study = studyIdToSearchModel(fields[0]);
            scoreName = fields[1];
        }
        String name = scoreField + FIELD_SEPARATOR + study + FIELD_SEPARATOR + scoreName;
        return getRange(name, keyOpValue.getOp(), keyOpValue.getValue());
    }

    private void checkMissingStudy(String studyId, String msg, String value) {
        // Missing study
        if (StringUtils.isEmpty(studyId)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing study in Solr " + msg + " query: " + value);
        }
    }

    private String parseGenomicFilter(Query query) {
        List<Region> regions = new ArrayList<>();
        List<String> xrefs = new ArrayList<>();
        List<String> genes = new ArrayList<>();
        List<String> biotypes = new ArrayList<>();
        List<String> consequenceTypes = new ArrayList<>();
        List<String> flags = new ArrayList<>();

        ParsedVariantQuery.VariantQueryXref variantQueryXref = VariantQueryParser.parseXrefs(query);
        genes.addAll(variantQueryXref.getGenes());
        xrefs.addAll(variantQueryXref.getIds());
        xrefs.addAll(variantQueryXref.getOtherXrefs());
        xrefs.addAll(variantQueryXref.getVariants().stream()
                .map(VariantSearchToVariantConverter::getVariantId)
                .collect(Collectors.toList()));

        // Regions
        if (StringUtils.isNotEmpty(query.getString(REGION.key()))) {
            regions = Region.parseRegions(query.getString(REGION.key()), true);
        }

        // Biotypes
        if (StringUtils.isNotEmpty(query.getString(ANNOT_BIOTYPE.key()))) {
            biotypes = Arrays.asList(query.getString(ANNOT_BIOTYPE.key()).split("[,;]"));
        }

        // Consequence types (cts)
        String ctLogicalOperator = " OR ";
        if (StringUtils.isNotEmpty(query.getString(ANNOT_CONSEQUENCE_TYPE.key(), ""))) {
            consequenceTypes = parseConsequenceTypes(Arrays.asList(query.getString(ANNOT_CONSEQUENCE_TYPE.key()).split("[,;]")));
            if (query.getString(ANNOT_CONSEQUENCE_TYPE.key()).contains(";")) {
                ctLogicalOperator = " AND ";
                // TODO This must be removed as soon as we have the Query procesing in use
                if (query.getString(ANNOT_CONSEQUENCE_TYPE.key()).contains(",")) {
                    ctLogicalOperator = " OR ";
                    logger.info("Misuse of consequence type values by mixing ';' and ',': using ',' as default.");
                }
            }
        }
        List<String> cts = consequenceTypes.stream().map(ct -> String.valueOf(parseConsequenceType(ct))).collect(Collectors.toList());

        // Flags
        if (StringUtils.isNotEmpty(query.getString(ANNOT_TRANSCRIPT_FLAG.key()))) {
            flags = Arrays.asList(query.getString(ANNOT_TRANSCRIPT_FLAG.key()).split("[,;]"));
        }

        String regionXrefPart = "";
        if (CollectionUtils.isNotEmpty(regions) || CollectionUtils.isNotEmpty(xrefs)) {
            regionXrefPart = buildXrefOrGeneOrRegion(xrefs, null, regions);
        }

        String combinationPart;
        String geneCombinationPart;
        String onlyCobinationPart;

        BiotypeConsquenceTypeFlagCombination biotypeConsquenceTypeFlagCombination = BiotypeConsquenceTypeFlagCombination.fromQuery(query);
        switch (biotypeConsquenceTypeFlagCombination) {
            case BIOTYPE:
                combinationPart = parseCategoryTermValue("biotypes", query.getString(ANNOT_BIOTYPE.key()));
                geneCombinationPart = buildFrom(genes, biotypes);
                onlyCobinationPart = combinationPart;
                break;
            case BIOTYPE_CT: // biotype __ consequence type
                combinationPart = buildFrom(biotypes, cts);
                geneCombinationPart = buildFrom(genes, biotypes, cts);
                onlyCobinationPart = combinationPart;
                break;
            case BIOTYPE_CT_FLAG: // biotype __ consequence type __ flag
                combinationPart = buildFrom(biotypes, cts) + " AND " + buildFrom(cts, flags);
                geneCombinationPart = buildFrom(genes, biotypes, cts, flags);
                onlyCobinationPart = combinationPart;
                break;
            case CT:
                combinationPart = buildConsequenceTypeOrAnd(consequenceTypes, ctLogicalOperator);
                geneCombinationPart = buildFrom(genes, cts);
                onlyCobinationPart = combinationPart;
                break;
            case CT_FLAG:    //   consequence type __ flag
                combinationPart = buildFrom(cts, flags);
                geneCombinationPart = buildFrom(genes, cts, flags);
                onlyCobinationPart = combinationPart;
                break;
            case FLAG:
                combinationPart = parseCategoryTermValue("other",  "TRANS*" + query.getString(ANNOT_TRANSCRIPT_FLAG.key()));
                if (CollectionUtils.isNotEmpty(genes)) {
                    geneCombinationPart = "(" + buildXrefOrGeneOrRegion(null, genes, null) + ") AND (" + combinationPart + ")";
                } else {
                    geneCombinationPart = "";
                    //geneCombinationPart = "(" + combinationPart + ")";
                }
                onlyCobinationPart = combinationPart;
                break;
            case BIOTYPE_FLAG:
                combinationPart = parseCategoryTermValue("biotypes", query.getString(ANNOT_BIOTYPE.key()));
                geneCombinationPart = buildFrom(genes, biotypes);
                onlyCobinationPart = parseCategoryTermValue("biotypes", query.getString(ANNOT_BIOTYPE.key())) + " AND "
                        + parseCategoryTermValue("other",  "TRANS*" + query.getString(ANNOT_TRANSCRIPT_FLAG.key()));
                break;
            case NONE:
                return buildXrefOrGeneOrRegion(xrefs, genes, regions);
            default:
                throw new VariantQueryException("Not supported combination: " + query.toString());
        }

        if (StringUtils.isEmpty(regionXrefPart)) {
            if (StringUtils.isEmpty(geneCombinationPart)) {
                return "(" + onlyCobinationPart + ")";
            } else {
                return "(" + geneCombinationPart + ")";
            }
        } else {
            if (StringUtils.isEmpty(geneCombinationPart)) {
                return "((" + regionXrefPart + ") AND (" + combinationPart + "))";
            } else {
                return "((" + regionXrefPart + ") AND (" + combinationPart + ")) OR (" + geneCombinationPart + ")";
            }
        }
    }

    private String parseFacet(String facetQuery) {
        StringBuilder sb = new StringBuilder();
        List<String> facetList = new ArrayList<>();
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
        if (facet.contains(CHROM_DENSITY)) {
            return parseChromDensity(facet);
        } else if (facet.contains(ANNOT_FUNCTIONAL_SCORE.key())) {
            return parseFacet(facet, ANNOT_FUNCTIONAL_SCORE.key());
        } else if (facet.contains(ANNOT_CONSERVATION.key())) {
            return parseFacet(facet, ANNOT_CONSERVATION.key());
        } else if (facet.contains(ANNOT_PROTEIN_SUBSTITUTION.key())) {
            return parseFacet(facet, ANNOT_PROTEIN_SUBSTITUTION.key());
        } else if (facet.contains(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key())) {
            return parseFacetWithStudy(facet, "popFreq");
        } else if (facet.contains(STATS_ALT.key())) {
            return parseFacetWithStudy(facet, "altStats");
        } else if (facet.contains(SCORE.key())) {
            return parseFacetWithStudy(facet, SCORE.key());
        } else {
            return facet;
        }
    }

    private String parseFacet(String facet, String categoryName) {
        if (facet.contains("(")) {
            // Aggregation function
            return facet.replace(categoryName, "").replace("[", "").replace("]", "");
        } else if (facet.contains("..")) {
            // Range
            Matcher matcher = FACET_RANGE_PATTERN.matcher(facet);
            if (matcher.find()) {
                return matcher.group(2) + "[" + matcher.group(3) + "]:" + matcher.group(4);
            } else {
                throw VariantQueryException.malformedParam(categoryName, facet, "Invalid syntax for facet range.");
            }
        }
        // Nothing to do
        return facet;
    }

    private String parseFacetWithStudy(String facet, String categoryName) {
        if (facet.contains("(")) {
            // Aggregation function
            Matcher matcher = FACET_FUNCTION_STUDY_PATTERN.matcher(facet);
            if (matcher.find()) {
                return matcher.group(1) + "(" + categoryName + FIELD_SEPARATOR + matcher.group(3) + FIELD_SEPARATOR + matcher.group(4)
                        + ")";
            } else {
                throw VariantQueryException.malformedParam(categoryName, facet, "Invalid syntax for facet function.");
            }
        } else if (facet.contains("..")) {
            // Range
            Matcher matcher = FACET_RANGE_STUDY_PATTERN.matcher(facet);
            if (matcher.find()) {
                return categoryName + FIELD_SEPARATOR + matcher.group(2) + FIELD_SEPARATOR + matcher.group(3) + "[" + matcher.group(4)
                        + "]:" + matcher.group(5);
            } else {
                throw VariantQueryException.malformedParam(categoryName, facet, "Invalid syntax for facet range.");
            }
        }
        // Nothing to do
        return facet;
    }

    private String parseChromDensity(String facet) {
        // Categorical...
        Matcher matcher = FacetQueryParser.CATEGORICAL_PATTERN.matcher(facet);
        if (matcher.find()) {
            if (matcher.group(1).equals(CHROM_DENSITY)) {
                // Step management
                int step = 1000000;
                if (StringUtils.isNotEmpty(matcher.group(3))) {
                    step = Integer.parseInt(matcher.group(3).substring(1));
                }
                int maxLength = 0;
                // Include management
                List<String> chromList;
                String include = matcher.group(2);
                if (StringUtils.isNotEmpty(include)) {
                    chromList = new ArrayList<>();
                    include = include.replace("]", "").replace("[", "");
                    for (String value : include.split(FacetQueryParser.INCLUDE_SEPARATOR)) {
                        chromList.add(value);
                    }
                } else {
                    chromList = new ArrayList<>(chromosomeMap.keySet());
                }

                List<String> chromQueryList = new ArrayList<>();
                for (String chrom : chromList) {
                    if (chromosomeMap.get(chrom) > maxLength) {
                        maxLength = chromosomeMap.get(chrom);
                    }
                    chromQueryList.add("chromosome:" + chrom);
                }
                return "start[1.." + maxLength + "]:" + step + ":chromDensity" + FacetQueryParser.LABEL_SEPARATOR + "chromosome:"
                        + StringUtils.join(chromQueryList, " OR ");
            } else {
                throw VariantQueryException.malformedParam(CHROM_DENSITY, facet, "Invalid syntax.");
            }
        } else {
            throw VariantQueryException.malformedParam(CHROM_DENSITY, facet, "Invalid syntax.");
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
        return parseCategoryTermValue(name, value, "", false);
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
        return parseCategoryTermValue(name, value, "", partialSearch);
    }

    public String parseCategoryTermValue(String name, String val, String valuePrefix, boolean partialSearch) {
        StringBuilder filter = new StringBuilder();
        if (StringUtils.isNotEmpty(val)) {
            String negation;

            Values<String> values = splitValues(val).map(s -> StringUtils.remove(s, '"'));
            QueryOperation queryOperation = parseOrAndFilter(name, val);
            String logicalComparator = queryOperation == QueryOperation.OR ? " OR " : " AND ";
            String wildcard = partialSearch ? "*" : "";

            if (values.size() == 1) {
                negation = "";
                String value = values.get(0);
                if (isNegated(value)) {
                    negation = "-";
                    value = removeNegation(value);
                }
                filter.append(negation).append(name).append(":\"").append(valuePrefix).append(wildcard).append(value)
                        .append(wildcard).append("\"");
            } else {
                filter.append("(");
                negation = "";
                String value = values.get(0);
                if (isNegated(value)) {
                    negation = "-";
                    value = removeNegation(value);
                }
                filter.append(negation).append(name).append(":\"").append(valuePrefix).append(wildcard)
                        .append(value).append(wildcard).append("\"");
                for (int i = 1; i < values.size(); i++) {
                    filter.append(logicalComparator);
                    negation = "";
                    value = values.get(i);
                    if (isNegated(value)) {
                        negation = "-";
                        value = removeNegation(value);
                    }
                    filter.append(negation).append(name).append(":\"").append(valuePrefix).append(wildcard)
                            .append(value).append(wildcard).append("\"");
                }
                filter.append(")");
            }
        }
        return filter.toString();
    }

    public String parseNumericValue(String name, String value) {
        StringBuilder filter = new StringBuilder();
        Matcher matcher = NUMERIC_PATTERN.matcher(value);
        if (matcher.find()) {
            // concat expression, e.g.: value:[0 TO 12]
            filter.append(getRange(name, matcher.group(1), matcher.group(2)));
        } else {
            logger.debug("Invalid expression: {}", value);
            throw new IllegalArgumentException("Invalid expression " +  value);
        }
        return filter.toString();
    }

    /**
     * Parse string values, e.g.: polyPhen, gerp, caddRaw,... This function takes into account multiple values and
     * the separator between them can be:
     *     "," to apply a "OR condition"
     *     ";" to apply a "AND condition"
     *
     * @param param        VariantQueryParam, e.g.: conservation, functionalScore, proteinSubstitution
     * @param value        Field value
     * @return             The string with the boolean conditions
     */
    public String parseScoreValue(VariantQueryParam param, String value) {
        // In Solr, range queries can be inclusive or exclusive of the upper and lower bounds:
        //    - Inclusive range queries are denoted by square brackets.
        //    - Exclusive range queries are denoted by curly brackets.
        String name = param.key();
        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotEmpty(value)) {
            QueryOperation queryOperation = parseOrAndFilter(name, value);
            String logicalComparator = queryOperation == QueryOperation.OR ? " OR " : " AND ";

            Matcher matcher;
            String[] values = value.split("[,;]");
            if (values.length == 1) {
                matcher = SCORE_PATTERN.matcher(value);
                if (matcher.find()) {
                    // concat expression, e.g.: value:[0 TO 12]
                    checkRangeParams(param, matcher.group(1), matcher.group(3));
                    sb.append(getRange(matcher.group(1), matcher.group(2), matcher.group(3)));
                } else {
                    throw new IllegalArgumentException("Invalid expression " +  value);
                }
            } else {
                List<String> list = new ArrayList<>(values.length);
                String prevName = null;
                String prevOp = null;
                for (String v : values) {
                    matcher = SCORE_PATTERN.matcher(v);
                    if (matcher.find()) {
                        // concat expression, e.g.: value:[0 TO 12]
                        String filterName = matcher.group(1);
                        String filterOp = matcher.group(2);
                        String filterValue = matcher.group(3);
                        if (StringUtils.isEmpty(filterOp)) {
                            filterName = prevName;
                            filterOp = prevOp;
                            filterValue = v;
                        } else {
                            prevName = filterName;
                            prevOp = filterOp;
                        }
                        if (StringUtils.isEmpty(filterName)) {
                            throw VariantQueryException.malformedParam(param, value);
                        }

                        checkRangeParams(param, filterName, filterValue);
                        list.add(getRange(filterName, filterOp, filterValue));
                    } else {
                        throw new IllegalArgumentException("Invalid expression " +  value);
                    }
                }
                sb.append("(").append(StringUtils.join(list, logicalComparator)).append(")");
            }
        }
        return sb.toString();
    }

    private void checkRangeParams(VariantQueryParam param, String source, String value) {
        if (param == ANNOT_PROTEIN_SUBSTITUTION) {
            if (!"polyphen".equals(source) && !"sift".equals(source)) {
                throw new IllegalArgumentException("Invalid source '" + source + "' for " + ANNOT_PROTEIN_SUBSTITUTION.key() + ", valid "
                        + "values are: polyphen, sift");
            }
        } else if (param == ANNOT_FUNCTIONAL_SCORE) {
            if (!"cadd_scaled".equals(source) && !"cadd_raw".equals(source)) {
                throw new IllegalArgumentException("Invalid source '" + source + "' for " + ANNOT_PROTEIN_SUBSTITUTION.key() + ", valid "
                        + "values are: cadd_scaled, cadd_raw");
            }
        } else if (param == ANNOT_CONSERVATION) {
            if (!"phastCons".equals(source) && !"phylop".equals(source) && !"gerp".equals(source)) {
                throw new IllegalArgumentException("Invalid source '" + source + "' for " + ANNOT_PROTEIN_SUBSTITUTION.key() + ", valid "
                        + "values are: phastCons, phylop, gerp");
            }
        }

        if (param != ANNOT_PROTEIN_SUBSTITUTION && !NumberUtils.isNumber(value)) {
            throw new IllegalArgumentException("Invalid expression: value '" +  value + "' must be numeric.");
        }
    }

    /**
     * Parse population/altStats values, e.g.: 1000g:all>0.4 or 1000G:JPN<0.00982. This function takes into account
     * multiple values and the separator between them can be:
     *     "," to apply a "OR condition"
     *     ";" to apply a "AND condition"
     *
     *
     * @param param        Param name
     * @param field        Parameter field: propFreq, altStats or passStats
     * @param value        Filter value, e.g.: 1000G:CEU<=0.0053191,1000G:CLM>0.0125319
     * @param type         Type of frequency: REF, ALT, MAF, empty
     * @param defaultStudy Default study. To be used only if the study is not present.
     * @param studies      True if multiple studies are present joined by , (i.e., OR logical operation), only for STATS
     * @return             The string with the boolean conditions
     */
    private String parsePopFreqValue(VariantQueryParam param, String field, String value, String type, String defaultStudy,
                                     String studies) {
        // In Solr, range queries can be inclusive or exclusive of the upper and lower bounds:
        //    - Inclusive range queries are denoted by square brackets.
        //    - Exclusive range queries are denoted by curly brackets.
        if (StringUtils.isNotEmpty(value)) {
            // FIXME at the higher level
            value = value.replace("<<", "<");
            value = value.replace("<", "<<");

            QueryOperation queryOperation = parseOrAndFilter(param, value);
            String logicalComparator = (queryOperation == QueryOperation.OR) ? " OR " : " AND ";
            List<String> values = splitValue(value, queryOperation);

            // We need to know if
            boolean addOr = true;
            if (field.equals("altStats") || field.equals("passStats")) {
                if (StringUtils.isNotEmpty(studies) || StringUtils.isNotEmpty(defaultStudy)) {
                    Set<String> studiesSet = new HashSet<>();
                    if (defaultStudy != null) {
                        studiesSet.add(defaultStudy);
                    }
                    // Studies...
                    if (studies != null && !studies.contains(",")) {
                        studiesSet.addAll(Arrays.asList(studies.split(";")));
                    }

                    if (studiesSet.size() > 0) {
                        addOr = false;
                        for (String val: values) {
                            String study = splitStudyResource(defaultStudy, val)[0];
                            if (!studiesSet.contains(study)) {
                                addOr = true;
                                break;
                            }
                        }
                    }
                }
            }

            List<String> list = new ArrayList<>(values.size());
            for (String v : values) {
                KeyOpValue<String, String> keyOpValue = parseKeyOpValue(v);
                String studyPop = keyOpValue.getKey();
                String op = keyOpValue.getOp();
                String numValue = keyOpValue.getValue();
                if (studyPop == null) {
                    throw VariantQueryException.malformedParam(param, value);
                }

                String[] studyPopSplit = splitStudyResource(defaultStudy, studyPop);
                String study = studyPopSplit[0];
                String pop = studyPopSplit[1];
                if (StringUtils.isEmpty(study)) {
                    throw VariantQueryException.malformedParam(param, value, "Missing study");
                }

                if (field.equals("popFreq")) {
                    // Solr only stores ALT frequency, we need to calculate the MAF or REF before querying
                    String[] fixedFreqValue = getMafOrRefFrequency(type, op, numValue);
                    op = fixedFreqValue[0];
                    numValue = fixedFreqValue[1];

                    if ((study.equals(ParamConstants.POP_FREQ_1000G) || study.equals("GNOMAD_GENOMES")) && pop.equals("ALL")) {
                        addOr = false;
                    } else {
                        addOr = true;
                    }
                    // concat expression, e.g.: value:[0 TO 12]
                    list.add(getRange(field + FIELD_SEPARATOR + study + FIELD_SEPARATOR, pop, op, numValue, addOr));
                } else if (!functionQueryStats) {
                    // Backwards compatibility for altStats and passStats

                    // Solr only stores ALT frequency, we need to calculate the MAF or REF before querying
                    String[] fixedFreqValue = getMafOrRefFrequency(type, op, numValue);
                    op = fixedFreqValue[0];
                    numValue = fixedFreqValue[1];

                    // concat expression, e.g.: value:[0 TO 12]
                    list.add(getRange(field + FIELD_SEPARATOR + study + FIELD_SEPARATOR, pop, op, numValue, addOr));

                } else {
                    String cohort = pop;
                    int studyId = variantStorageMetadataManager.getStudyId(study);
                    CohortMetadata cohortMetadata = variantStorageMetadataManager.getCohortMetadata(studyId, cohort);
                    if (cohortMetadata == null) {
                        throw VariantQueryException.malformedParam(param, value, "Missing cohort " + cohort + " in study " + study);
                    }

                    if (field.equals("altStats")) {
                        StringBuilder sb = new StringBuilder();

            // See https://solr.apache.org/guide/solr/latest/query-guide/local-params.html#specifying-the-parameter-value-with-the-v-key
                        // The v key is used to specify the value of the parameter. This is required so we can use AND/OR
                        // e.g. {!frange l=0 u=0.1 v='div(altCount, sub(cohortSize * 2 , alleleCountDiff))'}

                        String functionQuery;

                        switch (type) {
                            case "ALT":
                                functionQuery = altFreqF(study, cohortMetadata);
                                break;
                            case "REF":
                                functionQuery = refFreqF(study, cohortMetadata);
                                break;
                            case "MAF":
                                functionQuery = mafFreqF(study, cohortMetadata);
                                break;
                            default:
                                throw new IllegalArgumentException("Invalid type '" + type + "' for field " + field
                                        + ". Valid values are: MAF, REF, ALT.");
                        }

                        sb.append("{!frange ")
                                .append(getFrangeQuery(op, Float.parseFloat(numValue)))
                                .append(" v='").append(functionQuery).append("'}");

                        list.add(sb.toString());
                    } else if (field.equals("passStats")) {
                        // TODO: Implement this method
                        throw new IllegalArgumentException("Unsupported field: " + field
                                + ". Only 'altStats' is supported for stats queries.");
                    } else {
                        throw new IllegalArgumentException("Unknown field: " + field);
                    }
                }
            }

            StringBuilder sb = new StringBuilder();
            if (list.isEmpty()) {
                return "";
            }
            if (list.size() == 1) {
                sb.append(list.get(0));
            } else {
                for (int i = 0; i < list.size(); i++) {
                    String s = list.get(i);
                    if (i != 0) {
                        sb.append(logicalComparator);
                    }
                    sb.append("(").append(s).append(")");
                }
            }
            return sb.toString();
        } else {
            return "";
        }
    }

    /**
     * Obtain a FunctionQuery to calculate the AlleleCount.
     *
     * AlleleCount ~ count of non-missing alleles
     *             ~ expected alleles of diploid GTs - (missing alleles + gaps from non-diploid GTs)
     *
     * AlleleCount = expectedNumAlleles - (missingAlleles + gapAlleles)
     * AlleleCount = numSamples * 2 - (missingAlleles + gapAlleles)
     *
     * @param study          Study Name
     * @param cohortMetadata Cohort Metadata
     * @return function query to calculate the AlleleCount
     */
    private static String alleleCountF(String study, CohortMetadata cohortMetadata) {
        String studySearchModel = studyIdToSearchModel(study);
        double expectedNumAlleles = cohortMetadata.getSamples().size() * 2;
        String alleleMissOrGap = buildStatsAlleleMissGapCountField(studySearchModel, cohortMetadata.getName());

        return "sub(" + expectedNumAlleles + ", " + alleleMissOrGap + ")";
    }

    /**
     * Obtain a FunctionQuery to calculate the AlternateAlleleFrequency.
     *
     * AlternateAlleleFrequency = alternateAlleles / alelleCount
     *
     * @param study          Study Name
     * @param cohortMetadata Cohort Metadata
     * @return function query to calculate the AlternateAlleleFrequency
     */
    private static String altFreqF(String study, CohortMetadata cohortMetadata) {
        String studySearchModel = studyIdToSearchModel(study);
        String altAlleleCount = buildStatsAltAlleleCountField(studySearchModel, cohortMetadata.getName());

        return "div(" + altAlleleCount + ", " + alleleCountF(study, cohortMetadata) + ")";
    }

    /**
     * Obtain a FunctionQuery to calculate the ReferenceAlleleFrequency.
     *
     * ReferenceAlleleFrequency = referenceAlleles / alleleCount
     * referenceAlleles = alleleCount - (alternateAlleles + otherAlleles)
     * nonRefAlleles = alternateAlleles + otherAlleles == alleleCount - referenceAlleles
     * ReferenceAlleleFrequency = (alleleCount - nonRefAlleles) / alelleCount
     * ReferenceAlleleFrequency = 1 - (nonRefAlleles / alleleCount)
     *
     * @param study          Study Name
     * @param cohortMetadata Cohort Metadata
     * @return function query to calculate the ReferenceAlleleFrequency
     */
    private static String refFreqF(String study, CohortMetadata cohortMetadata) {
        String studySearchModel = studyIdToSearchModel(study);
        String nonRefCount = buildStatsAlleleNonRefCountField(studySearchModel, cohortMetadata.getName());

        return "sub(1, div(" + nonRefCount + ", " + alleleCountF(study, cohortMetadata) + "))";
    }

    /**
     * Obtain a FunctionQuery to calculate the Minor Allele Frequency.
     *
     * MinorAlleleFrequency = min(AlternateAlleleFrequency, ReferenceAlleleFrequency)
     *
     * @param study          Study Name
     * @param cohortMetadata Cohort Metadata
     * @return
     */
    private static String mafFreqF(String study, CohortMetadata cohortMetadata) {
        String studySearchModel = studyIdToSearchModel(study);
        String altFreq = altFreqF(studySearchModel, cohortMetadata);
        String refFreq = refFreqF(studySearchModel, cohortMetadata);
        return "min(" + altFreq + ", " + refFreq + ")";
    }

    /**
     * Translate num operator into a FunctionRangeQuery (frange).
     *
     * @see <a href="https://solr.apache.org/guide/solr/latest/query-guide/other-parsers.html#function-range-query-parser"></a>
     *
     * @param op       Operator, e.g.: <, <=, <<, <<=, >, >=, >>, >>=
     * @param value    Num value
     * @return         String with the frange query parameters
     */
    private static String getFrangeQuery(String op, float value) {
        StringBuilder frangeVars = new StringBuilder();
        switch (op) {
            case "<":
            case "<<":
                // Exclude the upper bound
                frangeVars.append("u=").append(value).append(" incu=false");
                break;
            case "<=":
            case "<<=":
                // Include the upper bound
                frangeVars.append("u=").append(value).append(" incu=true");
                break;
            case ">":
            case ">>":
                // Exclude the lower bound
                frangeVars.append("l=").append(value).append(" incl=false");
                break;
            case ">=":
            case ">>=":
                // Include the lower bound
                frangeVars.append("l=").append(value).append(" incl=true");
                break;
            default:
                throw new IllegalArgumentException("Invalid operator: " + op);
        }
        return frangeVars.toString();
    }

//    private int getNumberOfStudies(List<String> values, VariantQueryParam paramName, String paramValue, String defaultStudy) {
//        Set<String> studies = new HashSet<>();
//
//        for (String v : values) {
//            String[] keyOpValue = splitOperator(v);
//
//            String studyPop = keyOpValue[0];
//            if (studyPop == null) {
//                throw VariantQueryException.malformedParam(paramName, paramValue);
//            }
//
//            String[] studyPopSplit = splitStudyResource(studyPop);
//            String study;
//            if (studyPopSplit.length == 2) {
//                study = VariantSearchToVariantConverter.studyIdToSearchModel(studyPopSplit[0]);
//            } else {
//                if (StringUtils.isEmpty(defaultStudy)) {
//                    throw VariantQueryException.malformedParam(paramName, paramValue, "Missing study");
//                }
//                study = defaultStudy;
//            }
//            studies.add(study);
//        }
//
//        return studies.size();
//    }

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
     * @param name      Parameter name, e.g.: sift, phylop, gerp, caddRaw,...
     * @param op        Operator, e.g.: =, !=, <, <=, <<, <<=, >,...
     * @param value     Parameter value, e.g.: 0.314, tolerated,...
     * @return          Solr query range
     */
    public String getRange(String name, String op, String value) {
        return getRange("", name, op, value);
    }

    /**
     * Build Solr query range, e.g.: query range [0 TO 23}.
     *
     * @param prefix    Prefix, e.g.: popFreq__study__cohort, altStats__ or null
     * @param name      Parameter name, e.g.: sift, phylop, gerp, caddRaw,...
     * @param op        Operator, e.g.: =, !=, <, <=, <<, <<=, >,...
     * @param value     Parameter value, e.g.: 0.314, tolerated,...
     * @return          Solr query range
     */
    public String getRange(String prefix, String name, String op, String value) {
        return getRange(prefix, name, op, value, true);
    }

    public String getRange(String prefix, String name, String op, String value, boolean addOr) {
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
                        .append(MISSING_VALUE).append(" TO ").append(value).append("}");
                break;
            case "<=":
                sb.append(prefix).append(getSolrFieldName(name)).append(":{")
                        .append(MISSING_VALUE).append(" TO ").append(value).append("]");
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
                if (StringUtils.isNotEmpty(prefix) && (prefix.startsWith("popFreq_") || prefix.startsWith("altStats_")
                        || prefix.startsWith("passStats_"))) {
                    sb.append("(");
                    sb.append(prefix).append(getSolrFieldName(name)).append(":[0 TO ").append(value).append(rightCloseOperator);
                    if (addOr) {
                        sb.append(" OR ");
                        sb.append("(* -").append(prefix).append(getSolrFieldName(name)).append(":*)");
                    }
                    sb.append(")");
                } else {
                    sb.append(prefix).append(getSolrFieldName(name)).append(":[")
                            .append(MISSING_VALUE).append(" TO ").append(value).append(rightCloseOperator);
                }
                break;
            case ">>":
            case ">>=":
                String leftCloseOperator = (">>").equals(op) ? "{" : "[";
                sb.append("(");
                if (StringUtils.isNotEmpty(prefix) && (prefix.startsWith("popFreq_") || prefix.startsWith("altStats_")
                        || prefix.startsWith("passStats_"))) {
                    sb.append(prefix).append(getSolrFieldName(name)).append(":").append(leftCloseOperator).append(value).append(" TO *]");
                    if (addOr) {
                        sb.append(" OR ");
                        sb.append("(* -").append(prefix).append(getSolrFieldName(name)).append(":*)");
                    }
                } else {
                    // attention: negative values must be escaped
                    sb.append(prefix).append(getSolrFieldName(name)).append(":").append(leftCloseOperator).append(value).append(" TO *]");
                    sb.append(" OR ");
                    sb.append(prefix).append(getSolrFieldName(name)).append(":\\").append(MISSING_VALUE);
                }
                sb.append(")");
                break;
            default:
                throw new VariantQueryException("Unknown operator " + op);
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
        if (CollectionUtils.isNotEmpty(regions)) {
            for (Region region : regions) {
                if (StringUtils.isNotEmpty(region.getChromosome())) {
                    // Clean chromosome
                    String chrom = region.getChromosome();
                    chrom = chrom.replace("chrom", "");
                    chrom = chrom.replace("chrm", "");
                    chrom = chrom.replace("chr", "");
                    chrom = chrom.replace("ch", "");

                    if (sb.length() > 0) {
                        sb.append(" OR ");
                    }
                    sb.append("(");
                    if (region.getStart() == 0 && region.getEnd() == Integer.MAX_VALUE) {
                        sb.append("chromosome:\"").append(chrom).append("\"");
                    } else if (region.getEnd() == Integer.MAX_VALUE) {
                        sb.append("chromosome:\"").append(chrom)
                                .append("\" AND end:[").append(region.getStart()).append(" TO *]")
                                .append(" AND start:[* TO ").append(region.getStart()).append("]");
                    } else {
                        sb.append("chromosome:\"").append(chrom)
                                .append("\" AND end:[").append(region.getStart()).append(" TO *]")
                                .append(" AND start:[* TO ").append(region.getEnd()).append("]");
                    }
                    sb.append(")");
                }
            }
        }

        return sb.toString();
    }

    /**
     * Build an OR/AND-condition with all consequence types from the input list. It uses the VariantDBAdaptorUtils
     * to parse the consequence type (accession or term) into an integer.
     *
     * @param cts   List of consequence types
     * @param op    Boolean operator (OR / AND)
     * @return      OR/AND-condition string
     */
    private String buildConsequenceTypeOrAnd(List<String> cts, String op) {
        StringBuilder sb = new StringBuilder();
        for (String ct : cts) {
            if (sb.length() > 0) {
                sb.append(op);
            }
            sb.append("soAcc:\"").append(parseConsequenceType(ct)).append("\"");
        }
        return sb.toString();
    }

    private String buildFrom(List<String> list1, List<String> list2) {
        // E.g., in the VariantSearchModel the (gene AND biotype) is modeled in the field: geneToSoAcc:gene_biotype
        // and if there are multiple genes and consequence types, we have to build the combination of all of them in a OR expression
        if (CollectionUtils.isEmpty(list1) || CollectionUtils.isEmpty(list2)) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (String item1: list1) {
            for (String item2: list2) {
                if (sb.length() > 0) {
                    sb.append(" OR ");
                }
                sb.append("geneToSoAcc:\"").append(item1).append("_").append(item2).append("\"");
            }
        }
        return sb.toString();
    }

    private String buildFrom(List<String> list1, List<String> list2, List<String> list3) {
        // E.g.: in the VariantSearchModel the (gene AND ct AND flag) is modeled in the field: geneToSoAcc:gene_ct_flag
        // and if there are multiple genes and consequence types, we have to build the combination of all of them in a OR expression
        if (CollectionUtils.isEmpty(list1) || CollectionUtils.isEmpty(list2) || CollectionUtils.isEmpty(list2)) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (String item1: list1) {
            for (String item2: list2) {
                for (String item3: list3) {
                    if (sb.length() > 0) {
                        sb.append(" OR ");
                    }
                    sb.append("geneToSoAcc:\"").append(item1).append("_").append(item2).append("_").append(item3).append("\"");
                }
            }
        }
        return sb.toString();
    }

    private String buildFrom(List<String> genes, List<String> biotypes, List<String> cts, List<String> flags) {
        // In the VariantSearchModel the (gene AND biotype AND ct AND flag) is modeled in the field:
        // geneToSoAcc:gene_biotype_ct AND geneToSoAcc:gene_ct_flag
        // and if there are multiple genes and consequence types, we have to build the combination of all of them in a OR expression
        StringBuilder sb = new StringBuilder();
        for (String gene: genes) {
            for (String biotype: biotypes) {
                for (String ct: cts) {
                    for (String flag: flags) {
                        if (sb.length() > 0) {
                            sb.append(" OR ");
                        }
                        sb.append("(")
                                .append("geneToSoAcc:")
                                .append("\"")
                                .append(gene).append("_").append(biotype).append("_").append(ct)
                                .append("\"")
                                .append(" AND ")
                                .append("geneToSoAcc:")
                                .append("\"")
                                .append(gene).append("_").append(ct).append("_").append(flag)
                                .append("\"")
                                .append(")");
                    }
                }
            }
        }
        return sb.toString();
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
                List<String> solrFields = Arrays.asList(includeMap.getOrDefault(exclude, "").split(","));
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

        Set<String> mandatoryIncludeFields = new HashSet<>(Arrays.asList("id", "attr_id", "chromosome", "start", "end", "type"));
        Set<String> includeWithMandatory = new LinkedHashSet<>(includes.length + mandatoryIncludeFields.size());

        includeWithMandatory.addAll(Arrays.asList(includes));
        includeWithMandatory.addAll(mandatoryIncludeFields);
        return includeWithMandatory.toArray(new String[0]);
    }

    /**
     * Get the Solr fields to be included in the Solr query (fl parameter) from the variant query.
     *
     * @param query Variant query
     * @return      List of Solr fields to be included in the Solr query
     */
    private List<String> getSolrFieldsFromVariantIncludes(Query query, QueryOptions queryOptions) {
        List<String> solrFields = new ArrayList<>();

        Set<VariantField> incFields = VariantField.getIncludeFields(queryOptions);
        List<String> incStudies = VariantQueryProjectionParser.getIncludeStudiesList(query, incFields);
        if (incStudies != null && incStudies.size() == 0) {
            // Empty (not-null) study list means NONE studies!
            return solrFields;
        }
        if (incStudies != null) {
            incStudies = incStudies.stream().map(VariantSearchToVariantConverter::studyIdToSearchModel).collect(Collectors.toList());
        }

        return solrFields;
    }

    private QueryOperation parseOrAndFilter(String param, String value) {
        return parseOrAndFilter(valueOf(param), value);
    }

    private QueryOperation parseOrAndFilter(VariantQueryParam param, String value) {
        QueryOperation queryOperation = checkOperator(value, param);
        if (queryOperation == null) {
            // return AND by default
            return QueryOperation.AND;
        } else {
            return queryOperation;
        }
    }

    private static void initChromosomeMap() {
        chromosomeMap = new HashMap<>();
        chromosomeMap.put("1", 249250621);
        chromosomeMap.put("2", 243199373);
        chromosomeMap.put("3", 198022430);
        chromosomeMap.put("4", 191154276);
        chromosomeMap.put("5", 180915260);
        chromosomeMap.put("6", 171115067);
        chromosomeMap.put("7", 159138663);
        chromosomeMap.put("8", 146364022);
        chromosomeMap.put("9", 141213431);
        chromosomeMap.put("10", 135534747);
        chromosomeMap.put("11", 135006516);
        chromosomeMap.put("12", 133851895);
        chromosomeMap.put("13", 115169878);
        chromosomeMap.put("14", 107349540);
        chromosomeMap.put("15", 102531392);
        chromosomeMap.put("16", 90354753);
        chromosomeMap.put("17", 81195210);
        chromosomeMap.put("18", 78077248);
        chromosomeMap.put("20", 63025520);
        chromosomeMap.put("19", 59128983);
        chromosomeMap.put("22", 51304566);
        chromosomeMap.put("21", 48129895);
        chromosomeMap.put("X", 155270560);
        chromosomeMap.put("Y", 59373566);
        chromosomeMap.put("MT", 16571);
    }

    public static Map<String, Integer> getChromosomeMap() {
        return chromosomeMap;
    }
}
