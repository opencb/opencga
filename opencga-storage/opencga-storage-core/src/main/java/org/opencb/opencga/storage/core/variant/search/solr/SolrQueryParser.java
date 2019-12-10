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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrException;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.solr.FacetQueryParser;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter;
import org.opencb.opencga.storage.core.variant.search.VariantSearchUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;

/**
 * Created by imedina on 18/11/16.
 * Created by jtarraga on 18/11/16.
 * Created by wasim on 18/11/16.
 */
public class SolrQueryParser {

    private final VariantStorageMetadataManager variantStorageMetadataManager;

    private static Map<String, String> includeMap;

    private static Map<String, Integer> chromosomeMap;
    public static final String CHROM_DENSITY = "chromDensity";

    private static final Pattern STUDY_PATTERN = Pattern.compile("^([^=<>!]+):([^=<>!]+)(!=?|<=?|>=?|<<=?|>>=?|==?|=?)([^=<>!]+.*)$");
    private static final Pattern SCORE_PATTERN = Pattern.compile("^([^=<>!]+)(!=?|<=?|>=?|<<=?|>>=?|==?|=?)([^=<>!]+.*)$");
    private static final Pattern NUMERIC_PATTERN = Pattern.compile("(!=?|<=?|>=?|=?)([^=<>!]+.*)$");

    protected static Logger logger = LoggerFactory.getLogger(SolrQueryParser.class);

    static {
        includeMap = new HashMap<>();

        includeMap.put("id", "id,variantId");
        includeMap.put("chromosome", "chromosome");
        includeMap.put("start", "start");
        includeMap.put("end", "end");
        includeMap.put("type", "type");

        // Remove from this map filter_*,qual_*, fileInfo__* and sampleFormat__*, they will be processed with include-file,
        // include-sample, include-genotype...
        //includeMap.put("studies", "studies,stats__*,gt_*,filter_*,qual_*,fileInfo_*,sampleFormat_*");
        includeMap.put("studies", "studies,stats_*");
        includeMap.put("studies.stats", "studies,stats_*");

        includeMap.put("annotation", "genes,soAcc,geneToSoAcc,biotypes,sift,siftDesc,polyphen,polyphenDesc,popFreq_*,"
                + "xrefs,phastCons,phylop,gerp,caddRaw,caddScaled,traits,other");
        includeMap.put("annotation.consequenceTypes", "genes,soAcc,geneToSoAcc,biotypes,sift,siftDesc,polyphen,"
                + "polyphenDesc,other");
        includeMap.put("annotation.populationFrequencies", "popFreq_*");
        includeMap.put("annotation.xrefs", "xrefs");
        includeMap.put("annotation.conservation", "phastCons,phylop,gerp");
        includeMap.put("annotation.functionalScore", "caddRaw,caddScaled");
        includeMap.put("annotation.traitAssociation", "traits");
    }

    public SolrQueryParser(VariantStorageMetadataManager variantStorageMetadataManager) {
        this.variantStorageMetadataManager = variantStorageMetadataManager;
        initChromosomeMap();
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
                String facetQuery = queryOptions.getString(QueryOptions.FACET);

                if (facetQuery.contains(CHROM_DENSITY)) {
                    facetQuery = parseFacet(facetQuery);
                }
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

            // Add Solr fields from the variant includes, i.e.: includeSample, includeFormat,...
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

            if (queryOptions.containsKey(QueryOptions.SORT)) {
                solrQuery.addSort(queryOptions.getString(QueryOptions.SORT), getSortOrder(queryOptions));
            }
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
        StudyMetadata defaultStudy = getDefaultStudy(query, queryOptions, variantStorageMetadataManager);
        String defaultStudyName = (defaultStudy == null)
                ? null
                : VariantSearchToVariantConverter.studyIdToSearchModel(defaultStudy.getName());
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
                        studyNames.add(VariantSearchToVariantConverter.studyIdToSearchModel(name));
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
            filterList.add(parseScoreValue(ANNOT_PROTEIN_SUBSTITUTION, query.getString(key)));
        }

        // conservation
        key = ANNOT_CONSERVATION.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parseScoreValue(ANNOT_CONSERVATION, query.getString(key)));
        }

        // cadd, functional score
        key = ANNOT_FUNCTIONAL_SCORE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parseScoreValue(ANNOT_FUNCTIONAL_SCORE, query.getString(key)));
        }

        // ALT population frequency
        // in the query: 1kG_phase3:CEU<=0.0053191,1kG_phase3:CLM>0.0125319"
        // in the search model: "popFreq__1kG_phase3__CEU":0.0053191,popFreq__1kG_phase3__CLM">0.0125319"
        key = ANNOT_POPULATION_ALTERNATE_FREQUENCY.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parsePopFreqValue(ANNOT_POPULATION_ALTERNATE_FREQUENCY, "popFreq", query.getString(key), "ALT", null, null));
        }

        // MAF population frequency
        // in the search model: "popFreq__1kG_phase3__CLM":0.005319148767739534
        key = ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parsePopFreqValue(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY, "popFreq", query.getString(key), "MAF", null, null));
        }

        // REF population frequency
        // in the search model: "popFreq__1kG_phase3__CLM":0.005319148767739534
        key = ANNOT_POPULATION_REFERENCE_FREQUENCY.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parsePopFreqValue(ANNOT_POPULATION_REFERENCE_FREQUENCY, "popFreq", query.getString(key), "REF", null, null));
        }

        // Stats ALT
        // In the model: "stats__1kg_phase3__ALL"=0.02
        key = STATS_ALT.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parsePopFreqValue(STATS_ALT, "stats", query.getString(key), "ALT", defaultStudyName,
                    query.getString(STUDY.key())));
        }

        // Stats MAF
        // In the model: "stats__1kg_phase3__ALL"=0.02
        key = STATS_MAF.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parsePopFreqValue(STATS_MAF, "stats", query.getString(key), "MAF", defaultStudyName,
                    query.getString(STUDY.key())));
        }

        // Stats REF
        // In the model: "stats__1kg_phase3__ALL"=0.02
        key = STATS_REF.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            filterList.add(parsePopFreqValue(STATS_REF, "stats", query.getString(key), "REF", defaultStudyName,
                    query.getString(STUDY.key())));
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
            filterList.add(parseCategoryTermValue("traits", query.getString(key)));
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
        key = ANNOT_CLINICAL_SIGNIFICANCE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
//            filterList.add(parseCategoryTermValue("traits", query.getString(key), "cs\\:", true));
            Pair<QueryOperation, List<String>> pair = splitValue(query.getString(key));
            List<String> clinSig = pair.getRight();
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            for (int i = 0; i < clinSig.size(); i++) {
                if (i > 0) {
                    sb.append(QueryOperation.OR.equals(pair.getLeft()) ? " OR " : " AND ");
                }
                // FIXME:
                //   We are storing raw ClinicalSignificance values
                //   We should use the enum {@link org.opencb.biodata.models.variant.avro.ClinicalSignificance}
                //   Replace "_" with " " works to search by "Likely benign" instead of "likely_benign"
                String value = clinSig.get(i).replace("_", " ");
                sb.append("traits:\"*cs\\:").append(value).append("*\"");
            }
            sb.append(")");
            filterList.add(sb.toString());
        }

        // Add Solr query filter for genotypes
        addSampleFilters(query, filterList);

        // Add Solr query filters for files, QUAL and FILTER
        addFileFilters(query, filterList);

        // File info filter are not supported
        key = INFO.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            throw VariantQueryException.unsupportedVariantQueryFilter(INFO, "Solr", "");
        }

        // Create Solr query, adding filter queries and fields to show
        solrQuery.setQuery("*:*");
        filterList.forEach(solrQuery::addFilterQuery);

        logger.debug("----------------------");
        logger.debug("query     : " + printQuery(query));
        logger.debug("solrQuery : " + solrQuery);
        return solrQuery;
    }

    private String parseGenomicFilter(Query query) {
        List<Region> regions = new ArrayList<>();
        List<String> xrefs = new ArrayList<>();
        List<String> genes = new ArrayList<>();
        List<String> biotypes = new ArrayList<>();
        List<String> consequenceTypes = new ArrayList<>();
        List<String> flags = new ArrayList<>();

        VariantQueryParser.VariantQueryXref variantQueryXref = VariantQueryParser.parseXrefs(query);
        genes.addAll(variantQueryXref.getGenes());
        xrefs.addAll(variantQueryXref.getIds());
        xrefs.addAll(variantQueryXref.getOtherXrefs());
        xrefs.addAll(variantQueryXref.getVariants().stream().map(Variant::toString).collect(Collectors.toList()));

        // Regions
        if (StringUtils.isNotEmpty(query.getString(REGION.key()))) {
            regions = Region.parseRegions(query.getString(REGION.key()));
        }

        // Biotypes
        if (StringUtils.isNotEmpty(query.getString(ANNOT_BIOTYPE.key()))) {
            biotypes = Arrays.asList(query.getString(ANNOT_BIOTYPE.key()).split("[,;]"));
        }

        // Consequence types (cts)
        String ctLogicalOperator = " OR ";
        if (StringUtils.isNotEmpty(query.getString(ANNOT_CONSEQUENCE_TYPE.key(), ""))) {
            consequenceTypes = Arrays.asList(query.getString(ANNOT_CONSEQUENCE_TYPE.key()).split("[,;]"));
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
        List<String> facetList = new ArrayList<>();
        String[] facets = facetQuery.split(FacetQueryParser.FACET_SEPARATOR);
        for (String facet: facets) {
            if (facet.contains(CHROM_DENSITY)) {
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
                            for (String value: include.split(FacetQueryParser.INCLUDE_SEPARATOR)) {
                                chromList.add(value);
                            }
                        } else {
                            chromList = new ArrayList<>(chromosomeMap.keySet());
                        }

                        List<String> chromQueryList = new ArrayList<>();
                        for (String chrom: chromList) {
                            if (chromosomeMap.get(chrom) > maxLength) {
                                maxLength = chromosomeMap.get(chrom);
                            }
                            chromQueryList.add("chromosome:" + chrom);
                        }
                        facetList.add("start[1.." + maxLength + "]:" + step + ":chromDensity"
                                + FacetQueryParser.LABEL_SEPARATOR + "chromosome:"
                                + StringUtils.join(chromQueryList, " OR "));
//                        for (String chr: chromosomes) {
//                            facetList.add("start[1.." + chromosomeMap.get(chr) + "]:" + step + ":chromDensity." + chr
//                                    + ":chromosome:" + chr);
//                        }
                    } else {
                        throw VariantQueryException.malformedParam(null, CHROM_DENSITY, "Invalid syntax: " + facet);
                    }
                } else {
                    throw VariantQueryException.malformedParam(null, CHROM_DENSITY, "Invalid syntax: " + facet);
                }
            } else {
                facetList.add(facet);
            }
        }
        return StringUtils.join(facetList, FacetQueryParser.FACET_SEPARATOR);
    }

    /**
     * Add Solr query filter for genotypes.
     *
     * @param query         Query
     * @param filterList    Output list with Solr query filters added
     */
    private void addSampleFilters(Query query, List<String> filterList) {
        String[] studies = getStudies(query);

        String key = GENOTYPE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            if (studies == null) {
                throw VariantQueryException.malformedParam(STUDY, "", "Missing study parameter when "
                        + " filtering by 'genotype'");
            }
            Map<Object, List<String>> genotypeSamples = new HashMap<>();
            try {
                QueryOperation queryOperation = parseGenotypeFilter(query.getString(key), genotypeSamples);
                boolean addOperator = false;
                if (MapUtils.isNotEmpty(genotypeSamples)) {
                    StringBuilder sb = new StringBuilder("(");
                    for (Object sampleName : genotypeSamples.keySet()) {
                        if (addOperator) {
                            sb.append(" ").append(queryOperation.name()).append(" ");
                        }
                        addOperator = true;
                        sb.append("(");
                        boolean addOr = false;
                        for (String gt : genotypeSamples.get(sampleName)) {
                            if (addOr) {
                                sb.append(" OR ");
                            }
                            addOr = true;
                            sb.append("gt").append(VariantSearchUtils.FIELD_SEPARATOR).append(studies[0])
                                    .append(VariantSearchUtils.FIELD_SEPARATOR).append(sampleName.toString())
                                    .append(":\"").append(gt).append("\"");
                        }
                        sb.append(")");
                    }
                    sb.append(")");
                    filterList.add(sb.toString());
                }
            } catch (Exception e) {
                throw VariantQueryException.internalException(e);
            }
        }

        key = FORMAT.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            if (studies == null) {
                throw VariantQueryException.malformedParam(FORMAT, query.getString(FORMAT.key()),
                        "Missing study parameter when filtering by 'format'");
            }

            Pair<QueryOperation, Map<String, String>> parsedSampleFormats = parseFormat(query);
            String logicOpStr = parsedSampleFormats.getKey() == QueryOperation.AND ? " AND " : " OR ";
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            boolean first = true;
            for (String sampleId : parsedSampleFormats.getValue().keySet()) {
                // Sanity check, only DP is permitted
                Pair<QueryOperation, List<String>> formats = splitValue(parsedSampleFormats.getValue().get(sampleId));
                if (formats.getValue().size() > 1) {
                    throw VariantQueryException.malformedParam(FORMAT, query.getString(FORMAT.key()),
                            "Only one format name (and it has to be 'DP') is permitted in Solr search");
                }
                if (!first) {
                    sb.append(logicOpStr);
                }
                String[] split = splitOperator(parsedSampleFormats.getValue().get(sampleId));
                if (split[0] == null) {
                    throw VariantQueryException.malformedParam(FORMAT, query.getString(FORMAT.key()),
                            "Invalid format value");
                }
                if ("DP".equals(split[0].toUpperCase())) {
                    sb.append(parseNumericValue("dp" + VariantSearchUtils.FIELD_SEPARATOR + studies[0]
                            + VariantSearchUtils.FIELD_SEPARATOR + sampleId, split[1] + split[2]));
                    first = false;
                } else {
                    throw VariantQueryException.malformedParam(FORMAT, query.getString(FORMAT.key()),
                            "Only format name 'DP' is permitted in Solr search");
                }
            }
            sb.append(")");
            filterList.add(sb.toString().replace(String.valueOf(VariantSearchToVariantConverter.MISSING_VALUE),
                    "" + Math.round(VariantSearchToVariantConverter.MISSING_VALUE)));
        }
    }

    /**
     * Add Solr query filters for files, QUAL and FILTER.
     *
     * @param query         Query
     * @param filterList    Output list with Solr query filters added
     */
    private void addFileFilters(Query query, List<String> filterList) {
        // IMPORTANT: Only the first study is taken into account! Multiple studies support ??
        String[] studies = getStudies(query);

        String[] files = null;
        QueryOperation fileQueryOp = null;

        String key = FILE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            if (studies == null) {
                throw VariantQueryException.malformedParam(STUDY, "", "Missing study parameter when "
                        + " filtering with 'files'");
            }

            files = query.getString(key).split("[,;]");
            fileQueryOp = parseOrAndFilter(key, query.getString(key));

            if (fileQueryOp == QueryOperation.OR) {     // OR
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < files.length; i++) {
                    sb.append("fileInfo").append(VariantSearchUtils.FIELD_SEPARATOR)
                            .append(studies[0]).append(VariantSearchUtils.FIELD_SEPARATOR).append(files[i]).append(": [* TO *]");
                    if (i < files.length - 1) {
                        sb.append(" OR ");
                    }
                }
                filterList.add(sb.toString());
            } else {    // AND
                for (String file: files) {
                    filterList.add("fileInfo" + VariantSearchUtils.FIELD_SEPARATOR
                            + studies[0] + VariantSearchUtils.FIELD_SEPARATOR + file + ": [* TO *]");
                }
            }
        }
        if (files == null) {
            List<String> includeFiles = getIncludeFilesList(query);
            if (includeFiles != null) {
                files = includeFiles.toArray(new String[0]);
            }
        }

        // QUAL
        key = QUAL.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            if (files == null) {
                throw VariantQueryException.malformedParam(FILE, "", "Missing file parameter when "
                        + " filtering with QUAL.");
            }
            String qual = query.getString(key);
            if (fileQueryOp == QueryOperation.OR) {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < files.length; i++) {
                    sb.append(parseNumericValue("qual" + VariantSearchUtils.FIELD_SEPARATOR + studies[0]
                            + VariantSearchUtils.FIELD_SEPARATOR + files[i], qual));
                    if (i < files.length - 1) {
                        sb.append(" OR ");
                    }
                }
                filterList.add(sb.toString());
            } else {
                for (String file: files) {
                    filterList.add(parseNumericValue("qual" + VariantSearchUtils.FIELD_SEPARATOR + studies[0]
                            + VariantSearchUtils.FIELD_SEPARATOR + file, qual));
                }
            }
        }

        // FILTER
        key = FILTER.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            if (files == null) {
                throw VariantQueryException.malformedParam(FILE, "", "Missing file parameter when "
                        + " filtering with FILTER.");
            }

            QueryOperation filterQueryOp = parseOrAndFilter(key, query.getString(key));
            String filterQueryOpString = (filterQueryOp == QueryOperation.OR ? " OR " : " AND ");

            StringBuilder sb = new StringBuilder();
            List<String> filters = splitQuotes(query.getString(key), filterQueryOp);
            if (fileQueryOp == QueryOperation.AND) {
                // AND- between files
                for (String file : files) {
                    sb.setLength(0);
                    for (int j = 0; j < filters.size(); j++) {
                        sb.append("filter").append(VariantSearchUtils.FIELD_SEPARATOR).append(studies[0])
                                .append(VariantSearchUtils.FIELD_SEPARATOR).append(file)
                                .append(":/(.*)?").append(filters.get(j)).append("(.*)?/");
                        if (j < filters.size() - 1) {
                            sb.append(filterQueryOpString);
                        }
                    }
                    filterList.add(sb.toString());
                }
            } else {
                // OR- between files (...or skip when only one file is present)
                for (int i = 0; i < files.length; i++) {
                    sb.append("(");
                    for (int j = 0; j < filters.size(); j++) {
                        sb.append("filter").append(VariantSearchUtils.FIELD_SEPARATOR).append(studies[0])
                                .append(VariantSearchUtils.FIELD_SEPARATOR).append(files[i])
                                .append(":/(.*)?").append(filters.get(j)).append("(.*)?/");
                        if (j < filters.size() - 1) {
                            sb.append(filterQueryOpString);
                        }
                    }
                    sb.append(")");
                    if (i < files.length - 1) {
                        sb.append(" OR ");
                    }
                }
                filterList.add(sb.toString());
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
            String value = val.replace("\"", "");

            QueryOperation queryOperation = parseOrAndFilter(name, val);
            String logicalComparator = queryOperation == QueryOperation.OR ? " OR " : " AND ";
            String wildcard = partialSearch ? "*" : "";

            String[] values = splitValue(value, queryOperation).toArray(new String[0]);
            if (values.length == 1) {
                negation = "";
                if (isNegated(value)) {
                    negation = "-";
                    value = removeNegation(value);
                }
                filter.append(negation).append(name).append(":\"").append(valuePrefix).append(wildcard).append(value)
                        .append(wildcard).append("\"");
            } else {
                filter.append("(");
                negation = "";
                if (isNegated(values[0])) {
                    negation = "-";
                    values[0] = removeNegation(values[0]);
                }
                filter.append(negation).append(name).append(":\"").append(valuePrefix).append(wildcard)
                        .append(values[0]).append(wildcard).append("\"");
                for (int i = 1; i < values.length; i++) {
                    filter.append(logicalComparator);
                    negation = "";
                    if (isNegated(values[i])) {
                        negation = "-";
                        values[i] = removeNegation(values[i]);
                    }
                    filter.append(negation).append(name).append(":\"").append(valuePrefix).append(wildcard)
                            .append(values[i]).append(wildcard).append("\"");
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
            filter.append(getRange("", name, matcher.group(1), matcher.group(2)));
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
                    sb.append(getRange("", matcher.group(1), matcher.group(2), matcher.group(3)));
                } else {
                    logger.debug("Invalid expression: {}", value);
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

                        list.add(getRange("", filterName, filterOp, filterValue));
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
     *
     * @param param        Param name
     * @param name         Parameter type: propFreq or stats
     * @param value        Parameter value
     * @param type         Type of frequency: REF, ALT, MAF
     * @param defaultStudy Default study. To be used only if the study is not present.
     * @param studies    True if multiple studies are present joined by , (i.e., OR logical operation), only for STATS
     * @return             The string with the boolean conditions
     */
    private String parsePopFreqValue(VariantQueryParam param, String name, String value, String type, String defaultStudy,
                                     String studies) {
        // In Solr, range queries can be inclusive or exclusive of the upper and lower bounds:
        //    - Inclusive range queries are denoted by square brackets.
        //    - Exclusive range queries are denoted by curly brackets.
        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotEmpty(value)) {
            // FIXME at the higher level
            value = value.replace("<<", "<");
            value = value.replace("<", "<<");

            QueryOperation queryOperation = parseOrAndFilter(param, value);
            String logicalComparator = (queryOperation == QueryOperation.OR) ? " OR " : " AND ";

            // We need to know if
            boolean addOr = true;
            if (name.equals("stats")) {
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
                        List<String> values = splitValue(value, queryOperation);
                        addOr = false;
                        for (String val: values) {
                            String std = val.contains(":") ? val.split(":")[0] : defaultStudy;
                            if (!studiesSet.contains(std)) {
                                addOr = true;
                                break;
                            }
                        }
                    }
                }
            }

            List<String> values = splitValue(value, queryOperation);
            List<String> list = new ArrayList<>(values.size());
            for (String v : values) {
                String[] keyOpValue = splitOperator(v);
                String studyPop = keyOpValue[0];
                String op = keyOpValue[1];
                String numValue = keyOpValue[2];
                if (studyPop == null) {
                    throw VariantQueryException.malformedParam(param, value);
                }

                // Solr only stores ALT frequency, we need to calculate the MAF or REF before querying
                String[] freqValue = getMafOrRefFrequency(type, op, numValue);

                String[] studyPopSplit = splitStudyResource(studyPop);
                String study;
                String pop;
                if (studyPopSplit.length == 2) {
                    study = VariantSearchToVariantConverter.studyIdToSearchModel(studyPopSplit[0]);
                    pop = studyPopSplit[1];
                } else {
                    if (StringUtils.isEmpty(defaultStudy)) {
                        throw VariantQueryException.malformedParam(param, value, "Missing study");
                    }
                    study = defaultStudy;
                    pop = studyPop;
                }

                if (name.equals("popFreq")) {
                    if ((study.equals("1kG_phase3") || study.equals("GNOMAD_GENOMES") || study.equals("GNOMAD_EXOMES"))
                            && pop.equals("ALL")) {
                        addOr = false;
                    } else {
                        addOr = true;
                    }
                }

                // concat expression, e.g.: value:[0 TO 12]
                list.add(getRange(name + VariantSearchUtils.FIELD_SEPARATOR + study
                        + VariantSearchUtils.FIELD_SEPARATOR, pop, freqValue[0], freqValue[1], addOr));
            }
            if (list.size() == 1) {
                sb.append(list.get(0));
            } else {
                sb.append('(').append(StringUtils.join(list, logicalComparator)).append(')');
            }
        }
        return sb.toString();
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
     * @param prefix    Prefix, e.g.: popFreq__study__cohort, stats__ or null
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
                    sb.append(prefix).append(getSolrFieldName(name)).append(":[0 TO ").append(value).append(rightCloseOperator);
                    if (addOr) {
                        sb.append(" OR ");
                        sb.append("(* -").append(prefix).append(getSolrFieldName(name)).append(":*)");
                    }
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
                    if (addOr) {
                        sb.append(" OR ");
                        sb.append("(* -").append(prefix).append(getSolrFieldName(name)).append(":*)");
                    }
                } else {
                    // attention: negative values must be escaped
                    sb.append(prefix).append(getSolrFieldName(name)).append(":").append(leftCloseOperator).append(value).append(" TO *]");
                    sb.append(" OR ");
                    sb.append(prefix).append(getSolrFieldName(name)).append(":\\").append(VariantSearchToVariantConverter.MISSING_VALUE);
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
                        sb.append("chromosome:\"").append(chrom).append("\" AND start:").append(region.getStart());
                    } else {
                        sb.append("chromosome:\"").append(chrom)
                                .append("\" AND start:[").append(region.getStart()).append(" TO *]")
                                .append(" AND end:[* TO ").append(region.getEnd()).append("]");
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

    /**
     * Get the Solr fields to be included in the Solr query (fl parameter) from the variant query, i.e.: include-file,
     * include-format, include-sample, include-study and include-genotype.
     *
     * @param query Variant query
     * @return      List of Solr fields to be included in the Solr query
     */
    private List<String> getSolrFieldsFromVariantIncludes(Query query, QueryOptions queryOptions) {
        List<String> solrFields = new ArrayList<>();

        Set<VariantField> incFields = VariantField.getIncludeFields(queryOptions);
        List<String> incStudies = getIncludeStudiesList(query, incFields);
        if (incStudies != null && incStudies.size() == 0) {
            // Empty (not-null) study list means NONE studies!
            return solrFields;
        }
        if (incStudies != null) {
            incStudies = incStudies.stream().map(VariantSearchToVariantConverter::studyIdToSearchModel).collect(Collectors.toList());
        }

        // --include-file management
        List<String> incFiles = getIncludeFilesList(query, incFields);
        if (incFiles == null) {
            // If file list is null, it means ALL files
            if (incStudies == null) {
                // Here, the file and study lists are null
                solrFields.add("fileInfo" + VariantSearchUtils.FIELD_SEPARATOR + "*");
                solrFields.add("qual" + VariantSearchUtils.FIELD_SEPARATOR + "*");
                solrFields.add("filter" + VariantSearchUtils.FIELD_SEPARATOR + "*");
            } else {
                // The file list is null but the study list is not empty
                for (String incStudy: incStudies) {
                    solrFields.add("fileInfo" + VariantSearchUtils.FIELD_SEPARATOR + incStudy + VariantSearchUtils.FIELD_SEPARATOR + "*");
                    solrFields.add("qual" + VariantSearchUtils.FIELD_SEPARATOR + incStudy + VariantSearchUtils.FIELD_SEPARATOR + "*");
                    solrFields.add("filter" + VariantSearchUtils.FIELD_SEPARATOR + incStudy + VariantSearchUtils.FIELD_SEPARATOR + "*");
                }
            }
        } else {
            if (incStudies == null) {
                for (String incFile: incFiles) {
                    solrFields.add("fileInfo" + VariantSearchUtils.FIELD_SEPARATOR + "*" + VariantSearchUtils.FIELD_SEPARATOR + incFile);
                    solrFields.add("qual" + VariantSearchUtils.FIELD_SEPARATOR + "*" + VariantSearchUtils.FIELD_SEPARATOR + incFile);
                    solrFields.add("filter" + VariantSearchUtils.FIELD_SEPARATOR + "*" + VariantSearchUtils.FIELD_SEPARATOR + incFile);
                }
            } else {
                for (String incFile: incFiles) {
                    for (String incStudy: incStudies) {
                        solrFields.add("fileInfo" + VariantSearchUtils.FIELD_SEPARATOR + incStudy + VariantSearchUtils.FIELD_SEPARATOR
                                + incFile);
                        solrFields.add("qual" + VariantSearchUtils.FIELD_SEPARATOR + incStudy + VariantSearchUtils.FIELD_SEPARATOR
                                + incFile);
                        solrFields.add("filter" + VariantSearchUtils.FIELD_SEPARATOR + incStudy + VariantSearchUtils.FIELD_SEPARATOR
                                + incFile);
                    }
                }
            }
        }

        // --include-sample management
        List<String> incSamples = getIncludeSamplesList(query, queryOptions);
        if (incSamples != null && incSamples.size() == 0) {
            // Empty list means NONE sample!
            return solrFields;
        }

        if (incSamples == null) {
            // null means ALL samples
            if (query.getBoolean(INCLUDE_GENOTYPE.key())) {
                // Genotype
                if (incStudies == null) {
                    // null means ALL studies: include genotype for all studies and samples
                    solrFields.add("gt" + VariantSearchUtils.FIELD_SEPARATOR + "*");
                    solrFields.add("sampleFormat" + VariantSearchUtils.FIELD_SEPARATOR + "*"
                            + VariantSearchUtils.FIELD_SEPARATOR + "sampleName");
                    solrFields.add("sampleFormat" + VariantSearchUtils.FIELD_SEPARATOR + "*"
                            + VariantSearchUtils.FIELD_SEPARATOR + "format");
                } else {
                    // Include genotype for the specified studies and all samples
                    for (String incStudy: incStudies) {
                        solrFields.add("gt" + VariantSearchUtils.FIELD_SEPARATOR + incStudy + VariantSearchUtils.FIELD_SEPARATOR + "*");
                        solrFields.add("sampleFormat" + VariantSearchUtils.FIELD_SEPARATOR + incStudy
                                + VariantSearchUtils.FIELD_SEPARATOR + "sampleName");
                        solrFields.add("sampleFormat" + VariantSearchUtils.FIELD_SEPARATOR + incStudy
                                + VariantSearchUtils.FIELD_SEPARATOR + "format");
                    }
                }
            } else {
                // Sample format
                if (incStudies == null) {
                    // null means ALL studies: include sample format for all studies and samples
                    solrFields.add("sampleFormat" + VariantSearchUtils.FIELD_SEPARATOR + "*");
                } else {
                    // Include sample format for the specified studies and samples
                    for (String incStudy: incStudies) {
                        solrFields.add("sampleFormat" + VariantSearchUtils.FIELD_SEPARATOR + incStudy
                                + VariantSearchUtils.FIELD_SEPARATOR + "*");
                    }
                }
            }
        } else {
            // Processing the list of samples
            if (query.getBoolean(INCLUDE_GENOTYPE.key())) {
                // Genotype
                if (incStudies == null) {
                    // null means ALL studies: include genotype for all studies and the specified samples
                    solrFields.add("sampleFormat" + VariantSearchUtils.FIELD_SEPARATOR + "*"
                            + VariantSearchUtils.FIELD_SEPARATOR + "sampleName");
                    solrFields.add("sampleFormat" + VariantSearchUtils.FIELD_SEPARATOR + "*"
                            + VariantSearchUtils.FIELD_SEPARATOR + "format");
                    for (String incSample: incSamples) {
                        solrFields.add("gt" + VariantSearchUtils.FIELD_SEPARATOR + "*"
                                + VariantSearchUtils.FIELD_SEPARATOR + incSample);
                    }
                } else {
                    // Include genotype for the specified studies and samples
                    for (String incStudy: incStudies) {
                        solrFields.add("sampleFormat" + VariantSearchUtils.FIELD_SEPARATOR + incStudy
                                + VariantSearchUtils.FIELD_SEPARATOR + "sampleName");
                        solrFields.add("sampleFormat" + VariantSearchUtils.FIELD_SEPARATOR + incStudy
                                + VariantSearchUtils.FIELD_SEPARATOR + "format");
                        for (String incSample: incSamples) {
                            solrFields.add("gt" + VariantSearchUtils.FIELD_SEPARATOR + incStudy + VariantSearchUtils.FIELD_SEPARATOR
                                    + incSample);
                        }
                    }
                }
            } else {
                // Sample format
                if (incStudies == null) {
                    // null means ALL studies: include sample format for all studies and the specified samples
                    solrFields.add("sampleFormat" + VariantSearchUtils.FIELD_SEPARATOR + "*"
                            + VariantSearchUtils.FIELD_SEPARATOR + "sampleName");
                    solrFields.add("sampleFormat" + VariantSearchUtils.FIELD_SEPARATOR + "*"
                            + VariantSearchUtils.FIELD_SEPARATOR + "format");
                    for (String incSample: incSamples) {
                        solrFields.add("sampleFormat" + VariantSearchUtils.FIELD_SEPARATOR + "*" + VariantSearchUtils.FIELD_SEPARATOR
                                + incSample);
                    }
                } else {
                    // Include sample format for the specified studies and samples
                    for (String incStudy: incStudies) {
                        solrFields.add("sampleFormat" + VariantSearchUtils.FIELD_SEPARATOR + incStudy
                                + VariantSearchUtils.FIELD_SEPARATOR + "sampleName");
                        solrFields.add("sampleFormat" + VariantSearchUtils.FIELD_SEPARATOR + incStudy
                                + VariantSearchUtils.FIELD_SEPARATOR + "format");
                        for (String incSample: incSamples) {
                            solrFields.add("sampleFormat" + VariantSearchUtils.FIELD_SEPARATOR + incStudy
                                    + VariantSearchUtils.FIELD_SEPARATOR + incSample);
                        }
                    }
                }
            }
        }

        return solrFields;
    }

    private String[] getStudies(Query query) {
        // Sanity check for QUAL and FILTER, only one study is permitted, but multiple files
        String[] studies = null;
        if (StringUtils.isNotEmpty(query.getString(STUDY.key()))) {
            studies = query.getString(STUDY.key()).split("[,;]");
            for (int i = 0; i < studies.length; i++) {
                studies[i] = VariantSearchToVariantConverter.studyIdToSearchModel(studies[i]);
            }
        }
        return studies;
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

    private void initChromosomeMap() {
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
