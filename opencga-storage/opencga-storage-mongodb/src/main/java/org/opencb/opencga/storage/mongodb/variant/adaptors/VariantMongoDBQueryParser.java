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

package org.opencb.opencga.storage.mongodb.variant.adaptors;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.*;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;
import org.opencb.opencga.storage.mongodb.variant.converters.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.*;
import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.LOADED_GENOTYPES;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;
import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageOptions.DEFAULT_GENOTYPE;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantAnnotationConverter.SEPARATOR;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter.INDEX_FIELD;

/**
 * Created on 31/03/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMongoDBQueryParser {

    public static final String OVERLAPPED_FILES_ONLY = "overlappedFilesOnly";
    public static final VariantStringIdConverter STRING_ID_CONVERTER = new VariantStringIdConverter();
    public static final JsonWriterSettings JSON_WRITER_SETTINGS = JsonWriterSettings.builder()
            .outputMode(JsonMode.SHELL)
            .indent(false).build();
    protected static Logger logger = LoggerFactory.getLogger(VariantMongoDBQueryParser.class);
    private final VariantStorageMetadataManager metadataManager;
    //    private final CellBaseUtils cellBaseUtils;

    public VariantMongoDBQueryParser(VariantStorageMetadataManager variantStorageMetadataManager) {
        this.metadataManager = variantStorageMetadataManager;

    }

    @Deprecated
    protected Bson parseQuery(Query query) {
        return parseQuery(new VariantQueryParser(null, metadataManager).parseQuery(query, null, false));
    }

    protected Bson parseQuery(ParsedVariantQuery parsedVariantQuery) {

        // Region filters that intersect with other filters, but create a union between them
        Bson idIntersectBson = null;
        List<Bson> regionFilters = new ArrayList<>();
        List<Bson> filters = new ArrayList<>();
        if (parsedVariantQuery != null) {
            // Copy given query. It may be modified
            Query query = new Query(parsedVariantQuery.getInputQuery());
            // Object with all VariantIds, ids, genes and xrefs from ID, XREF, GENES, ... filters
            ParsedVariantQuery.VariantQueryXref variantQueryXref = parsedVariantQuery.getXrefs();

            boolean pureGeneRegionFilter = !variantQueryXref.getGenes().isEmpty();
            /* VARIANT PARAMS */

            if (isValidParam(query, REGION)) {
                pureGeneRegionFilter = false;
                List<Region> regions = Region.parseRegions(query.getString(REGION.key()), true);
                if (!regions.isEmpty()) {
                    getRegionFilter(regions, regionFilters);
                }
            }

            if (!variantQueryXref.getIds().isEmpty()) {
                pureGeneRegionFilter = false;
                addQueryStringFilter(DocumentToVariantAnnotationConverter.XREFS_ID,
                        variantQueryXref.getIds(), regionFilters);
//                addQueryStringFilter(DocumentToVariantConverter.IDS_FIELD, variantQueryXref.getIds(), regionFilters);
            }

            if (!variantQueryXref.getOtherXrefs().isEmpty()) {
                pureGeneRegionFilter = false;
                addQueryStringFilter(DocumentToVariantAnnotationConverter.XREFS_ID,
                        variantQueryXref.getOtherXrefs(), regionFilters);
            }
            if (!variantQueryXref.getVariants().isEmpty()) {
                pureGeneRegionFilter = false;
                List<String> mongoIds = variantQueryXref.getVariants().stream().map(STRING_ID_CONVERTER::buildId)
                        .collect(Collectors.toList());
                Bson variantXrefIntersect;
                if (mongoIds.size() == 1) {
                    variantXrefIntersect = eq("_id", mongoIds.get(0));
                } else {
                    variantXrefIntersect = in("_id", mongoIds);
                }
                regionFilters.add(variantXrefIntersect);
            }

            List<Variant> idIntersect = query.getAsStringList(ID_INTERSECT.key()).stream().map(Variant::new).collect(Collectors.toList());
            if (!idIntersect.isEmpty()) {
                List<String> mongoIds = idIntersect.stream().map(STRING_ID_CONVERTER::buildId).collect(Collectors.toList());
                if (mongoIds.size() == 1) {
                    idIntersectBson = eq("_id", mongoIds.get(0));
                } else {
                    idIntersectBson = in("_id", mongoIds);
                }
            }

            boolean ctBtFlagApplied = addGeneCombinationFilter(parsedVariantQuery, filters, regionFilters,
                    pureGeneRegionFilter, !variantQueryXref.getGenes().isEmpty());
            if (!variantQueryXref.getGenes().isEmpty()) {
                if (parsedVariantQuery.getAnnotationQuery().getGeneCombinations() == null) {
                    // If gene combination is present, the gene filter will be applied as part of the combination filter
                    // Combination not present, so we can apply the gene filter directly
                    addQueryStringFilter(DocumentToVariantAnnotationConverter.XREFS_ID, variantQueryXref.getGenes(), regionFilters);
                }
            }

            if (isValidParam(query, REFERENCE)) {
                addQueryStringFilter(DocumentToVariantConverter.REFERENCE_FIELD, query.getString(REFERENCE.key()),
                        filters);
            }

            if (isValidParam(query, ALTERNATE)) {
                addQueryStringFilter(DocumentToVariantConverter.ALTERNATE_FIELD, query.getString(ALTERNATE.key()),
                        filters);
            }

            if (isValidParam(query, TYPE)) {
                filters.add(addQueryFilter(DocumentToVariantConverter.TYPE_FIELD, query.getAsStringList(TYPE.key())));
                //addQueryStringFilter(DBObjectToVariantConverter.TYPE_FIELD,
//                query.getString(VariantQueryParams.TYPE.key()), builder, QueryOperation.AND);
            }

            if (isValidParam(query, RELEASE)) {
                int release = query.getInt(RELEASE.key(), -1);
                if (release <= 0) {
                    throw VariantQueryException.malformedParam(RELEASE, query.getString(RELEASE.key()));
                }

                filters.add(lte(DocumentToVariantConverter.RELEASE_FIELD, release));
            }

            /* ANNOTATION PARAMS */
            filters.addAll(parseAnnotationQueryParams(parsedVariantQuery, query, ctBtFlagApplied));

            /* STUDIES */
            filters.addAll(parseStudyQueryParams(parsedVariantQuery, query));

            /* STATS PARAMS */
            parseStatsQueryParams(parsedVariantQuery, query, filters);
        }

        return combine(parsedVariantQuery, regionFilters, idIntersectBson, filters);
    }

    /**
     * Combine region filters, idIntersect filter and other filters.
     *
     *  AND
     * ├─ ID_INTERSECT_FILTER
     * ├─ FILTER_1
     * ├─ FILTER_2
     * ├─ ...
     * └─ OR
     *    ├─ REGION_FILTER_1
     *    ├─ REGION_FILTER_2
     *    └─ ...
     *
     * @param parsedVariantQuery
     * @param regionFilters
     * @param idIntersectBson
     * @param otherFilters
     * @return
     */
    private static Bson combine(ParsedVariantQuery parsedVariantQuery, List<Bson> regionFilters, Bson idIntersectBson,
                                List<Bson> otherFilters) {
        List<Bson> filters = new ArrayList<>();

        if (idIntersectBson != null) {
            filters.add(idIntersectBson);
        }
        // Combine region filters
        if (!regionFilters.isEmpty()) {
            Bson regionFilterBson;
            if (regionFilters.size() == 1) {
                regionFilterBson = regionFilters.get(0);
            } else {
                regionFilterBson = or(regionFilters);
            }
            filters.add(regionFilterBson);
        }
        filters.addAll(otherFilters);

        Bson filter;
        if (filters.isEmpty()) {
            filter = empty();
        } else {
            filter = and(filters);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("----------------------");
            logger.debug("Query         = {}", VariantQueryUtils.printQuery(parsedVariantQuery == null
                    ? null
                    : parsedVariantQuery.getInputQuery()));
            logger.debug("MongoDB Query = {}", filter.toBsonDocument().toJson(JSON_WRITER_SETTINGS));
        }
        logger.info("MongoDB Query = {}", filter.toBsonDocument().toJson(JSON_WRITER_SETTINGS));
        if (!filters.isEmpty()) {
            logger.info("MongoDB Query (all of):");
            if (idIntersectBson != null) {
                logger.info("  IdIntersect = {}", idIntersectBson.toBsonDocument().toJson(JSON_WRITER_SETTINGS));
            }
            if (regionFilters.size() > 1) {
                logger.info("  Region filters (any of):");
                for (Bson regionFilter : regionFilters) {
                    logger.info("    {}", regionFilter.toBsonDocument().toJson(JSON_WRITER_SETTINGS));
                }
            }
            if (!otherFilters.isEmpty()) {
                logger.info("  Other filters (all of):");
                for (Bson otherFilter : otherFilters) {
                    logger.info("    {}", otherFilter.toBsonDocument().toJson(JSON_WRITER_SETTINGS));
                }
            }
        }
        return filter;
    }

    /**
     * Add gene combination filters.
     * Depending on the type of combination, the filter will be applied directly to the main filter list, or as part of the region filters.
     *
     * Scenarios:
     *  - No region nor gene filter:
     *     - Combination filter can be applied directly to the main filter list
     *     - ct, bt and flag filters do not need to be applied separately
     *  - Gene filter, but no other region filter:
     *     - Combination filter can be applied as region filter, together with the gene filter.
     *     - ct, bt and flag filters do not need to be applied separately
     *  - Gene filter, and other region filters (e.g. region, id, xref):
     *     - We need to apply the combination (ct+bt+flag) for gene and non-gene variants
     *     - Combination filter is applied as region filter, together with the gene filter
     *     - Combination filter (without gene) is applied as filter, to be applied for non-gene variants
     *
     * @param parsedVariantQuery  Parsed variant query with the gene combination to be applied
     * @param filters             Filters
     * @param regionFilters       Region filters
     * @param pureGeneFilter      Is pure gene filter (no other region filters, and gene filter is applied as region filter)
     * @param hasGeneFilter       Has gene filter, either pure or not
     * @return  ctBtFlagApplied   ct+bt+flag filter fully applied as combination filter, no need to apply ct, bt and flag filters separately
     */
    private static boolean addGeneCombinationFilter(ParsedVariantQuery parsedVariantQuery, List<Bson> filters, List<Bson> regionFilters,
                                                    boolean pureGeneFilter, boolean hasGeneFilter) {
        boolean ctBtFlagApplied = false;
        if (parsedVariantQuery.getAnnotationQuery().getGeneCombinations() == null) {
            return ctBtFlagApplied;
        }
        if (hasGeneFilter && !pureGeneFilter) {
            // Gene filter is applied, but not as pure gene region filter, so we need to apply the
            // combination filter for non-gene variants as well
            ParsedVariantQuery.ConsequenceTypeCombinations combination = VariantQueryParser.parseGeneBtSoFlagCombination(
                    Collections.emptyList(), parsedVariantQuery.getInputQuery());
            if (combination != null) {
                addGeneCombinationFilter(combination, filters, regionFilters, false);
                ctBtFlagApplied = true;
            }
        } else {
            // No gene filter, or pure gene filter, so we can apply the combination filter directly
             ctBtFlagApplied = true;
        }
        addGeneCombinationFilter(parsedVariantQuery.getAnnotationQuery().getGeneCombinations(), filters, regionFilters, hasGeneFilter);
        return ctBtFlagApplied;
    }

    private static void addGeneCombinationFilter(ParsedVariantQuery.ConsequenceTypeCombinations combinations, List<Bson> filters,
                                                 List<Bson> regionFilters, boolean hasGeneFilter) {
        if (combinations != null) {
            List<Bson> combinationFilters = new ArrayList<>();
            String ctCombinedField = DocumentToVariantAnnotationConverter.CT_COMBINED;

            for (ParsedVariantQuery.ConsequenceTypeCombination combination : combinations.getCombinations()) {
                String gene = combination.getGene();
                String bt = combination.getBiotype() != null
                        ? DocumentToVariantAnnotationConverter.biotypeToStorage(combination.getBiotype()) : null;
                String so = combination.getSo() != null
                        ? String.valueOf(parseConsequenceType(combination.getSo())) : null;
                String flag = combination.getFlag() != null
                        ? DocumentToVariantAnnotationConverter.flagToStorage(combination.getFlag()) : null;

                String any = "[^" + SEPARATOR + "]+";
                String anySo = "\\d+";
                switch (combinations.getType()) {
                    case GENE_BIOTYPE_SO_FLAG:
                        // Exact match on 4-part: gene_biotype_so_flag
                        combinationFilters.add(eq(ctCombinedField,
                                gene + SEPARATOR + bt + SEPARATOR + so + SEPARATOR + flag));
                        break;
                    case GENE_BIOTYPE_SO:
                        // Expand all flag values → $in on 4-part
                        combinationFilters.add(in(ctCombinedField,
                                allCtCombinedValues(gene + SEPARATOR + bt + SEPARATOR + so + SEPARATOR)));
                        break;
                    case GENE_BIOTYPE:
                        // Prefix match on 4-part: gene_biotype_*_*
                        combinationFilters.add(regex(ctCombinedField,
                                "^" + Pattern.quote(gene + SEPARATOR + bt) + SEPARATOR));
                        break;
                    case GENE_BIOTYPE_FLAG:
                        // Prefix gene_biotype_, any so, specific flag on 4-part
                        combinationFilters.add(regex(ctCombinedField,
                                "^" + Pattern.quote(gene + SEPARATOR + bt) + SEPARATOR + anySo + SEPARATOR + Pattern.quote(flag) + "$"));
                        break;
                    case GENE_SO_FLAG:
                        // Skip biotype, anchored on gene prefix on 4-part
                        combinationFilters.add(regex(ctCombinedField,
                                "^" + Pattern.quote(gene) + SEPARATOR + any + SEPARATOR + so + SEPARATOR + Pattern.quote(flag) + "$"));
                        break;
                    case GENE_SO:
                        // Skip biotype+flag, anchored on gene prefix on 4-part
                        combinationFilters.add(regex(ctCombinedField,
                                "^" + Pattern.quote(gene) + SEPARATOR + any + SEPARATOR + so + SEPARATOR));
                        break;
                    case GENE_FLAG:
                        // Skip biotype+so, anchored on gene prefix on 4-part
                        combinationFilters.add(regex(ctCombinedField,
                                "^" + Pattern.quote(gene) + SEPARATOR + any + SEPARATOR + anySo + SEPARATOR + Pattern.quote(flag) + "$"));
                        break;
                    case BIOTYPE_SO_FLAG:
                        // Exact match on 3-part: biotype_so_flag
                        combinationFilters.add(eq(ctCombinedField,
                                bt + SEPARATOR + so + SEPARATOR + flag));
                        break;
                    case BIOTYPE_SO:
                        // Expand all flag values → $in on 3-part
                        combinationFilters.add(in(ctCombinedField,
                                allCtCombinedValues(bt + SEPARATOR + so + SEPARATOR)));
                        break;
                    case BIOTYPE_FLAG:
                        // Prefix biotype_, any so, specific flag on 3-part
                        combinationFilters.add(regex(ctCombinedField,
                                "^" + Pattern.quote(bt) + SEPARATOR + anySo + SEPARATOR + Pattern.quote(flag) + "$"));
                        break;
                    case SO_FLAG:
                        // Any biotype, specific so+flag on 3-part
                        combinationFilters.add(regex(ctCombinedField,
                                "^" + any + SEPARATOR + so + SEPARATOR + Pattern.quote(flag) + "$"));
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + combinations.getType());
                }
            }
            if (!combinationFilters.isEmpty()) {
                if (hasGeneFilter) {
                    // Apply as part of the Region Filter
                    regionFilters.add(or(combinationFilters));
                } else {
                    // No gene filter, so we can apply the combination filter directly
                    filters.add(combinationFilters.size() == 1
                            ? combinationFilters.get(0)
                            : or(combinationFilters));
                }
            }
        }
    }

    private List<Bson> parseAnnotationQueryParams(ParsedVariantQuery parsedVariantQuery, Query query, boolean ctBtFlagApplied) {
        List<Bson> filters = new ArrayList<>();

        if (query != null) {
            if (isValidParam(query, ANNOTATION_EXISTS)) {
                boolean exists = query.getBoolean(ANNOTATION_EXISTS.key());
                filters.add(exists(DocumentToVariantAnnotationConverter.ANNOT_ID,
                        exists));
                if (!exists) {
                    filters.add(exists(DocumentToVariantAnnotationConverter.CT_SO_ACCESSION, false));
                }
                // else , should be combined with an or, and it would not speed up the filtering. This scenario is not so common
            }

            if (!ctBtFlagApplied) {
                // If the combination of ct, bt and flag is not applied as region filter, we need to apply them separately
                if (isValidParam(query, ANNOT_CONSEQUENCE_TYPE)) {
                    String value = query.getString(ANNOT_CONSEQUENCE_TYPE.key());
                    addQueryFilter(DocumentToVariantAnnotationConverter.CT_SO_ACCESSION, value, filters,
                            VariantQueryUtils::parseConsequenceType);
                }

                if (isValidParam(query, ANNOT_BIOTYPE)) {
                    String biotypes = query.getString(ANNOT_BIOTYPE.key());
                    addQueryStringFilter(DocumentToVariantAnnotationConverter.CT_BIOTYPE, biotypes, filters);
                }

                if (isValidParam(query, ANNOT_TRANSCRIPT_FLAG)) {
                    String value = query.getString(ANNOT_TRANSCRIPT_FLAG.key());
                    addQueryStringFilter(DocumentToVariantAnnotationConverter.CT_TRANSCRIPT_ANNOT_FLAGS, value, filters);
                }
            }

            if (isValidParam(query, ANNOT_POLYPHEN)) {
                String value = query.getString(ANNOT_POLYPHEN.key());
//                addCompListQueryFilter(DocumentToVariantConverter.ANNOTATION_FIELD
//                                + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
//                                + "." + DocumentToVariantAnnotationConverter.CT_PROTEIN_POLYPHEN_FIELD
//                                + "." + DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD,
//                        value, builder);
                addScoreFilter(value, filters, ANNOT_POLYPHEN, DocumentToVariantAnnotationConverter.POLYPHEN, true);
            }

            if (isValidParam(query, ANNOT_SIFT)) {
                String value = query.getString(ANNOT_SIFT.key());
//                addCompListQueryFilter(DocumentToVariantConverter.ANNOTATION_FIELD
//                        + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
//                        + "." + DocumentToVariantAnnotationConverter.CT_PROTEIN_SIFT_FIELD + "."
//                        + DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD, value, builder);
                addScoreFilter(value, filters, ANNOT_SIFT, DocumentToVariantAnnotationConverter.SIFT, true);
            }

            if (isValidParam(query, ANNOT_PROTEIN_SUBSTITUTION)) {
                String value = query.getString(ANNOT_PROTEIN_SUBSTITUTION.key());
                addScoreFilter(value, filters, ANNOT_PROTEIN_SUBSTITUTION, true);
            }

            if (isValidParam(query, ANNOT_CONSERVATION)) {
                String value = query.getString(ANNOT_CONSERVATION.key());
                addScoreFilter(value, filters, ANNOT_CONSERVATION, false);
            }

            /* FIXME: TASK-8038
            if (isValidParam(query, ANNOT_GENE_TRAIT_ID)) {
                String value = query.getString(ANNOT_GENE_TRAIT_ID.key());
                QueryOperation internalOp = checkOperator(value);
                List<String> values = splitValue(value, internalOp);
                QueryBuilder geneTraitBuilder;
                if (internalOp == QueryOperation.OR) {
                    geneTraitBuilder = QueryBuilder.start();
                } else {
                    geneTraitBuilder = builder;
                }


                List<String> geneTraitId = new LinkedList<>();
                List<String> hpo = new LinkedList<>();
                for (String v : values) {
                    if (isHpo(v)) {
                        hpo.add(v);
                    } else {
                        geneTraitId.add(v);
                    }
                }

                if (!geneTraitId.isEmpty()) {
                    addQueryFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                                    + '.' + DocumentToVariantAnnotationConverter.GENE_TRAIT_FIELD
                                    + '.' + DocumentToVariantAnnotationConverter.GENE_TRAIT_ID_FIELD, geneTraitId, geneTraitBuilder,
                            internalOp, internalOp, Object::toString);
                }

                if (!hpo.isEmpty()) {
                    addQueryFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                                    + '.' + DocumentToVariantAnnotationConverter.XREFS_FIELD
                                    + '.' + DocumentToVariantAnnotationConverter.XREF_ID_FIELD, hpo, geneTraitBuilder,
                            internalOp, internalOp, Object::toString);
                }

                if (internalOp == QueryOperation.OR) {
                    builder.and(geneTraitBuilder.get());
                }
            }
*/
            if (isValidParam(query, ANNOT_GENE_TRAIT_NAME)) {
                String value = query.getString(ANNOT_GENE_TRAIT_NAME.key());
//                addCompQueryFilter(DocumentToVariantConverter.ANNOTATION_FIELD
//                        + '.' + DocumentToVariantAnnotationConverter.GENE_TRAIT_FIELD
//                        + '.' + DocumentToVariantAnnotationConverter.GENE_TRAIT_NAME_FIELD, value, builder, false);
                addQueryStringFilter(DocumentToVariantAnnotationConverter.GENE_TRAIT_NAME, value, filters);
            }

            List<List<String>> clinicalCombinationFilter = VariantQueryParser.parseClinicalCombination(query);
            if (!clinicalCombinationFilter.isEmpty()) {
                String key = DocumentToVariantAnnotationConverter.CLINICAL_COMBINATIONS;
                if (clinicalCombinationFilter.size() == 1) {
                    filters.add(in(key, clinicalCombinationFilter.get(0)));
                } else {
                    List<Bson> subQueries = new ArrayList<>(clinicalCombinationFilter.size());
                    for (List<String> clinicalCombination : clinicalCombinationFilter) {
                        subQueries.add(in(key, clinicalCombination));
                    }
                    // FIXME: why not to flatten here?
                    filters.add(and(subQueries));
                }
            }

            if (isValidParam(query, ANNOT_HPO)) {
                String value = query.getString(ANNOT_HPO.key());
//                addQueryStringFilter(DocumentToVariantAnnotationConverter.GENE_TRAIT_HPO_FIELD, value, geneTraitBuilder,
//                        QueryOperation.AND);
                addQueryStringFilter(DocumentToVariantAnnotationConverter.XREFS_ID, value, filters);
            }

//            DBObject geneTraitQuery = geneTraitBuilder.get();
//            if (geneTraitQuery.keySet().size() != 0) {
//                builder.and(DocumentToVariantConverter.ANNOTATION_FIELD
//                        + "." + DocumentToVariantAnnotationConverter.GENE_TRAIT_FIELD).elemMatch(geneTraitQuery);
//            }

            if (isValidParam(query, ANNOT_GO_GENES)) {
                String value = query.getString(ANNOT_GO_GENES.key());

                // Check if comma separated of semi colon separated (AND or OR)
                QueryOperation queryOperation = checkOperator(value);
                // Split by comma or semi colon
                List<String> goGenes = splitValue(value, queryOperation);

                filters.add(in(DocumentToVariantAnnotationConverter.XREFS_ID, goGenes));

            }

            if (isValidParam(query, ANNOT_EXPRESSION_GENES)) {
                String value = query.getString(ANNOT_EXPRESSION_GENES.key());

                // Check if comma separated of semi colon separated (AND or OR)
                QueryOperation queryOperation = checkOperator(value);
                // Split by comma or semi colon
                List<String> expressionGenes = splitValue(value, queryOperation);

                filters.add(in(DocumentToVariantAnnotationConverter.XREFS_ID, expressionGenes));

            }


            if (isValidParam(query, ANNOT_PROTEIN_KEYWORD)) {
                String value = query.getString(ANNOT_PROTEIN_KEYWORD.key());
                addQueryStringFilter(DocumentToVariantAnnotationConverter.CT_PROTEIN_KEYWORDS, value, filters);
            }

            if (isValidParam(query, ANNOT_DRUG)) {
                String value = query.getString(ANNOT_DRUG.key());
                addQueryStringFilter(DocumentToVariantAnnotationConverter.DRUG_NAME, value, filters);
            }

            if (isValidParam(query, ANNOT_FUNCTIONAL_SCORE)) {
                String value = query.getString(ANNOT_FUNCTIONAL_SCORE.key());
                addScoreFilter(value, filters, ANNOT_FUNCTIONAL_SCORE, false);
            }
            /* FIXME: TASK-8038
            if (isValidParam(query, CUSTOM_ANNOTATION)) {
                String value = query.getString(CUSTOM_ANNOTATION.key());
                addCompListQueryFilter(DocumentToVariantConverter.CUSTOM_ANNOTATION_FIELD, value, f, true);
            }
*/
            if (isValidParam(query, ANNOT_POPULATION_ALTERNATE_FREQUENCY)) {
                String value = query.getString(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key());
                addFrequencyFilter(DocumentToVariantAnnotationConverter.POPULATION_FREQUENCIES,
                        DocumentToVariantAnnotationConverter.POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD, value, filters,
                        ANNOT_POPULATION_ALTERNATE_FREQUENCY, true); // Same
                // method addFrequencyFilter is used for reference and allele frequencies. Need to provide the field
                // (reference/alternate) where to check the frequency
            }

            if (isValidParam(query, ANNOT_POPULATION_REFERENCE_FREQUENCY)) {
                String value = query.getString(ANNOT_POPULATION_REFERENCE_FREQUENCY.key());
                addFrequencyFilter(DocumentToVariantAnnotationConverter.POPULATION_FREQUENCIES,
                        DocumentToVariantAnnotationConverter.POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD, value, filters,
                        ANNOT_POPULATION_REFERENCE_FREQUENCY, false); // Same
                // method addFrequencyFilter is used for reference and allele frequencies. Need to provide the field
                // (reference/alternate) where to check the frequency
            }

            /* FIXME: TASK-8038
            if (isValidParam(query, ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY)) {
                String value = query.getString(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key());
                addFrequencyFilter(DocumentToVariantConverter.ANNOTATION_FIELD + "."
                                + DocumentToVariantAnnotationConverter.POPULATION_FREQUENCIES_FIELD,
                        value, builder, ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY, true,
                        (v, queryBuilder) -> {
                            String[] split = splitOperator(v);
                            String op = split[1];
                            String obj = split[2];

                            double aDouble = Double.parseDouble(obj);
                            switch (op) {
                                case "<":
                                    queryBuilder.or(QueryBuilder.start(DocumentToVariantAnnotationConverter.
                                                    POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD).lessThan(aDouble).get(),
                                            QueryBuilder.start(DocumentToVariantAnnotationConverter.
                                                    POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD).lessThan(aDouble).get()
                                    );
                                    break;
                                case "<=":
                                    queryBuilder.or(QueryBuilder.start(DocumentToVariantAnnotationConverter.
                                                    POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD).lessThanEquals(aDouble).get(),
                                            QueryBuilder.start(DocumentToVariantAnnotationConverter.
                                                    POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD).lessThanEquals(aDouble).get()
                                    );
                                    break;
                                case ">":
                                    queryBuilder.and(DocumentToVariantAnnotationConverter.
                                            POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD).greaterThan(aDouble)
                                            .and(DocumentToVariantAnnotationConverter.
                                                    POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD).greaterThan(aDouble);
                                    break;
                                case ">=":
                                    queryBuilder.and(DocumentToVariantAnnotationConverter.
                                            POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD).greaterThanEquals(aDouble)
                                            .and(DocumentToVariantAnnotationConverter.
                                                    POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD).greaterThanEquals(aDouble);
                                    break;
                                default:
                                    throw new IllegalArgumentException("Unsupported operator '" + op + "'");
                            }
                        });
            }
*/
            if (isValidParam(query, VARIANTS_TO_INDEX)) {
                long ts = metadataManager.getProjectMetadata().getSecondaryAnnotationIndex().getSearchIndexMetadataForLoading()
                        .getLastUpdateDateTimestamp();
                if (ts > 0) {
                    String key = INDEX_FIELD + '.' + DocumentToVariantConverter.INDEX_TIMESTAMP_FIELD;
                    filters.add(gte(key, ts));
                } // Otherwise, get all variants
            }
        }
        return filters;
    }

    private List<Bson> parseStudyQueryParams(ParsedVariantQuery parsedVariantQuery, @Deprecated Query query) {
        List<Bson> filters = new ArrayList<>();
        if (query == null) {
            return filters;
        }

        String studyQueryPrefix = DocumentToVariantConverter.STUDIES_FIELD + '.';
        final StudyMetadata defaultStudy = parsedVariantQuery.getStudyQuery().getDefaultStudy();


        if (parsedVariantQuery.getStudyQuery().getStudies() != null) {
            ParsedQuery<NegatableValue<ResourceId>> studies = parsedVariantQuery.getStudyQuery().getStudies();

            filters.add(addQueryFilter(studyQueryPrefix + DocumentToStudyVariantEntryConverter.STUDYID_FIELD,
                    studies.getValues(),
                    study -> study.getValue().getId()));
        }

        boolean overlappedFilesFiles = query.getBoolean(OVERLAPPED_FILES_ONLY);
        List<Integer> fileIds = Collections.emptyList();
        QueryOperation filesOperation = QueryOperation.OR;
        ParsedQuery<NegatableValue<ResourceId>> fileQuery = null;
        if (parsedVariantQuery.getStudyQuery().getFiles() != null) {
            fileQuery = parsedVariantQuery.getStudyQuery().getFiles();
            filesOperation = fileQuery.getOperation();
            fileIds = fileQuery.getValues().stream().filter(v -> !v.isNegated())
                    .map(NegatableValue::getValue).map(ResourceId::getId).collect(Collectors.toList());

        } else if (isValidParam(query, INCLUDE_FILE)) {
            List<String> files = VariantQueryProjectionParser.getIncludeFilesList(query);
            if (files != null) {
                fileIds = new ArrayList<>(files.size());
                for (String file : files) {
                    fileIds.add(metadataManager.getFileIdPair(file, false, defaultStudy).getValue());
                }
            }
        }

        if (isValidParam(query, FILTER) || isValidParam(query, QUAL) || isValidParam(query, FILE_DATA)) {
            String filterValue = query.getString(FILTER.key());
            QueryOperation filterOperation = checkOperator(filterValue);
            List<String> filterValues = splitValue(filterValue, filterOperation);
            ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> parsedFileData = parseFileData(query);
            QueryOperation fileDataOperation = parsedFileData.getOperation();


            boolean useFileElemMatch = !fileIds.isEmpty();
            boolean infoInFileElemMatch = useFileElemMatch && (fileDataOperation == null || filesOperation == fileDataOperation);

//                values = query.getString(QUAL.key());
//                QueryOperation qualOperation = checkOperator(values);
//                List<String> qualValues = splitValue(values, qualOperation);
            if (!useFileElemMatch) {
                String key = studyQueryPrefix
                        + DocumentToStudyVariantEntryConverter.FILES_FIELD + '.'
                        + DocumentToStudyVariantEntryConverter.ATTRIBUTES_FIELD + '.';

                if (isValidParam(query, FILTER)) {
                    getFileFilter(key + StudyEntry.FILTER, filterValues, filterOperation, filters);
                }
                if (isValidParam(query, QUAL)) {
                    addCompListQueryFilter(key + StudyEntry.QUAL, query.getString(QUAL.key()), filters, false);
                }
            } else {
                List<Bson> fileElemMatch = new ArrayList<>(fileIds.size());
                String key = DocumentToStudyVariantEntryConverter.ATTRIBUTES_FIELD + '.';

                for (Integer fileId : fileIds) {
                    List<Bson> fileFilters = new ArrayList<>();

                    fileFilters.add(eq(DocumentToStudyVariantEntryConverter.FILEID_FIELD, fileId));
                    if (isValidParam(query, FILTER)) {
                        getFileFilter(key + StudyEntry.FILTER, filterValues, filterOperation, fileFilters);
                    }
                    if (isValidParam(query, QUAL)) {
                        addCompListQueryFilter(key + StudyEntry.QUAL, query.getString(QUAL.key()), fileFilters, false);
                    }

                    if (infoInFileElemMatch && !parsedFileData.getValues().isEmpty()) {
                        if (defaultStudy == null) {
                            throw VariantQueryException.missingStudyForFile(fileId.toString(),
                                    metadataManager.getStudyNames());
                        }
                        String fileName = metadataManager.getFileName(defaultStudy.getId(), fileId);
                        KeyValues<String, KeyOpValue<String, String>> fileDataValue =
                                parsedFileData.getValue(kv -> kv.getKey().equals(fileName));
                        if (fileDataValue.getValues() != null) {
                            KeyOpValue<String, String> filterKeyOp = fileDataValue.getValue(s -> s.getKey().equals(StudyEntry.FILTER));
                            Values<Bson> extraFilterFilters = null;
                            if (filterKeyOp != null) {
                                fileDataValue.getValues().remove(filterKeyOp);
                                Values<String> splitFilterValues = splitValues(filterKeyOp.getValue());
                                List<Bson> filterFilters = getFileFilter(key + StudyEntry.FILTER, splitFilterValues.getValues());
                                extraFilterFilters = new Values<>(splitFilterValues.getOperation(), filterFilters);
                            }
                            addCompListQueryFilter(DocumentToStudyVariantEntryConverter.ATTRIBUTES_FIELD, fileDataValue, fileFilters,
                                    true, extraFilterFilters);
                        }
                    }
                    fileElemMatch.add(elemMatch(studyQueryPrefix + DocumentToStudyVariantEntryConverter.FILES_FIELD, and(fileFilters)));
                }
                addAll(filters, filesOperation, fileElemMatch);

            }

            if (!infoInFileElemMatch && !parsedFileData.getValues().isEmpty()) {
                List<Bson> infoElemMatch = new ArrayList<>(parsedFileData.getValues().size());
                for (KeyValues<String, KeyOpValue<String, String>> fileDataValue : parsedFileData.getValues()) {
                    if (defaultStudy == null) {
                        throw VariantQueryException.missingStudyForFile(fileDataValue.getKey(), metadataManager.getStudyNames());
                    }
                    List<Bson> infoFilters = new ArrayList<>();
                    Integer fileId = metadataManager.getFileId(defaultStudy.getId(), fileDataValue.getKey(), true);
                    infoFilters.add(eq(DocumentToStudyVariantEntryConverter.FILEID_FIELD, fileId));
                    if (fileDataValue.getValues() != null) {
                        KeyOpValue<String, String> filterKeyOp = fileDataValue.getValue(s -> s.getKey().equals(StudyEntry.FILTER));
                        Values<Bson> extraFilterFilters = null;
                        if (filterKeyOp != null) {
                            fileDataValue.getValues().remove(filterKeyOp);
                            Values<String> splitFilterValues = splitValues(filterKeyOp.getValue());
                            String key = DocumentToStudyVariantEntryConverter.ATTRIBUTES_FIELD + ".";
                            List<Bson> filterFilters = getFileFilter(key + StudyEntry.FILTER, splitFilterValues.getValues());
                            extraFilterFilters = new Values<>(splitFilterValues.getOperation(), filterFilters);
                        }

                        addCompListQueryFilter(DocumentToStudyVariantEntryConverter.ATTRIBUTES_FIELD, fileDataValue, infoFilters,
                                true, extraFilterFilters);
                    }
                    infoElemMatch.add(elemMatch(studyQueryPrefix + DocumentToStudyVariantEntryConverter.FILES_FIELD, and(infoFilters)));
                }
                addAll(filters, fileDataOperation, infoElemMatch);
            }
        }

        if (isValidParam(query, SAMPLE_DATA)) {
            throw VariantQueryException.unsupportedVariantQueryFilter(SAMPLE_DATA, MongoDBVariantStorageEngine.STORAGE_ENGINE_ID);
        }

        // Only will contain values if the genotypesOperator is AND
        Set<List<Integer>> fileIdGroupsFromSamples = Collections.emptySet();
        Set<Integer> fileIdsFromSamples = Collections.emptySet();
        ParsedQuery<KeyOpValue<SampleMetadata, List<String>>> genotypesQuery = parsedVariantQuery.getStudyQuery().getGenotypes();
        if (genotypesQuery != null) {
            // One bson query per sample
            List<Bson> genotypeQueries = new ArrayList<>(genotypesQuery.getValues().size());

            fileIdGroupsFromSamples = new LinkedHashSet<>();
            fileIdsFromSamples = new LinkedHashSet<>();

            List<String> defaultGenotypes;
            List<String> loadedGenotypes;
            if (defaultStudy != null) {
                defaultGenotypes = defaultStudy.getAttributes().getAsStringList(DEFAULT_GENOTYPE.key());
                loadedGenotypes = defaultStudy.getAttributes().getAsStringList(LOADED_GENOTYPES.key());
                loadedGenotypes.replaceAll(DocumentToSamplesConverter::genotypeToDataModelType);
            } else {
                defaultGenotypes = DEFAULT_GENOTYPE.defaultValue();
                loadedGenotypes = Arrays.asList(
                        "0/0", "0|0",
                        "0/1", "1/0", "1/1", "./.",
                        "0|1", "1|0", "1|1", ".|.",
                        "0|2", "2|0", "2|1", "1|2", "2|2",
                        "0/2", "2/0", "2/1", "1/2", "2/2",
                        GenotypeClass.UNKNOWN_GENOTYPE);
            }

            for (KeyOpValue<SampleMetadata, List<String>> sampleGenotypeFilter : genotypesQuery.getValues()) {
                SampleMetadata sample = sampleGenotypeFilter.getKey();
                List<String> genotypes = sampleGenotypeFilter.getValue();
                // Build the query for this sample
                // {
                //   $and: [
                //      <genotypesFiltersAnd>... ,
                //      {$or : <genotypesFiltersOr> }
                //   ]
                // }
                //
                // e.g.
                // genotype="0/1,1/1" -> genotype = "0/1" OR genotype = "1/1"
                // {
                //   $and: [
                //      { $or: [ {"gt.0|1" : <sampleId> } , {"gt.1|1" : <sampleId> } ] }
                //      { "files.fileId" : { $in : [<fileIdFromSample>] } }
                //   ]
                // }

                List<Bson> genotypesFiltersAnd = new ArrayList<>();
                List<Bson> genotypesFiltersOr = new ArrayList<>();

                // If empty, should find none. Add non-existing genotype
                // TODO: Fast empty result
                if (genotypes.isEmpty()) {
                    genotypes.add(GenotypeClass.NONE_GT_VALUE);
                }

                int sampleId = sample.getId();

                boolean canFilterSampleByFile = true;
                boolean defaultGenotypeNegated = false;

                for (String genotype : genotypes) {
                    if (genotype.equals(GenotypeClass.UNKNOWN_GENOTYPE)) {
                        // We can not filter sample by file if one of the requested genotypes is the unknown genotype
                        canFilterSampleByFile = false;
                        break;
                    } else if (isNegated(genotype)) {
                        // Do not filter sample by file if the genotypes are negated, unless is a defaultGenotype
                        if (defaultGenotypes.contains(removeNegation(genotype))) {
                            canFilterSampleByFile = true;
                            defaultGenotypeNegated = true;
                        } else {
                            canFilterSampleByFile = false;
                            break;
                        }
                    }
                }

                if (canFilterSampleByFile) {
                    // Extra filter by FILE IDs associated to the sample
                    List<Integer> fileIdsFromSample = metadataManager.getFileIdsFromSampleId(defaultStudy.getId(), sampleId, true);

                    String key = studyQueryPrefix + DocumentToStudyVariantEntryConverter.FILES_FIELD
                            + '.' + DocumentToStudyVariantEntryConverter.FILEID_FIELD;
                    if (defaultGenotypeNegated) {
                        Bson negatedFile;
                        if (fileIdsFromSample.size() == 1) {
                            negatedFile = ne(key, fileIdsFromSample.get(0));
                        } else {
                            negatedFile = nin(key, fileIdsFromSample);
                        }
                        genotypesFiltersOr.add(negatedFile);
                    } else if (genotypesQuery.getOperation() == QueryOperation.OR) {
                        if (fileIdsFromSample.size() == 1) {
                            genotypesFiltersAnd.add(eq(key, fileIdsFromSample.get(0)));
                        } else {
                            genotypesFiltersAnd.add(in(key, fileIdsFromSample));
                        }
                    } else {
                        // FILE ID filter can be added at the end, together with the main FILE filter
                        fileIdGroupsFromSamples.add(fileIdsFromSample);
                        fileIdsFromSamples.addAll(fileIdsFromSample);
                    }
                }

                for (String genotype : genotypes) {
                    if (genotype.equals(GenotypeClass.NA_GT_VALUE)) {
                        continue;
                    }

                    boolean negated = isNegated(genotype);
                    if (negated) {
                        genotype = removeNegation(genotype);
                    }
                    if (defaultGenotypes.contains(genotype)) {
                        // Filter by default genotype, which is missing
                        // This means that we need to find the sample in all genotypes different than the default genotype
                        if (negated) {
                            for (String otherGenotype : loadedGenotypes) {
                                if (defaultGenotypes.contains(otherGenotype)) {
                                    continue;
                                }
                                String key = studyQueryPrefix
                                        + DocumentToStudyVariantEntryConverter.GENOTYPES_FIELD
                                        + '.' + DocumentToSamplesConverter.genotypeToStorageType(otherGenotype);
                                genotypesFiltersOr.add(eq(key, sampleId));
                            }
                        } else {
                            List<Bson> defaultGenotypeFilter = new ArrayList<>();
                            for (String otherGenotype : loadedGenotypes) {
                                if (defaultGenotypes.contains(otherGenotype)) {
                                    continue;
                                }
                                String key = studyQueryPrefix
                                        + DocumentToStudyVariantEntryConverter.GENOTYPES_FIELD
                                        + '.' + DocumentToSamplesConverter.genotypeToStorageType(otherGenotype);
                                defaultGenotypeFilter.add(ne(key, sampleId));
                            }
                            genotypesFiltersOr.add(and(defaultGenotypeFilter));
                        }
                    } else {
                        String key = studyQueryPrefix
                                + DocumentToStudyVariantEntryConverter.GENOTYPES_FIELD
                                + '.' + DocumentToSamplesConverter.genotypeToStorageType(genotype);
                        if (negated) {
                            //and [ {"gt.0|1" : { $ne : <sampleId> } } ]
                            genotypesFiltersAnd.add(ne(key, sampleId));
                        } else {
                            //or [ {"gt.0|1" : <sampleId> } ]
                            genotypesFiltersOr.add(eq(key, sampleId));
                        }
                    }
                }

                if (!genotypesFiltersOr.isEmpty()) {
                    genotypesFiltersAnd.add(0, or(genotypesFiltersOr));
                }
                if (genotypesFiltersAnd.size() == 1) {
                    genotypeQueries.add(genotypesFiltersAnd.get(0));
                } else {
                    genotypeQueries.add(and(genotypesFiltersAnd));
                }
            }


            addAll(filters, genotypesQuery.getOperation(), genotypeQueries);
        }

        if (fileQuery == null) {
            // If there is no valid files filter, add files filter to speed up this query
            if (!fileIdGroupsFromSamples.isEmpty()) {
                if ((genotypesQuery.getOperation() != QueryOperation.AND || fileIdGroupsFromSamples.size() == 1)
                        && fileIdsFromSamples.containsAll(metadataManager.getIndexedFiles(defaultStudy.getId()))) {
                    // Do not add files filter if operator is OR and must select all the files.
                    // i.e. ANY file among ALL indexed files
                    logger.debug("Skip filter by all files");
                } else {
                    addFileGroupsFilter(filters, studyQueryPrefix, genotypesQuery.getOperation(), fileIdGroupsFromSamples, null);
                }
            }
        } else {
            if (fileIdGroupsFromSamples.isEmpty()) {
                filters.add(addQueryFilter(studyQueryPrefix + DocumentToStudyVariantEntryConverter.FILES_FIELD
                                + '.' + DocumentToStudyVariantEntryConverter.FILEID_FIELD,
                        fileQuery.getValues(), filesOperation,
                        value -> {
                            int fileId = value.getValue().getId();
                            if (overlappedFilesFiles) {
                                return -fileId;
                            } else {
                                return fileId;
                            }
                        }));
            } else {
                // fileIdGroupsFromSamples is not empty. gtQueryOperation is always AND at this point
                // assert gtQueryOperation == Operation.AND || gtQueryOperation == null
                if (filesOperation == QueryOperation.AND || filesOperation == null) {
                    // sample = AND, files = AND
                    // Simple mix

                    // Some files may be negated. Get them, and put them appart
                    List<Integer> negatedFiles = null;
                    if (fileQuery.stream().anyMatch(NegatableValue::isNegated)) {
                        negatedFiles = fileQuery
                                .stream()
                                .filter(NegatableValue::isNegated)
                                .map(value -> {
                                    int fileId = value.getValue().getId();
                                    if (overlappedFilesFiles) {
                                        return -fileId;
                                    } else {
                                        return fileId;
                                    }
                                })
                                .collect(Collectors.toList());
                    }
                    for (Integer fileId : fileIds) {
                        fileIdGroupsFromSamples.add(Collections.singletonList(fileId));
                    }
                    addFileGroupsFilter(filters, studyQueryPrefix, QueryOperation.AND, fileIdGroupsFromSamples, negatedFiles);

                } else if (filesOperation == QueryOperation.OR) {
                    // samples = AND, files = OR

                    // Put all files in a group
                    // the filesOperation==OR will be expressed with an "$in" of the new group
                    fileIdGroupsFromSamples.add(fileIds);
                    addFileGroupsFilter(filters, studyQueryPrefix, QueryOperation.AND, fileIdGroupsFromSamples, null);
                }
            }

        }

        return filters;
    }

    private static void addAll(List<Bson> filters, QueryOperation filterOperation, List<Bson> filterFilters) {
        if (filterOperation == QueryOperation.OR) {
            filters.add(or(filterFilters));
        } else {
            filters.add(and(filterFilters));
//            filters.addAll(filterFilters);
        }
    }

    private void addFileGroupsFilter(List<Bson> filters, String studyQueryPrefix, QueryOperation operation,
                                     Set<List<Integer>> fileIdGroups, List<Integer> negatedFiles) {

        if (operation == QueryOperation.OR) {
            // Merge into one single group
            HashSet<Integer> fileIds = new HashSet<>();
            for (List<Integer> files : fileIdGroups) {
                fileIds.addAll(files);
            }
            fileIdGroups = Collections.singleton(new ArrayList<>(fileIds));
        }

        String fileIdField = studyQueryPrefix + DocumentToStudyVariantEntryConverter.FILES_FIELD
                + '.' + DocumentToStudyVariantEntryConverter.FILEID_FIELD;
        List<Bson> fileQueries = new ArrayList<>(fileIdGroups.size());
        List<Integer> singleElementGroups = new ArrayList<>();
        for (List<Integer> group : fileIdGroups) {
            if (group.size() == 1) {
                singleElementGroups.add(group.get(0));
            } else {
                fileQueries.add(in(fileIdField, group));
            }
        }
        if (!singleElementGroups.isEmpty()) {
            if (singleElementGroups.size() == 1) {
                fileQueries.add(eq(fileIdField, singleElementGroups.get(0)));
            } else if (operation == QueryOperation.AND) {
                fileQueries.add(all(fileIdField, singleElementGroups));
            } else {
                fileQueries.add(in(fileIdField, singleElementGroups));
            }
        }

        if (CollectionUtils.isNotEmpty(negatedFiles)) {
            Bson negatedFilter;
            if (negatedFiles.size() == 1) {
                negatedFilter = ne(fileIdField, negatedFiles.get(0));
            } else {
                negatedFilter = nin(fileIdField, negatedFiles);
            }
            if (operation != QueryOperation.OR) {
                fileQueries.add(negatedFilter);
            } else {
                // This should never happen
                throw VariantQueryException.internalException(new IllegalStateException("Unsupported negated files with operator OR"));
            }
        }

        if (fileQueries.size() == 1) {
            filters.add(fileQueries.get(0));
        } else if (operation == QueryOperation.OR) {
            filters.add(Filters.or(fileQueries));
        } else {
//            filters.add(Filters.and(fileQueries));
            filters.addAll(fileQueries);
        }
    }

    private void getFileFilter(String key, List<String> filterValues, QueryOperation op, List<Bson> filters) {
        addAll(filters, op, getFileFilter(key, filterValues));
    }

    private List<Bson> getFileFilter(String key, List<String> filterValues) {
        List<Bson> filters = new ArrayList<>(filterValues.size());
        for (String value : filterValues) {
            final Bson filter;
            boolean negated = isNegated(value);
            if (negated) {
                value = removeNegation(value);
            }
            if (value.contains(VCFConstants.FILTER_CODE_SEPARATOR) || value.equals(VCFConstants.PASSES_FILTERS_v4)) {
                if (!negated) {
                    filter = eq(key, value);
                } else {
                    filter = ne(key, value);
                }
            } else {
                if (!negated) {
                    filter = regex(key, value);
                } else {
                    filter = not(regex(key, value));
                }
            }
            filters.add(filter);
        }
        return filters;
    }

    private void parseStatsQueryParams(ParsedVariantQuery parsedVariantQuery, Query query, List<Bson> filters) {

        if (query != null) {
            StudyMetadata defaultStudy = parsedVariantQuery.getStudyQuery().getDefaultStudy();

            if (query.get(COHORT.key()) != null && !query.getString(COHORT.key()).isEmpty()) {
                addQueryFilter(DocumentToVariantConverter.STATS_FIELD
                                + '.' + DocumentToVariantStatsConverter.COHORT_ID,
                        query.getString(COHORT.key()), filters,
                        s -> {
                            try {
                                return Integer.parseInt(s);
                            } catch (NumberFormatException ignore) {
                                String[] split = VariantQueryUtils.splitStudyResource(s);
                                if (defaultStudy == null && split.length == 1) {
                                    throw VariantQueryException.malformedParam(COHORT, s, "Expected {study}:{cohort}");
                                } else {
                                    String study;
                                    String cohort;
                                    Integer cohortId;
                                    if (defaultStudy != null && split.length == 1) {
                                        cohort = s;
                                        cohortId = metadataManager.getCohortId(defaultStudy.getId(), cohort);
                                        if (cohortId == null) {
                                            List<String> availableCohorts = new LinkedList<>();
                                            metadataManager.cohortIterator(defaultStudy.getId())
                                                    .forEachRemaining(c -> availableCohorts.add(c.getName()));
                                            throw VariantQueryException.cohortNotFound(cohort, defaultStudy.getId(), availableCohorts);
                                        }
                                    } else {
                                        study = split[0];
                                        cohort = split[1];
                                        int studyId = metadataManager.getStudyId(study);
                                        cohortId = metadataManager.getCohortId(studyId, cohort);
                                    }
                                    return cohortId;
                                }
                            }
                        });
            }

            if (isValidParam(query, STATS_REF)) {
                addStatsFilterList(DocumentToVariantStatsConverter.REF_FREQ_FIELD, query.getString(STATS_REF.key()),
                        filters, defaultStudy);
            }

            if (isValidParam(query, STATS_ALT)) {
                addStatsFilterList(DocumentToVariantStatsConverter.ALT_FREQ_FIELD, query.getString(STATS_ALT.key()),
                        filters, defaultStudy);
            }

            if (isValidParam(query, STATS_MAF)) {
                addStatsFilterList(DocumentToVariantStatsConverter.MAF_FIELD, query.getString(STATS_MAF.key()),
                        filters, defaultStudy);
            }

            if (isValidParam(query, STATS_MGF)) {
                addStatsFilterList(DocumentToVariantStatsConverter.MGF_FIELD, query.getString(STATS_MGF.key()),
                        filters, defaultStudy);
            }

            if (isValidParam(query, STATS_PASS_FREQ)) {
                addStatsFilterList(DocumentToVariantStatsConverter.FILTER_FREQ_FIELD + '.' + VCFConstants.PASSES_FILTERS_v4,
                        query.getString(STATS_PASS_FREQ.key()), filters, defaultStudy);
            }

            if (isValidParam(query, MISSING_ALLELES)) {
                addStatsFilterList(DocumentToVariantStatsConverter.MISSALLELE_FIELD, query.getString(MISSING_ALLELES
                        .key()), filters, defaultStudy);
            }

            if (isValidParam(query, MISSING_GENOTYPES)) {
                addStatsFilterList(DocumentToVariantStatsConverter.MISSGENOTYPE_FIELD, query.getString(
                        MISSING_GENOTYPES.key()), filters, defaultStudy);
            }
            /* FIXME: TASK-8038
            if (query.get("numgt") != null && !query.getString("numgt").isEmpty()) {
                for (String numgt : query.getAsStringList("numgt")) {
                    String[] split = numgt.split(":");
                    addCompQueryFilter(DocumentToVariantConverter.STATS_FIELD + '.'
                                    + DocumentToVariantStatsConverter.GENOTYPE_COUNT_FIELD + '.'
                                    + split[0],
                            split[1], builder, false);
                }
            }
             */
        }
    }

    protected Bson createProjection(Query query, QueryOptions options) {
        return createProjection(query, options, VariantQueryProjectionParser.parseVariantQueryFields(query, options, metadataManager));
    }

    protected Bson createProjection(Query query, QueryOptions options, VariantQueryProjection selectVariantElements) {
        if (options == null) {
            options = new QueryOptions();
        }

        Set<String> projections = new HashSet<>();
        Bson studyElemMatch = null;

        if (options.containsKey(QueryOptions.SORT) && !("_id").equals(options.getString(QueryOptions.SORT))) {
            if (options.getBoolean(QueryOptions.SORT)) {
                options.put(QueryOptions.SORT, "_id");
                options.putIfAbsent(QueryOptions.ORDER, QueryOptions.ASCENDING);
            } else {
                options.remove(QueryOptions.SORT);
            }
        }

        Set<VariantField> fields = new HashSet<>(selectVariantElements.getFields());
        // Add all required fields
        fields.addAll(DocumentToVariantConverter.REQUIRED_FIELDS_SET);
        // StudyID is mandatory if returning any STUDY element
        if (fields.contains(VariantField.STUDIES)) {
            fields.add(VariantField.STUDIES_STUDY_ID);
        }


        // Top level $elemMatch MUST be at the very beginning in the projection document, so all the fields apply correctly.
        //
        // This two queries return different values:
        //
        // > db.variants.find({}, {studies:{$elemMatch:{sid:1}}, "studies.files":1})
        // {  studies : [ { sid : 1, files : [ ... ] } ]  }
        //
        // > db.variants.find({}, {"studies.files":1, studies:{$elemMatch:{sid:1}}})
        // {  studies : [ { sid : 1, files : [ ... ] , gt : { ... } } ]  }
        List<Integer> studiesIds = selectVariantElements.getStudyIds();
        // Use elemMatch only if there is one study to return.
        if (studiesIds.size() == 1) {
            studyElemMatch = Projections.elemMatch(
                    DocumentToVariantConverter.STUDIES_FIELD,
                    eq(
                            DocumentToStudyVariantEntryConverter.STUDYID_FIELD,
                            studiesIds.get(0)
                    )
            );
        } else {
            if (fields.contains(VariantField.STUDIES_SAMPLES)) {
                List<String> formats = VariantQueryUtils.getIncludeSampleData(query);
                if (formats != null) { // If null, undefined. Return all
                    // Special conversion
                    fields.remove(VariantField.STUDIES_SAMPLES);
                    if (formats.contains(VariantQueryUtils.ALL)) {
                        projections.add(DocumentToVariantConverter.STUDIES_FIELD + '.'
                                + DocumentToStudyVariantEntryConverter.GENOTYPES_FIELD);
                        projections.add(DocumentToVariantConverter.STUDIES_FIELD + '.'
                                + DocumentToStudyVariantEntryConverter.FILES_FIELD + '.'
                                + DocumentToStudyVariantEntryConverter.SAMPLE_DATA_FIELD);
                        projections.add(DocumentToVariantConverter.STUDIES_FIELD + '.'
                                + DocumentToStudyVariantEntryConverter.FILES_FIELD + '.'
                                + DocumentToStudyVariantEntryConverter.FILEID_FIELD
                        );
                    } else {
                        for (String format : formats) {
                            if (format.equals(GT)) {
                                projections.add(DocumentToVariantConverter.STUDIES_FIELD + '.'
                                        + DocumentToStudyVariantEntryConverter.GENOTYPES_FIELD);
                            } else {
                                projections.add(DocumentToVariantConverter.STUDIES_FIELD + '.'
                                        + DocumentToStudyVariantEntryConverter.FILES_FIELD + '.'
                                        + DocumentToStudyVariantEntryConverter.SAMPLE_DATA_FIELD + '.' + format.toLowerCase());
                            }
                            projections.add(DocumentToVariantConverter.STUDIES_FIELD + '.'
                                    + DocumentToStudyVariantEntryConverter.FILES_FIELD + '.'
                                    + DocumentToStudyVariantEntryConverter.FILEID_FIELD);
                        }
                    }
                }
            }
        }

        fields = VariantField.prune(fields);

        if (!fields.isEmpty()) { //Include some
            for (VariantField s : fields) {
                List<String> keys = DocumentToVariantConverter.toShortFieldName(s);
                if (keys != null) {
                    for (String key : keys) {
                        // Put if absent. Do not overwrite $elemMatch, if any
                        projections.add(key);
                    }
                }
//                else {
//                    logger.warn("Unknown include field: {}", s);
//                }
            }
        }

        if (query.getBoolean(VARIANTS_TO_INDEX.key(), false)) {
            projections.add(INDEX_FIELD);
        }

        if (studyElemMatch != null) {
            // Avoid Path collision at studies
            projections.removeIf(key -> key.startsWith(DocumentToVariantConverter.STUDIES_FIELD));
        }
        Bson projection = Projections.include(new ArrayList<>(projections));

        if (studyElemMatch != null) {
            projection = Projections.fields(studyElemMatch, projection);
        }


        logger.debug("QueryOptions: = {}", options.toJson());
        logger.debug("Projection:   = {}", projection.toBsonDocument().toJson(JSON_WRITER_SETTINGS));
        return projection;
    }

    private void addQueryStringFilter(String key, String value, List<Bson> filters) {
        this.addQueryFilter(key, value, filters, Function.identity());
    }

    private void addQueryStringFilter(String key, List<String> value, List<Bson> filters) {
        filters.add(this.addQueryFilter(key, value));
    }

    private void addQueryIntegerFilter(String key, String value, List<Bson> filters) {
        this.addQueryFilter(key, value, filters, elem -> {
            try {
                return Integer.parseInt(elem);
            } catch (NumberFormatException e) {
                throw new VariantQueryException("Unable to parse int " + elem, e);
            }
        });
    }

    private void addQueryIntegerFilter(String key, Collection<Integer> value, List<Bson> filters) {
        filters.add(this.addQueryFilter(key, value));
    }

    private <T> void addQueryFilter(String key, String value, List<Bson> filters,
                                            Function<String, T> map) {
        VariantQueryUtils.QueryOperation intraOp = checkOperator(value);
        filters.add(addQueryFilter(key, splitValue(value, intraOp), intraOp, map));
    }

    private <T> void addQueryFilter(String key, Collection<T> value, List<Bson> filters) {
        filters.add(addQueryFilter(key, value));
    }

    private <T> Bson addQueryFilter(String key, Collection<T> value) {
        return addQueryFilter(key, value, Function.identity());
    }

    private <S, T> Bson addQueryFilter(String key, Collection<S> value, Function<S, T> map) {
        return addQueryFilter(key, value, QueryOperation.OR, map);
    }

    private <S, T> Bson addQueryFilter(String key, Collection<S> values, QueryOperation intraOp, Function<S, T> map) {
        final Bson filter;
        if (values.size() == 1) {
            S elem = values.iterator().next();
            if (elem instanceof String && isNegated((String) elem)
                    || elem instanceof NegatableValue && ((NegatableValue<?>) elem).isNegated()) {
                T mapped;
                if (elem instanceof String) {
                    mapped = map.apply((S) removeNegation((String) elem));
                } else {
                    mapped = map.apply(elem);
                }
                if (mapped instanceof Collection) {
                    filter = nin(key, mapped);
                } else {
                    filter = ne(key, mapped);
                }
            } else {
                T mapped = map.apply(elem);
                if (mapped instanceof Collection) {
                    filter = in(key, mapped);
                } else {
                    filter = eq(key, mapped);
                }
            }
        } else if (intraOp == QueryOperation.OR) {
            List<Object> list = new ArrayList<>(values.size());
            for (S elem : values) {
                if (elem instanceof String && isNegated((String) elem)) {
                    throw new VariantQueryException("Unable to use negate (!) operator in OR sequences (<it_1>(,<it_n>)*)");
                } else {
                    T mapped = map.apply(elem);
                    if (mapped instanceof Collection) {
                        list.addAll(((Collection) mapped));
                    } else {
                        list.add(mapped);
                    }
                }
            }
            if (list.size() == 1) {
                filter = eq(key, list);
            } else {
                filter = in(key, list);
            }
        } else {
            //Split in two lists: positive and negative
            List<Object> listIs = new ArrayList<>(values.size());
            List<Object> listNotIs = new ArrayList<>(values.size());

            for (S elem : values) {
                if (elem instanceof String && isNegated((String) elem)
                        || elem instanceof NegatableValue && ((NegatableValue<?>) elem).isNegated()) {
                    T mapped;
                    if (elem instanceof String) {
                        mapped = map.apply((S) removeNegation((String) elem));
                    } else {
                        mapped = map.apply(elem);
                    }
                    if (mapped instanceof Collection) {
                        listNotIs.addAll(((Collection) mapped));
                    } else {
                        listNotIs.add(mapped);
                    }
                } else {
                    T mapped = map.apply(elem);
                    if (mapped instanceof Collection) {
                        listIs.addAll(((Collection) mapped));
                    } else {
                        listIs.add(mapped);
                    }
                }
            }

            Bson isFilter = null;
            Bson notIsFilter = null;
            if (!listIs.isEmpty()) {    //Can not use method "is" because it will be overwritten with the "notEquals" or "notIn" method
                isFilter = all(key, listIs);
            }
            if (listNotIs.size() == 1) {
                notIsFilter = ne(key, listNotIs.get(0));
            } else if (listNotIs.size() > 1) {
                notIsFilter = nin(key, listNotIs);
            }

            if (isFilter != null && notIsFilter != null) {
                filter = and(isFilter, notIsFilter);
            } else if (isFilter != null) {
                filter = isFilter;
            } else if (notIsFilter != null) {
                filter = notIsFilter;
            } else {
                throw new IllegalStateException("Both filters are null");
            }
        }
        return filter;
    }

    /**
     * Accept a list of comparative filters separated with "," or ";" with the expression:
     * {OPERATION}{VALUE}, where the accepted operations are: <, <=, >, >=, =, ==, !=, ~=.
     *
     * @param key
     * @param value
     * @param filters
     * @param extendKey
     */
    private void addCompListQueryFilter(String key, String value, List<Bson> filters, boolean extendKey) {
        VariantQueryUtils.QueryOperation op = checkOperator(value);
        List<String> values = splitValue(value, op);

        List<Bson> compFilters = new ArrayList<>();

        for (String elem : values) {
            addCompQueryFilter(key, elem, compFilters, extendKey);
        }

        addAll(filters, op, compFilters);
    }

    private void addCompListQueryFilter(String key, KeyValues<String, KeyOpValue<String, String>> value, List<Bson> filters,
                                        boolean extendKey) {
        addCompListQueryFilter(key, value, filters, extendKey, null);
    }

    private void addCompListQueryFilter(String key, KeyValues<String, KeyOpValue<String, String>> value, List<Bson> filters,
                                                boolean extendKey, Values<Bson> extraFilters) {
        VariantQueryUtils.QueryOperation op = value.getOperation();

        if (extraFilters == null && value.getValues() == null) {
            return;
        }

        List<Bson> compFilters;
        if (op == QueryOperation.OR) {
            compFilters = new ArrayList<>();
        } else {
            compFilters = filters;
        }

        for (KeyOpValue<String, String> elem : value.getValues()) {
            addCompQueryFilter(key, elem, compFilters, extendKey);
        }
        /* FIXME: TASK-8038
        if (extraFilters != null) {
            if (extraFilters.getOperation() == QueryOperation.OR) {
                builder.or(extraFilters.getValues().toArray(new DBObject[0]));
            } else {
                builder.and(extraFilters.getValues().toArray(new DBObject[0]));
            }
        }
         */

        if (op == QueryOperation.OR) {
            filters.add(or(compFilters));
        }
    }

    private void addCompQueryFilter(String key, KeyOpValue<String, String> keyOpValue, List<Bson> filters, boolean extendKey) {
        if (extendKey && !keyOpValue.getKey().isEmpty()) {
            key = key + "." + keyOpValue.getKey();
        }
        addCompQueryFilter(key, keyOpValue.getValue(), filters, keyOpValue.getOp());
    }

    private void addCompQueryFilter(String key, String value, List<Bson> filters, boolean extendKey) {
        KeyOpValue<String, String> keyOpValue = parseKeyOpValue(value);
        String op = "";
        if (keyOpValue.getKey() != null) {
            if (extendKey && !keyOpValue.getKey().isEmpty()) {
                key = key + "." + keyOpValue.getKey();
            }
            value = keyOpValue.getValue();
            op = keyOpValue.getOp();
        }
        addCompQueryFilter(key, value, filters, op);
    }

    private void addCompQueryFilter(String key, String obj, List<Bson> filters, String op) {

        final Bson filter;
        switch (op) {
            case "<":
                filter = lt(key, Double.parseDouble(obj));
                break;
            case "<<":
                filter = or(
                        lt(key, Double.parseDouble(obj)),
                        exists(key, false)
                );
                break;
            case "<=":
                filter = lte(key, Double.parseDouble(obj));
                break;
            case "<<=":
                filter = or(
                        lte(key, Double.parseDouble(obj)),
                        exists(key, false)
                );
                break;
            case ">":
                filter = gt(key, Double.parseDouble(obj));
                break;
            case ">>":
                filter = or(
                        gt(key, Double.parseDouble(obj)),
                        exists(key, false)
                );
                break;
            case ">=":
                filter = gte(key, Double.parseDouble(obj));
                break;
            case ">>=":
                filter = or(
                        gte(key, Double.parseDouble(obj)),
                        exists(key, false)
                );
                break;
            case "=":
            case "==": {
                Object o;
                try {
                    o = Double.valueOf(obj);
                } catch (NumberFormatException e) {
                    o = obj;
                }
                filter = eq(key, o);
                break;
            }
            case "!=":
                Object o;
                try {
                    o = Double.valueOf(obj);
                } catch (NumberFormatException e) {
                    o = obj;
                }
                filter = ne(key, o);
                break;
            case "~=":
            case "~":
                filter = regex(key, obj);
                break;
            default:
                throw VariantQueryException.malformedParam(key, obj, "Unsupported operator " + op);
        }
        filters.add(filter);
    }

    private Bson addStringCompQueryFilter(VariantQueryParam param, String key, String value) {
        String[] split = splitOperator(value);
        String op = split[1];
        String obj = split[2];

        switch (op) {
            case "!=":
            case "!":
                return ne(key, obj);
            case "~=":
            case "~":
                return regex(key, obj);
            case "":
            case "=":
            case "==":
                return eq(key, obj);
            default:
                throw VariantQueryException.malformedParam(param, value, "Unsupported operator " + op);
        }
    }

    /**
     * Accepts a list of filters separated with "," or ";" with the expression: {SCORE}{OPERATION}{VALUE}.
     *
     * @param value        Value to parse
     * @param filters      List of filters
     * @param scoreParam Score query param
     * @param allowDescriptionFilter Use string values as filters for the score description
     */
    private void addScoreFilter(String value, List<Bson> filters, VariantQueryParam scoreParam,
                                        boolean allowDescriptionFilter) {
        addScoreFilter(value, filters, scoreParam, null, allowDescriptionFilter);
    }

    /**
     * Accepts a list of filters separated with "," or ";" with the expression: {SOURCE}{OPERATION}{VALUE}.
     *
     * @param value         Value to parse
     * @param filters       List of filters to add the score filters
     * @param scoreParam    Score VariantQueryParam
     * @param defaultSource Default source value. If null, must be present in the filter. If not, must not be present.
     * @param allowDescriptionFilter Use string values as filters for the score description
     */
    private void addScoreFilter(String value, List<Bson> filters, VariantQueryParam scoreParam, final String defaultSource,
                                        boolean allowDescriptionFilter) {
        final List<String> list;
        QueryOperation operation = checkOperator(value);
        list = splitValue(value, operation);
        List<Bson> scoreFilters = new ArrayList<>();
        for (String elem : list) {
            String[] score = VariantQueryUtils.splitOperator(elem);
            String source;
            String op;
            String scoreValue;
            // No given score
            if (StringUtils.isEmpty(score[0])) {
                if (defaultSource == null) {
                    logger.error("Bad score filter: " + elem);
                    throw VariantQueryException.malformedParam(scoreParam, value);
                }
                source = defaultSource;
                op = score[1];
                scoreValue = score[2];
            } else {
                if (defaultSource != null) {
                    logger.error("Bad score filter: " + elem);
                    throw VariantQueryException.malformedParam(scoreParam, value);
                }
                source = score[0];
                op = score[1];
                scoreValue = score[2];
            }

            String key = DocumentToVariantAnnotationConverter.SCORE_FIELD_MAP.get(source);
            if (key == null) {
                // Unknown score
                throw VariantQueryException.malformedParam(scoreParam, value);
            }

            List<Bson> scoreFilter = new ArrayList<>();
            if (NumberUtils.isParsable(scoreValue)) {
                // Query by score
                key += '.' + DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD;
                addCompQueryFilter(key, scoreValue, scoreFilter, op);
            } else if (allowDescriptionFilter) {
                // Query by description
                key += '.' + DocumentToVariantAnnotationConverter.SCORE_DESCRIPTION_FIELD;
                scoreFilter.add(addStringCompQueryFilter(scoreParam, key, scoreValue));
            } else {
                throw VariantQueryException.malformedParam(scoreParam, value);
            }
            scoreFilters.add(and(scoreFilter));
        }

        if (!scoreFilters.isEmpty()) {
            if (operation == null || operation == QueryOperation.AND) {
                filters.add(and(scoreFilters));
            } else {
                filters.add(or(scoreFilters));
            }
        }
    }

    /**
     * Accepts a list of filters separated with "," or ";" with the expression:
     * {STUDY}:{POPULATION}{OPERATION}{VALUE}.
     *
     * @param key                  PopulationFrequency schema field
     * @param alleleFrequencyField Allele frequency schema field
     * @param value                Value to parse
     * @param
     * @param queryParam           QueryParam filter
     */
    private void addFrequencyFilter(String key, String alleleFrequencyField, String value, List<Bson> filters,
                                            VariantQueryParam queryParam, boolean alternate) {
        addFrequencyFilter(key, value, filters, queryParam, alternate,
                (v, f) -> addCompQueryFilter(alleleFrequencyField, v, f, false));
    }

    /**
     * Accepts a list of filters separated with "," or ";" with the expression:
     * {STUDY}:{POPULATION}{OPERATION}{VALUE}.
     *
     * @param key       PopulationFrequency schema field
     * @param value     Value to parse
     * @param filters   List of filters
     * @param addFilter For complex filter
     */
    private void addFrequencyFilter(String key, String value, List<Bson> filters, VariantQueryParam queryParam,
                                            boolean alternate, BiConsumer<String, List<Bson>> addFilter) {
        final List<String> list;
        QueryOperation operation = checkOperator(value);
        list = splitValue(value, operation);

        List<Bson> freqFilters = new ArrayList<>();
        for (String elem : list) {
            String[] split = elem.split(IS);
            if (split.length != 2) {
                logger.error("Bad population frequency filter: " + elem);
                throw VariantQueryException.malformedParam(queryParam, value);
                //new IllegalArgumentException("Bad population frequency filter: " + elem);
            }
            String study = split[0];
            String populationFrequency = split[1];
            String[] populationFrequencySplit = splitOperator(populationFrequency);
            String population = populationFrequencySplit[0];
            String operator = populationFrequencySplit[1];
            String numValue = populationFrequencySplit[2];
            if (operator.startsWith(">>") || operator.startsWith("<<")) {
                // Remove first char
                operator = operator.substring(1);
            }

            logger.debug("populationFrequency = " + Arrays.toString(populationFrequencySplit));

            List<Bson> frequencyFilters = new ArrayList<>();
            frequencyFilters.add(eq(DocumentToVariantAnnotationConverter.POPULATION_FREQUENCY_STUDY_FIELD, study));
            frequencyFilters.add(eq(DocumentToVariantAnnotationConverter.POPULATION_FREQUENCY_POP_FIELD, population));

            Bson studyPopFilter = and(frequencyFilters);
            addFilter.accept(operator + numValue, frequencyFilters);
            Bson elemMatch = elemMatch(key, and(frequencyFilters));
            if (alternate && operator.startsWith("<") || !alternate && operator.startsWith(">")) {
                Bson orNotExistsAnyPopulation = exists(key, false);
                Bson orNotExistsPopulation = not(elemMatch(key, studyPopFilter));
                freqFilters.add(or(orNotExistsAnyPopulation, orNotExistsPopulation, elemMatch));
            } else {
                freqFilters.add(elemMatch);
            }
        }
        if (!freqFilters.isEmpty()) {
            if (operation == null || operation == QueryOperation.AND) {
                filters.addAll(freqFilters);
            } else {
                filters.add(or(freqFilters));
            }
        }
    }

    /**
     * Accept filters separated with "," or ";" with the expression:
     * [{STUDY}:]{COHORT}{OPERATION}{VALUE}.
     * Where STUDY is optional if defaultStudyMetadata is provided
     *
     * @param key                       Stats field to filter
     * @param values                    Values to parse
     * @param filters                   List of filters to add the stats filters
     * @param defaultStudyMetadata
     */
    private void addStatsFilterList(String key, String values, List<Bson> filters, StudyMetadata defaultStudyMetadata) {
        QueryOperation op = checkOperator(values);
        List<String> valuesList = splitValue(values, op);
        List<Bson> statsQueries = new LinkedList<>();
        for (String value : valuesList) {
            statsQueries.add(addStatsFilter(key, value, defaultStudyMetadata));
        }

        if (!statsQueries.isEmpty()) {
            addAll(filters, op, statsQueries);
        }
    }

    /**
     * Accepts filters with the expresion: [{STUDY}:]{COHORT}{OPERATION}{VALUE}.
     * Where STUDY is optional if defaultStudyMetadata is provided
     *
     * @param key                       Stats field to filter
     * @param filter                    Filter to parse
     * @param defaultStudyMetadata
     */
    private Bson addStatsFilter(String key, String filter, StudyMetadata defaultStudyMetadata) {
        String[] studyValue = VariantQueryUtils.splitStudyResource(filter);
        if (studyValue.length == 2 || defaultStudyMetadata != null) {
            int studyId;
            Integer cohortId;
            String operator;
            String valueStr;
            if (studyValue.length == 2) {
                String[] cohortOpValue = VariantQueryUtils.splitOperator(studyValue[1]);
                String study = studyValue[0];
                String cohort = cohortOpValue[0];
                operator = cohortOpValue[1];
                valueStr = cohortOpValue[2];

                studyId = metadataManager.getStudyId(study);
                cohortId = metadataManager.getCohortId(studyId, cohort);
            } else {
//                String study = defaultStudyMetadata.getStudyName();
                studyId = defaultStudyMetadata.getId();
                String[] cohortOpValue = VariantQueryUtils.splitOperator(filter);
                String cohort = cohortOpValue[0];
                cohortId = metadataManager.getCohortId(studyId, cohort);
                operator = cohortOpValue[1];
                valueStr = cohortOpValue[2];
            }

            List<Bson> filters = new LinkedList<>();

            filters.add(eq(DocumentToVariantStatsConverter.STUDY_ID, studyId));
            if (cohortId != null) {
                filters.add(eq(DocumentToVariantStatsConverter.COHORT_ID, cohortId));
            }
            addCompQueryFilter(key, valueStr, filters, operator);
            return elemMatch(DocumentToVariantConverter.STATS_FIELD, and(filters));
        } else {
            List<Bson> filters = new LinkedList<>();
            addCompQueryFilter(DocumentToVariantConverter.STATS_FIELD + "." + key, filter, filters, false);
            return and(filters);
        }
    }

    private List<Bson> getRegionFilter(Region region, List<Bson> andBsonList) {
        List<String> chunkIds = getChunkIds(region);
        andBsonList.add(and(
                in(DocumentToVariantConverter.AT_FIELD + '.' + DocumentToVariantConverter.CHUNK_IDS_FIELD, chunkIds),
                gte(DocumentToVariantConverter.END_FIELD, region.getStart()),
                lte(DocumentToVariantConverter.START_FIELD, region.getEnd())
        ));
        return andBsonList;
    }

    private void getRegionFilter(List<Region> regions, List<Bson> filters) {
        if (regions != null && !regions.isEmpty()) {
            for (Region region : regions) {
//                if (region.getEnd() - region.getStart() < 1000000) {
//                    List<String> chunkIds = getChunkIds(region);
//                    regionObject.put(DocumentToVariantConverter.AT_FIELD + '.' + DocumentToVariantConverter
//                            .CHUNK_IDS_FIELD,
//                            new Document("$in", chunkIds));
//                } else {
//                    regionObject.put(DocumentToVariantConverter.CHROMOSOME_FIELD, region.getChromosome());
//                }

                int end = region.getEnd();
                if (end < Integer.MAX_VALUE) { // Avoid overflow
                    end++;
                }
                filters.add(and(
                        gte("_id",
                                VariantStringIdConverter.buildId(
                                        region.getChromosome(),
                                        region.getStart())
                        ),
                        lt("_id",
                                VariantStringIdConverter.buildId(
                                        region.getChromosome(),
                                        end)
                        )
                ));
            }
        }
    }

    /* *******************
     * Auxiliary methods *
     * *******************/
    private List<String> getChunkIds(Region region) {
        List<String> chunkIds = new LinkedList<>();

        int chunkSize = (region.getEnd() - region.getStart() > VariantMongoDBAdaptor.CHUNK_SIZE_BIG)
                ? VariantMongoDBAdaptor.CHUNK_SIZE_BIG
                : VariantMongoDBAdaptor.CHUNK_SIZE_SMALL;
        int ks = chunkSize / 1000;
        int chunkStart = region.getStart() / chunkSize;
        int chunkEnd = region.getEnd() / chunkSize;

        for (int i = chunkStart; i <= chunkEnd; i++) {
            String chunkId = region.getChromosome() + "_" + i + "_" + ks + "k";
            chunkIds.add(chunkId);
        }
        return chunkIds;
    }

    protected int getChunkId(int position, int chunksize) {
        return position / chunksize;
    }

    protected int getChunkStart(int id, int chunksize) {
        return (id == 0) ? 1 : id * chunksize;
    }

    protected int getChunkEnd(int id, int chunksize) {
        return (id * chunksize) + chunksize - 1;
    }

    /**
     * Given a prefix (e.g. "BRCA1_pc_1583_"), returns all possible complete values
     * by appending each known flag storage code and "N" (null flag).
     */
    private static List<String> allCtCombinedValues(String prefix) {
        List<String> flagValues = DocumentToVariantAnnotationConverter.allFlagStorageValues();
        List<String> values = new ArrayList<>(flagValues.size());
        for (String flagCode : flagValues) {
            values.add(prefix + flagCode);
        }
        return values;
    }

}
