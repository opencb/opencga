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

package org.opencb.opencga.storage.core.variant.query;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.variant.VariantAnnotationConstants;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.INCLUDE_GENOTYPE;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.STUDY;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

/**
 * Created on 29/01/16 .
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public final class VariantQueryUtils {

    private static final Pattern OPERATION_PATTERN = Pattern.compile("^([^=<>~!]*)(<?<=?|>>?=?|!=?|!?=?~|==?)([^=<>~!]+.*)$");

    public static final String OR = ",";
    public static final char OR_CHAR = ',';
    public static final String AND = ";";
    public static final char AND_CHAR = ';';
    public static final String IS = ":";
    public static final String NOT = "!";
    public static final String STUDY_POP_FREQ_SEPARATOR = ":";
    public static final char STUDY_RESOURCE_SEPARATOR = ':';
    public static final char QUOTE_CHAR = '"';

    public static final String NONE = ParamConstants.NONE;
    public static final String ALL = ParamConstants.ALL;
    public static final String GT = "GT";

    // Some private query params
    public static final QueryParam ANNOT_EXPRESSION_GENES = QueryParam.create("annot_expression_genes", "", QueryParam.Type.TEXT_ARRAY);
    public static final QueryParam ANNOT_GO_GENES = QueryParam.create("annot_go_genes", "", QueryParam.Type.TEXT_ARRAY);
    public static final QueryParam ANNOT_GENE_REGIONS = QueryParam.create("annot_gene_regions", "", QueryParam.Type.TEXT_ARRAY);
    public static final QueryParam VARIANTS_TO_INDEX = QueryParam.create("variantsToIndex",
            "Select variants that need to be updated in the SearchEngine", QueryParam.Type.BOOLEAN);
    public static final QueryParam SAMPLE_MENDELIAN_ERROR = QueryParam.create("sampleMendelianError",
            "Get the precomputed mendelian errors for the given samples", QueryParam.Type.TEXT_ARRAY);
    public static final QueryParam SAMPLE_DE_NOVO = QueryParam.create("sampleDeNovo",
            "Get the precomputed mendelian errors non HOM_REF for the given samples", QueryParam.Type.TEXT_ARRAY);
    public static final QueryParam SAMPLE_COMPOUND_HETEROZYGOUS = QueryParam.create("sampleCompoundHeterozygous",
            "", QueryParam.Type.TEXT_ARRAY);
    public static final QueryParam NUM_SAMPLES = QueryParam.create("numSamples", "", QueryParam.Type.INTEGER);
    public static final QueryParam NUM_TOTAL_SAMPLES = QueryParam.create("numTotalSamples", "", QueryParam.Type.INTEGER);

    public static final List<QueryParam> INTERNAL_VARIANT_QUERY_PARAMS = Arrays.asList(ANNOT_EXPRESSION_GENES,
            ANNOT_GO_GENES,
            ANNOT_GENE_REGIONS,
            VARIANTS_TO_INDEX,
            SAMPLE_MENDELIAN_ERROR,
            SAMPLE_DE_NOVO,
            SAMPLE_COMPOUND_HETEROZYGOUS,
            NUM_SAMPLES,
            NUM_TOTAL_SAMPLES);

    public static final String LOF = "lof";
    public static final String LOSS_OF_FUNCTION = "loss_of_function";
    public static final String PA = "pa";
    public static final String PROTEIN_ALTERING = "protein_altering";

    public static final Set<String> LOSS_OF_FUNCTION_SET = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            VariantAnnotationConstants.FRAMESHIFT_VARIANT,
            VariantAnnotationConstants.INCOMPLETE_TERMINAL_CODON_VARIANT,
            VariantAnnotationConstants.START_LOST,
            VariantAnnotationConstants.STOP_GAINED,
            VariantAnnotationConstants.STOP_LOST,
            VariantAnnotationConstants.SPLICE_ACCEPTOR_VARIANT,
            VariantAnnotationConstants.SPLICE_DONOR_VARIANT,
            VariantAnnotationConstants.FEATURE_TRUNCATION,
            VariantAnnotationConstants.TRANSCRIPT_ABLATION
    )));

    public static final Set<String> PROTEIN_ALTERING_SET = Collections.unmodifiableSet(new HashSet<>(
            ListUtils.concat(
                    new ArrayList<>(LOSS_OF_FUNCTION_SET),
                    Arrays.asList(
                            VariantAnnotationConstants.INFRAME_DELETION,
                            VariantAnnotationConstants.INFRAME_INSERTION,
                            VariantAnnotationConstants.MISSENSE_VARIANT
//            VariantAnnotationConstants.TRANSCRIPT_AMPLIFICATION,
//            VariantAnnotationConstants.INITIATOR_CODON_VARIANT,
//            VariantAnnotationConstants.SPLICE_REGION_VARIANT,
                    ))));

    public static final Set<String> IMPORTANT_TRANSCRIPT_FLAGS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "canonical",
            "MANE Select",
            "MANE Plus Clinical",
            "CCDS",
            "basic",
            "LRG",
            "EGLH_HaemOnc",
            "TSO500"
    )));

    public static final Set<VariantQueryParam> MODIFIER_QUERY_PARAMS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            INCLUDE_STUDY,
            INCLUDE_FILE,
            INCLUDE_SAMPLE,
//            INCLUDE_COHORT,
            INCLUDE_SAMPLE_DATA,
            INCLUDE_SAMPLE_ID,
            INCLUDE_GENOTYPE,
            UNKNOWN_GENOTYPE,
            SAMPLE_METADATA,
            SAMPLE_LIMIT,
            SAMPLE_SKIP
    )));

    public static final String SKIP_MISSING_GENES = "skipMissingGenes";
    public static final String SKIP_GENE_REGIONS = "skipGeneRegions";

    public static final String OP_LE = "<=";
    public static final String OP_GE = ">=";
    public static final String OP_EQ = "=";
    public static final String OP_NEQ = "!=";
    public static final String OP_GT = ">";
    public static final String OP_LT = "<";

    public static final Comparator<Region> REGION_COMPARATOR = Comparator.comparing(Region::getChromosome)
            .thenComparing(Region::getStart)
            .thenComparing(Region::getEnd);

    private static Logger logger = LoggerFactory.getLogger(VariantQueryUtils.class);

    public enum QueryOperation {
        AND(VariantQueryUtils.AND),
        OR(VariantQueryUtils.OR);

        private final String separator;

        QueryOperation(String separator) {
            this.separator = separator;
        }

        public String separator() {
            return separator;
        }
    }

    private static final ObjectMapper QUERY_MAPPER = new ObjectMapper().addMixIn(Variant.class, VariantMixin.class);

    public enum BiotypeConsquenceTypeFlagCombination {
        NONE,
        BIOTYPE,
        BIOTYPE_CT,
        BIOTYPE_FLAG,
        BIOTYPE_CT_FLAG,
        CT,
        CT_FLAG,
        FLAG;

        public int numParams() {
            return this == NONE ? 0 : StringUtils.countMatches(name(), "_") + 1;
        }

        public boolean isConsequenceType() {
            return name().contains("CT");
        }

        public boolean isBiotype() {
            return name().contains("BIOTYPE");
        }

        public boolean isFlag() {
            return name().endsWith("FLAG");
        }

        public static BiotypeConsquenceTypeFlagCombination fromQuery(Query query) {
            return fromQuery(query, IMPORTANT_TRANSCRIPT_FLAGS);
        }

        public static BiotypeConsquenceTypeFlagCombination fromQuery(Query query, Collection<String> knownFlags) {
            // Do not change the order of the following lines, it must match the Enum values!
            String combination = isValidParam(query, ANNOT_BIOTYPE) ? "BIOTYPE_" : "";
            combination += isValidParam(query, ANNOT_CONSEQUENCE_TYPE) ? "CT_" : "";
            if (isValidParam(query, ANNOT_TRANSCRIPT_FLAG)) {
                List<String> flags = new LinkedList<>(query.getAsStringList(ANNOT_TRANSCRIPT_FLAG.key()));
                if (knownFlags == null) {
                    // Consider any flag
                    if (!flags.isEmpty()) {
                        combination += "FLAG";
                    }
                } else {
                    // Consider only those known flags
                    flags.removeAll(knownFlags);
                    // If empty, it means it only contains "basic" or "CCDS"
                    if (flags.isEmpty()) {
                        combination += "FLAG";
                    }
                }
            }
            if (combination.isEmpty()) {
                return BiotypeConsquenceTypeFlagCombination.NONE;
            } else {
                if (combination.endsWith("_")) {
                    combination = combination.substring(0, combination.length() - 1);
                }
                return valueOf(combination);
            }
        }
    }

    interface VariantMixin {
        // Serialize variants with "toString". Used to serialize queries.
        @JsonValue
        String toString();
    }

    private VariantQueryUtils() {
    }

    /**
     * Check if the object query contains the value param, is not null and, if is an string or a list, is not empty.
     * <p>
     * isValidParam(new Query(), PARAM) == false
     * isValidParam(new Query(PARAM.key(), null), PARAM) == false
     * isValidParam(new Query(PARAM.key(), ""), PARAM) == false
     * isValidParam(new Query(PARAM.key(), Collections.emptyList()), PARAM) == false
     * isValidParam(new Query(PARAM.key(), 5), PARAM) == true
     * isValidParam(new Query(PARAM.key(), "sdfas"), PARAM) == true
     *
     * @param query Query to parse
     * @param param QueryParam to check
     * @return If is valid or not
     */
    public static boolean isValidParam(Query query, QueryParam param) {
        Object value = query == null ? null : query.getOrDefault(param.key(), null);
        return (value != null)
                && !(value instanceof String && ((String) value).isEmpty()
                || value instanceof Collection && ((Collection) value).isEmpty());
    }

    public static boolean isValidParam(Query query, QueryParam param, boolean discardNegated) {
        boolean validParam = isValidParam(query, param);
        if (!discardNegated || !validParam) {
            return validParam;
        } else {
            String strValue = query.getString(param.key());
            return splitValue(strValue, checkOperator(strValue))
                    .stream()
                    .anyMatch((v) -> !isNegated(v)); // Discard negated
        }
    }

    public static Set<VariantQueryParam> validParams(Query query) {
        return validParams(query, false);
    }

    public static Set<VariantQueryParam> validParams(Query query, boolean discardModifiers) {
        Set<VariantQueryParam> params = new HashSet<>(query == null ? 0 : query.size());

        for (VariantQueryParam queryParam : VariantQueryParam.values()) {
            if (isValidParam(query, queryParam)) {
                params.add(queryParam);
            }
        }
        if (discardModifiers) {
            params.removeAll(MODIFIER_QUERY_PARAMS);
        }
        return params;
    }

    public static void validateAnnotationQuery(Query query) {
        if (query == null) {
            return;
        }
        List<VariantQueryParam> acceptedParams = Arrays.asList(ID, REGION);
        List<VariantQueryParam> ignoredParams = Arrays.asList(INCLUDE_STUDY, INCLUDE_SAMPLE, INCLUDE_FILE);
        Set<VariantQueryParam> queryParams = VariantQueryUtils.validParams(query);
        queryParams.removeAll(acceptedParams);
        queryParams.removeAll(ignoredParams);
        if (!queryParams.isEmpty()) {
//            System.out.println("query.toJson() = " + query.toJson());
            throw VariantQueryException.unsupportedVariantQueryFilters(queryParams,
                    "Accepted params when querying annotation are : " + acceptedParams.stream()
                            .map(QueryParam::key)
                            .collect(Collectors.toList()));
        }
        List<String> invalidValues = new LinkedList<>();
        for (String s : query.getAsStringList(ID.key())) {
            if (!VariantQueryUtils.isVariantId(s)) {
                invalidValues.add(s);
                break;
            }
        }
        if (!invalidValues.isEmpty()) {
            throw VariantQueryException.malformedParam(ID, invalidValues.toString(),
                    "Only variants supported: chrom:start:ref:alt");
        }
    }

    public static QueryOptions validateAnnotationQueryOptions(QueryOptions queryOptions) {
        if (queryOptions == null) {
            return new QueryOptions(QueryOptions.INCLUDE, VariantField.ANNOTATION);
        } else {
            queryOptions = new QueryOptions(queryOptions);
        }

        boolean anyPresent = transformVariantAnnotationField(QueryOptions.INCLUDE, queryOptions);
        anyPresent |= transformVariantAnnotationField(QueryOptions.EXCLUDE, queryOptions);

        if (!anyPresent) {
            queryOptions.add(QueryOptions.INCLUDE, VariantField.ANNOTATION);
        }

        return queryOptions;
    }

    private static boolean transformVariantAnnotationField(String key, QueryOptions queryOptions) {
        List<String> include = new ArrayList<>(10);
        final String annotation = VariantField.ANNOTATION.fieldName();
        for (String field : queryOptions.getAsStringList(key)) {
            String newField;
            if (field.startsWith(annotation + '.') || field.equals(annotation)) {
                newField = field;
            } else {
                newField = annotation + '.' + field;
            }

            if (VariantField.get(newField) == null) {
                throw VariantQueryException.unknownVariantAnnotationField(key, field);
            }
            include.add(newField);
        }
        if (include.isEmpty()) {
            return false;
        } else {
            queryOptions.put(key, include);
            return true;
        }
    }

    /**
     * Determines if the filter is negated.
     *
     * @param value Value to check
     * @return If the value is negated
     */
    public static boolean isNegated(String value) {
        return value.startsWith(NOT);
    }

    public static String removeNegation(String value) {
        if (isNegated(value)) {
            return value.substring(NOT.length());
        } else {
            return value;
        }
    }

    public static boolean isNoneOrAll(String value) {
        return isNone(value) || isAll(value);
    }

    public static boolean isNoneOrEmpty(List<String> value) {
        return value != null && (value.isEmpty() || value.size() == 1 && isNone(value.get(0)));
    }

    public static boolean isNone(Query q, QueryParam queryParam) {
        return isNone(q.getString(queryParam.key()));
    }

    public static boolean isNone(String value) {
        return NONE.equals(value);
    }

    public static boolean isAllOrNull(List<String> value) {
        return value == null || value.size() == 1 && isAll(value.get(0));
    }

    public static boolean isAll(Query q, QueryParam queryParam) {
        return isAll(q.getString(queryParam.key()));
    }

    public static boolean isAll(String s) {
        return ALL.equals(s);
    }

    /**
     * Determines if the given value is a known variant accession or not.
     *
     * @param value Value to check
     * @return If is a known accession
     */
    public static boolean isVariantAccession(String value) {
        return value.startsWith("rs") || value.startsWith("VAR_");
    }

    /**
     * Determines if the given value is a known clinical accession or not.
     * <p>
     * ClinVar accession starts with 'VCV', 'RCV' or 'SCV'
     * COSMIC mutationId starts with 'COSM', 'COSV'
     *
     * @param value Value to check
     * @return If is a known accession
     */
    public static boolean isClinicalAccession(String value) {
        return value.startsWith("RCV") || value.startsWith("SCV")  || value.startsWith("VCV")
                || value.startsWith("COSM") || value.startsWith("COSV");
    }

    /**
     * Determines if the given value is a known gene accession or not.
     * <p>
     * Human Phenotype Ontology (HPO) terms starts with 'HP:'
     * Online Mendelian Inheritance in Man (OMIM) terms starts with 'OMIM:'
     * Unified Medical Language System (UMLS) terms starts with 'umls:'
     *
     * @param value Value to check
     * @return If is a known accession
     */
    public static boolean isGeneAccession(String value) {
        return isHpo(value) || value.startsWith("OMIM:") || value.startsWith("umls:");
    }

    /**
     * Determines if the given value is a HPO term or not.
     * <p>
     * Human Phenotype Ontology (HPO) terms starts with 'HP:'
     *
     * @param value Value to check
     * @return If is a HPO term
     */
    public static boolean isHpo(String value) {
        return value.startsWith("HP:");
    }

    /**
     * Determines if the given value is a variant id or not.
     * <p>
     * chr:pos:ref:alt
     *
     * @param value Value to check
     * @return If is a variant id
     */
    public static boolean isVariantId(String value) {
        int count = StringUtils.countMatches(value, ':');
        return count == 3
                // It may have more colons if is a symbolic alternate like <DUP:TANDEM>, or a breakend 4:100:C:]15:300]A
                || count > 3 && StringUtils.containsAny(value, '<', '[', ']');
    }

    /**
     * Determines if the given value is a variant id or not.
     * <p>
     * chr:pos:ref:alt
     *
     * @param value Value to check
     * @return If is a variant id
     */
    public static Variant toVariant(String value) {
        Variant variant = null;
        if (isVariantId(value)) {
            try {
                variant = new Variant(value);
            } catch (IllegalArgumentException ignore) {
                variant = null;
                // TODO: Should this throw an exception?
                logger.info("Wrong variant " + value, ignore);
            }
        }
        return variant;
    }

    public static String[] splitStudyResource(String value) {
        int idx = value.lastIndexOf(STUDY_RESOURCE_SEPARATOR);
        if (idx <= 0 || idx == value.length() - 1) {
            return new String[]{value};
        } else {
            return new String[]{value.substring(0, idx), value.substring(idx + 1)};
        }
    }

    public static boolean isOutputMultiStudy(Query query, QueryOptions options, Collection<?> studies) {
        Set<VariantField> fields = VariantField.getIncludeFields(options);
        if (!fields.contains(VariantField.STUDIES)) {
            return false;
        } else if (isValidParam(query, INCLUDE_STUDY)) {
            String includeStudy = query.getString(VariantQueryParam.INCLUDE_STUDY.key());
            if (NONE.equals(includeStudy)) {
                return false;
            } else if (ALL.equals(includeStudy)) {
                return studies.size() > 1;
            } else {
                return query.getAsList(VariantQueryParam.INCLUDE_STUDY.key()).size() > 1;
            }
        } else if (isValidParam(query, STUDY)) {
            String value = query.getString(STUDY.key());
            long numStudies = splitValue(value, checkOperator(value)).stream().filter(s -> !isNegated(s)).count();
            return numStudies > 1;
        } else {
            return studies.size() > 1;
        }
    }

    public static <T> VariantQueryResult<T> addSamplesMetadataIfRequested(DataResult<T> result, Query query, QueryOptions options,
                                                                          VariantStorageMetadataManager variantStorageMetadataManager) {
        return addSamplesMetadataIfRequested(new VariantQueryResult<>(result, null), query, options, variantStorageMetadataManager);
    }

    public static <T> VariantQueryResult<T> addSamplesMetadataIfRequested(VariantQueryResult<T> result, Query query, QueryOptions options,
                                                                   VariantStorageMetadataManager variantStorageMetadataManager) {
        if (query.getBoolean(SAMPLE_METADATA.key(), false)) {
            int numTotalSamples = query.getInt(NUM_TOTAL_SAMPLES.key(), -1);
            int numSamples = query.getInt(NUM_SAMPLES.key(), -1);
            Map<String, List<String>> samplesMetadata = VariantQueryProjectionParser
                    .getIncludeSampleNames(query, options, variantStorageMetadataManager);
            if (numTotalSamples < 0 && numSamples < 0) {
                numTotalSamples = samplesMetadata.values().stream().mapToInt(List::size).sum();
                VariantQueryProjectionParser.skipAndLimitSamples(query, samplesMetadata);
                numSamples = samplesMetadata.values().stream().mapToInt(List::size).sum();
            }
            return result.setNumSamples(numSamples)
                    .setNumTotalSamples(numTotalSamples)
                    .setSamples(samplesMetadata);
        } else {
            int numTotalSamples = query.getInt(NUM_TOTAL_SAMPLES.key(), -1);
            int numSamples = query.getInt(NUM_SAMPLES.key(), -1);
            if (numTotalSamples >= 0 && numSamples >= 0) {
                return result.setNumSamples(numSamples)
                        .setNumTotalSamples(numTotalSamples);
            }
            return result;
        }
    }

    /**
     * Gets a list of elements sample data keys to return.
     *
     * @param query Variants Query
     * @return List of sample data keys to include. Null if undefined or all. Empty list if none.
     * @see VariantQueryParam#INCLUDE_SAMPLE_DATA
     * @see VariantQueryParam#INCLUDE_GENOTYPE
     */
    public static List<String> getIncludeSampleData(Query query) {
        final Set<String> keysSet;
        boolean all = false;
        boolean none = false;
        boolean gt = query.getBoolean(INCLUDE_GENOTYPE.key(), false);

        if (isValidParam(query, INCLUDE_SAMPLE_DATA)) {
            List<String> includeFormat = query.getAsStringList(INCLUDE_SAMPLE_DATA.key(), "[,:]");
            if (includeFormat.size() == 1) {
                String format = includeFormat.get(0);
                if (format.equals(NONE)) {
                    none = true;
                    keysSet = Collections.emptySet();
                } else if (format.equals(ALL)) {
                    all = true;
                    keysSet = Collections.emptySet();
                } else {
                    if (format.equals(GT)) {
                        gt = true;
                        keysSet = Collections.emptySet();
                    } else {
                        keysSet = Collections.singleton(format);
                    }
                }
            } else {
                keysSet = new LinkedHashSet<>(includeFormat);
                if (keysSet.contains(GT)) {
                    keysSet.remove(GT);
                    gt = true;
                }
            }
        } else {
            keysSet = Collections.emptySet();
        }

        if (none) {
            if (gt) {
                // None but genotype
                return Collections.singletonList(GT);
            } else {
                // Empty list as none elements
                return Collections.emptyList();
            }
        } else if (all || keysSet.isEmpty() && !gt) {
            // Null as all or undefined
            return null;
        } else {
            // Ensure GT is the first element
            ArrayList<String> keys = new ArrayList<>(keysSet.size());
            if (gt) {
                keys.add(GT);
            }
            keys.addAll(keysSet);

            return keys;
        }
    }

    /**
     * Parse FILE_DATA param.
     *
     * <code>
     * f1:QUAL>3;FILTER=LowGQ;LowDP,f2:QUAL>4;FILTER=PASS
     * ( f1:QUAL>3;FILTER=LowGQ;LowDP ) , ( f2:QUAL>4;FILTER=PASS )
     * ( f1: ( QUAL>3;FILTER=LowGQ;LowDP ) ) , ( f2: ( QUAL>4;FILTER=PASS ) )
     * ( f1: ( ( QUAL > 3 ) ; ( FILTER = LowGQ;LowDP ) ) ) , ( f2: ( ( QUAL > 4 ) ; ( FILTER = PASS ) ) )
     *
     * ParsedQuery {
     *   param: FILE_DATA
     *   operation: OR
     *   values: [
     *      KeyValues {
     *        key: "f1",
     *        operation: AND
     *        values: [
     *           KeyOpValue: {
     *              key: "QUAL"
     *              op: ">"
     *              value: "3"
     *           }
     *           KeyOpValue: {
     *              key: "FILTER"
     *              op: "="
     *              value: "LowGQ;LowDP"
     *           }
     *        ]
     *      }
     *      KeyValues {
     *        key: "f2",
     *        operation: AND
     *        values: [
     *           KeyOpValue: {
     *              key: "QUAL"
     *              op: ">"
     *              value: "4"
     *           }
     *           KeyOpValue: {
     *              key: "FILTER"
     *              op: "="
     *              value: "PASS"
     *           }
     *        ]
     *      }
     *   ]
     * }
     * </code>
     *
     * @param query Query to parse
     * @return a pair with the internal QueryOperation (AND/OR) and a map between Files and INFO filters.
     */
    public static ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> parseFileData(Query query) {
        if (!isValidParam(query, FILE_DATA)) {
            return new ParsedQuery<>(FILE_DATA, null, Collections.emptyList());
        }
        String value = query.getString(FILE_DATA.key());
        if (value.contains(IS)) {
            Values<KeyOpValue<String, String>> fileValues = parseMultiKeyValueFilter(FILE_DATA, value);

            List<KeyValues<String, KeyOpValue<String, String>>> files = new LinkedList<>();
            for (KeyOpValue<String, String> fileValue : fileValues.getValues()) {
                String file = fileValue.getKey();
                String filtersString = fileValue.getValue();

                Values<KeyOpValue<String, String>> values = parseMultiKeyValueFilterComparators(FILE_DATA, filtersString);

                files.add(new KeyValues<>(file, values.getOperation(), values.getValues()));
            }

            return new ParsedQuery<>(FILE_DATA, fileValues.getOperation(), files);
        } else {
            ParsedQuery<String> fileIds = splitValue(query, FILE);
            fileIds.getValues().removeIf(VariantQueryUtils::isNegated);

            if (fileIds.getValues().isEmpty()) {
                fileIds = splitValue(query, INCLUDE_FILE);
            }

            if (fileIds.getValues().isEmpty()) {
                throw VariantQueryException.malformedParam(FILE_DATA, value, "Missing \"" + FILE.key() + "\" param.");
            }

            Values<KeyOpValue<String, String>> values = parseMultiKeyValueFilterComparators(FILE_DATA, value);

            List<KeyValues<String, KeyOpValue<String, String>>> files = new LinkedList<>();
            for (String file : fileIds.getValues()) {
                files.add(new KeyValues<>(file, values));
            }

            return new ParsedQuery<>(FILE_DATA, fileIds.operation, files);
        }
    }

    /**
     * Parse FORMAT param.
     *
     * @param query Query to parse
     * @return a pair with the internal QueryOperation (AND/OR) and a map between Samples and FORMAT filters.
     * @deprecated use {@link #parseSampleData(Query)}
     */
    @Deprecated
    public static Pair<QueryOperation, Map<String, String>> parseSampleDataOLD(Query query) {
        ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> parsedQuery = parseSampleData(query);
        HashMap<String, String> map = new HashMap<>();
        for (KeyValues<String, KeyOpValue<String, String>> sampleFilter : parsedQuery) {
            map.put(sampleFilter.getKey(), sampleFilter.toValues().toQuery());
        }
        return Pair.of(parsedQuery.getOperation(), map);
    }

    /**
     * Parse FORMAT param.
     *
     * @param query Query to parse
     * @return a pair with the internal QueryOperation (AND/OR) and a map between Samples and FORMAT filters.
     */
    public static ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> parseSampleData(Query query) {
        ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> parsedQuery;
        if (!isValidParam(query, SAMPLE_DATA)) {
            return new ParsedQuery<>(SAMPLE_DATA, null, Collections.emptyList());
        }
        String value = query.getString(SAMPLE_DATA.key());
        if (value.contains(IS)) {
            // SampleData with sample ID
            // ( SAMPLE : ( KEY OP VALUE ) * ) *
            Values<KeyOpValue<String, String>> partialParse = parseMultiKeyValueFilter(SAMPLE_DATA, value);
            parsedQuery = new ParsedQuery<>(SAMPLE_DATA, partialParse.getOperation(), new ArrayList<>(partialParse.size()));
            for (KeyOpValue<String, String> keyOpValue : partialParse) {
                String sample = keyOpValue.getKey();
                Values<KeyOpValue<String, String>> values = parseMultiKeyValueFilterComparators(SAMPLE_DATA, keyOpValue.getValue());
                parsedQuery.getValues().add(
                        new KeyValues<>(sample, new Values<>(values.getOperation(), values.getValues()))
                );
            }
            return parsedQuery;
        } else {
            // SampleData without sample ID. Get sample ids from SAMPLE
            // ( KEY OP VALUE ) *
            QueryOperation operator = checkOperator(value);
            QueryOperation samplesOperator;

            List<String> samples;
            String sampleFilter = query.getString(SAMPLE.key());
            if (sampleFilter.contains(IS)) {
                ParsedQuery<KeyOpValue<String, List<String>>> gtQ = parseGenotypeFilter(sampleFilter);
                samples = gtQ.getValues().stream().map(KeyOpValue::getKey).collect(Collectors.toList());
                samplesOperator = gtQ.getOperation();
            } else {
                Values<String> values = splitValues(sampleFilter);
                samples = new LinkedList<>(values.getValues());
                samplesOperator = values.getOperation();
            }
            samples.removeIf(VariantQueryUtils::isNegated);

            if (samples.isEmpty()) {
                HashMap<Object, List<String>> genotypeMap = new HashMap<>();
                samplesOperator = parseGenotypeFilter(query.getString(GENOTYPE.key()), genotypeMap);
                samples = genotypeMap.keySet().stream().map(Object::toString).collect(Collectors.toList());
            }

            if (samples.isEmpty()) {
                samples = VariantQueryProjectionParser.getIncludeSamplesList(query);
                samplesOperator = QueryOperation.OR;
                if (samples == null) {
                    samples = Collections.emptyList();
                }
            }

            if (operator == null && samples.size() > 1) {
                operator = samplesOperator;
            }

            if (samples.isEmpty()) {
                throw VariantQueryException.malformedParam(SAMPLE_DATA, value,
                        "Missing \"" + SAMPLE.key() + "\" or \"" + GENOTYPE.key() + "\" param.");
            }

            parsedQuery = new ParsedQuery<>(SAMPLE_DATA, operator, new ArrayList<>(samples.size()));
            for (String sample : samples) {
                Values<KeyOpValue<String, String>> values = parseMultiKeyValueFilterComparators(SAMPLE_DATA, value);
                parsedQuery.getValues().add(
                        new KeyValues<>(sample, new Values<>(values.getOperation(), values.getValues()))
                );
            }
            return parsedQuery;
        }
    }

//    private static KeyValues<String, String> parseMultiKeyValueFilter(VariantQueryParam param, String stringValue) {
//        return parseMultiKeyValueFilter(param, stringValue, IS);
//    }

    public static Values<KeyOpValue<String, String>> parseMultiKeyValueFilter(VariantQueryParam param, String stringValue) {
        return parseMultiKeyValueFilter(param, stringValue, IS);
    }

    public static Values<KeyOpValue<String, String>> parseMultiKeyValueFilterComparators(VariantQueryParam param, String stringValue) {
        return parseMultiKeyValueFilter(param, stringValue, OP_LE, OP_GE, OP_NEQ, OP_EQ, OP_GT, OP_LT);
    }

    public static Values<KeyOpValue<String, String>> parseMultiKeyValueFilter(VariantQueryParam param, String stringValue,
                                                                               String... separators) {
        Map<String, KeyOpValue<String, String>> map = new LinkedHashMap<>();
        StringTokenizer tokenizer = new StringTokenizer(stringValue, OR + AND, true);
        String key = "";
        String op = "";
        String values = "";
        String logicOpTemp = "";
        QueryOperation logicOp = null;

        while (tokenizer.hasMoreElements()) {
            String token = tokenizer.nextToken();

            if (StringUtils.containsAny(token, separators)) {
                if (!key.isEmpty()) {
                    // Prev operator is the main operator
                    if (AND.equals(logicOpTemp)) {
                        if (logicOp == QueryOperation.OR) {
                            throw VariantQueryException.mixedAndOrOperators(param, stringValue);
                        } else {
                            logicOp = QueryOperation.AND;
                        }
                    } else if (OR.equals(logicOpTemp)) {
                        if (logicOp == QueryOperation.AND) {
                            throw VariantQueryException.mixedAndOrOperators(param, stringValue);
                        } else {
                            logicOp = QueryOperation.OR;
                        }
                    }
                    key = key.trim();
                    op = op.trim();
                    values = values.trim();
                    // Add prev key/value to map
                    QueryOperation finalOperation = logicOp;
                    map.merge(key, new KeyOpValue<>(key, op, values),
                            (v1, v2) -> v1.setValue(v1.getValue() + finalOperation.separator() + v2.getValue()));
                }
                for (String separator : separators) {
                    if (token.contains(separator)) {
                        int idx = token.lastIndexOf(separator);
                        key = token.substring(0, idx);
                        op = separator;
                        values = token.substring(idx + separator.length());
                        break;
                    }
                }
            } else if (token.equals(OR) || token.equals(AND)) {
                logicOpTemp = token;
            } else {
                if (key.isEmpty()) {
                    throw VariantQueryException.malformedParam(param, stringValue);
                }
                values += logicOpTemp + token;
            }
        }

        if (!key.isEmpty()) {
            key = key.trim();
            op = op.trim();
            values = values.trim();

            QueryOperation finalOperation = logicOp;
            map.merge(key, new KeyOpValue<>(key, op, values),
                    (v1, v2) -> v1.setValue(v1.getValue() + finalOperation.separator() + v2.getValue()));
        }
        if (map.size() == 1 || map.isEmpty()) {
            logicOp = null;
        }

        return new Values<>(logicOp, new ArrayList<>(map.values()));
    }

    /**
     * Parse the genotype filter.
     *
     * @param sampleGenotypes Genotypes filter value
     * @return QueryOperation between samples
     */
    public static ParsedQuery<KeyOpValue<String, List<String>>> parseGenotypeFilter(String sampleGenotypes) {
        Map<Object, List<String>> map = new HashMap<>();
        QueryOperation op = VariantQueryUtils.parseGenotypeFilter(sampleGenotypes, map);

        List<KeyOpValue<String, List<String>>> values = new ArrayList<>();
        for (Map.Entry<Object, List<String>> entry : map.entrySet()) {
            values.add(new KeyOpValue<>(entry.getKey().toString(), "=", entry.getValue()));
        }

        return new ParsedQuery<>(GENOTYPE, op, values);
    }

    /**
     * Parse the genotype filter.
     *
     * @param sampleGenotypes Genotypes filter value
     * @param map             Initialized map to be filled with the sample to list of genotypes
     * @return QueryOperation between samples
     */
    public static QueryOperation parseGenotypeFilter(String sampleGenotypes, Map<Object, List<String>> map) {

        Values<KeyOpValue<String, String>> values = parseMultiKeyValueFilter(GENOTYPE, sampleGenotypes);

        for (KeyOpValue<String, String> keyOpValue : values.getValues()) {
            List<String> gts = splitValue(keyOpValue.getValue(), QueryOperation.OR);
            boolean anyNegated = false;
            boolean allNegated = true;
            for (String gt : gts) {
                if (isNegated(gt)) {
                    anyNegated = true;
                } else {
                    allNegated = false;
                }
            }
            if (!allNegated && anyNegated) {
                throw VariantQueryException.malformedParam(GENOTYPE, sampleGenotypes);
            }
            map.put(keyOpValue.getKey(), gts);
        }

        return values.getOperation();
    }

    public static List<String> parseConsequenceTypes(String cts) {
        if (StringUtils.isEmpty(cts)) {
            return new ArrayList<>();
        } else {
            return parseConsequenceTypes(Arrays.asList(cts.split(",")));
        }
    }

    public static List<String> parseConsequenceTypes(List<String> cts) {
        Set<String> parsedCts = new LinkedHashSet<>(cts.size());
        for (String ct : cts) {
            if (ct.equalsIgnoreCase(LOF) || ct.equalsIgnoreCase(LOSS_OF_FUNCTION)) {
                parsedCts.addAll(VariantQueryUtils.LOSS_OF_FUNCTION_SET);
            } else if (ct.equalsIgnoreCase(PA) || ct.equalsIgnoreCase(PROTEIN_ALTERING)) {
                parsedCts.addAll(VariantQueryUtils.PROTEIN_ALTERING_SET);
            } else {
                parsedCts.add(ConsequenceTypeMappings.accessionToTerm.get(VariantQueryUtils.parseConsequenceType(ct)));
            }
        }
        return new ArrayList<>(parsedCts);
    }

    public static int parseConsequenceType(String so) {
        int soAccession;
        boolean startsWithSO = so.toUpperCase().startsWith("SO:");
        if (startsWithSO || StringUtils.isNumeric(so)) {
            try {
                if (startsWithSO) {
                    soAccession = Integer.parseInt(so.substring("SO:".length()));
                } else {
                    soAccession = Integer.parseInt(so);
                }
            } catch (NumberFormatException e) {
                throw VariantQueryException.malformedParam(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE, so,
                        "Not a valid SO number");
            }
            if (!ConsequenceTypeMappings.accessionToTerm.containsKey(soAccession)) {
                throw VariantQueryException.malformedParam(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE, so,
                        "Not a valid SO number");
            }
        } else {
            if (!ConsequenceTypeMappings.termToAccession.containsKey(so)) {
                throw VariantQueryException.malformedParam(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE, so,
                        "Not a valid Accession term");
            } else {
                soAccession = ConsequenceTypeMappings.termToAccession.get(so);
            }
        }
        return soAccession;
    }

    public static Query extractGenotypeFromFormatFilter(Query query) {
        Pair<QueryOperation, Map<String, String>> formatPair = parseSampleDataOLD(query);

        if (formatPair.getValue().values().stream().anyMatch(v -> v.contains("GT"))) {
            if (isValidParam(query, SAMPLE) || isValidParam(query, GENOTYPE)) {
                throw VariantQueryException.malformedParam(SAMPLE_DATA, query.getString(SAMPLE_DATA.key()),
                        "Can not be used along with filter \"" + GENOTYPE.key() + "\" or \"" + SAMPLE.key() + '"');
            }

            StringBuilder formatBuilder = new StringBuilder();
            StringBuilder genotypeBuilder = new StringBuilder();

            for (Map.Entry<String, String> entry : formatPair.getValue().entrySet()) {
                String sample = entry.getKey();
                String gt = "";
                String other = "";
                String op = "";
                String gtOp = "";
                if (entry.getValue().contains("GT")) {
                    StringTokenizer tokenizer = new StringTokenizer(entry.getValue(), AND + OR, true);
                    boolean gtFilter = false;
                    while (tokenizer.hasMoreTokens()) {
                        String token = tokenizer.nextToken();
                        if (token.equals(OR) || token.equals(AND)) {
                            op = token;
                        } else if (StringUtils.containsAny(token, '>', '<', '=', '~')) {
                            gtFilter = false;
                        }
                        if (token.contains("GT")) {
                            if (!op.isEmpty()) {
                                gtOp = op;
                            }
                            gtFilter = true;
                            gt += splitOperator(token)[2];
                        } else if (gtFilter) {
                            gt += token;
                        } else {
                            other += token;
                        }
                    }
                } else {
                    other = entry.getValue();
                }

                if (!other.isEmpty()) {
                    if (formatBuilder.length() > 0) {
                        formatBuilder.append(formatPair.getLeft().separator());
                    }
                    if (other.endsWith(OR) || other.endsWith(AND)) {
                        other = other.substring(0, other.length() - 1);
                    }
                    formatBuilder.append(sample).append(IS).append(other);
                }
                if (!gt.isEmpty()) {
                    if (genotypeBuilder.length() > 0) {
                        genotypeBuilder.append(formatPair.getLeft().separator());
                    }
                    if (gt.endsWith(OR) || gtOp.equals(OR)) {
                        throw VariantQueryException.malformedParam(SAMPLE_DATA, query.getString(SAMPLE_DATA.key()),
                                "Unable to add GT filter with operator OR (" + OR + ").");
                    } else if (gt.endsWith(AND)) {
                        gt = gt.substring(0, gt.length() - 1);
                    }
                    genotypeBuilder.append(sample).append(IS).append(gt);
                }
            }

            query.put(GENOTYPE.key(), genotypeBuilder.toString());
            query.put(SAMPLE_DATA.key(), formatBuilder.toString());
        }

        return query;
    }

    /**
     * Checks that the filter value list contains only one type of operations.
     *
     * @param value List of values to check
     * @return The used operator. Null if no operator is used.
     * @throws VariantQueryException if the list contains different operators.
     */
    public static QueryOperation checkOperator(String value) throws VariantQueryException {
        return checkOperator(value, null);
    }

    /**
     * Checks that the filter value list contains only one type of operations.
     *
     * @param value List of values to check
     * @param param Variant query param
     * @return The used operator. Null if no operator is used.
     * @throws VariantQueryException if the list contains different operators.
     */
    public static QueryOperation checkOperator(String value, VariantQueryParam param) throws VariantQueryException {
        boolean inQuotes = false;
        boolean containsOr = false; //value.contains(OR);
        boolean containsAnd = false; //value.contains(AND);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == QUOTE_CHAR) {
                inQuotes = !inQuotes;
            } else if (!inQuotes) {
                if (c == OR_CHAR) {
                    containsOr = true;
                    if (containsAnd) {
                        break;
                    }
                } else if (c == AND_CHAR) {
                    containsAnd = true;
                    if (containsOr) {
                        break;
                    }
                }
            }
        }
        if (containsAnd && containsOr) {
            if (param == null) {
                throw VariantQueryException.mixedAndOrOperators();
            } else {
                throw VariantQueryException.mixedAndOrOperators(param, value);
            }
        } else if (containsAnd) {   // && !containsOr  -> true
            return QueryOperation.AND;
        } else if (containsOr) {    // && !containsAnd  -> true
            return QueryOperation.OR;
        } else {    // !containsOr && !containsAnd
            return null;
        }
    }

    public static ParsedQuery<String> splitValue(Query query, QueryParam param) {
        String value = query.getString(param.key());
        QueryOperation operation = checkOperator(value);
        return new ParsedQuery<>(param, operation, splitValue(value, operation));
    }

    /**
     * Splits the string with the specified operation.
     *
     * @param value     Value to split
     * @return List of values, without the delimiter
     */
    public static Values<String> splitValues(String value) {
        QueryOperation operation = checkOperator(value);
        return new Values<>(operation, splitValue(value, operation));
    }

    /*
     * @deprecated use {@link #splitValues(String)}
     */
    @Deprecated
    public static Pair<QueryOperation, List<String>> splitValue(String value) {
        QueryOperation operation = checkOperator(value);
        return Pair.of(operation, splitValue(value, operation));
    }

    /**
     * Splits the string with the specified operation.
     *
     * @param value     Value to split
     * @param operation Operation that defines the split delimiter
     * @return List of values, without the delimiter
     */
    public static List<String> splitValue(String value, QueryOperation operation) {
        List<String> list;
        if (value == null || value.isEmpty()) {
            list = new ArrayList<>();
        } else if (operation == null) {
            list = new ArrayList<>(1);
            if (value.charAt(0) == QUOTE_CHAR && value.charAt(value.length() - 1) == QUOTE_CHAR) {
                list.add(value.substring(1, value.length() - 1));
            } else {
                list.add(value);
            }
        } else if (operation == QueryOperation.AND) {
            list = splitQuotes(value, AND_CHAR);
        } else {
            list = splitQuotes(value, OR_CHAR);
        }
        return list;
    }

    public static List<String> splitQuotes(String value, QueryOperation operation) {
        return splitQuotes(value, operation == QueryOperation.AND ? AND_CHAR : OR_CHAR);
    }

    public static List<String> splitQuotes(String value, char separator) {
        boolean inQuote = false;
        List<String> list = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == QUOTE_CHAR) {
                inQuote = !inQuote;
            } else {
                if (!inQuote && c == separator) {
                    if (sb.length() > 0) {
                        list.add(sb.toString());
                    }
                    sb.setLength(0);
                } else {
                    sb.append(c);
                }
            }
        }
        if (sb.length() > 0) {
            list.add(sb.toString());
        }
        if (inQuote) {
            throw new VariantQueryException("Malformed value. Unbalanced quotes : \"" + value + "\".");
        }
        return list;
    }

    /**
     * This method parses a typical key-op-value param such as 'sift<=0.2' in an array ["sift", "<=", "0.2"].
     * In case of not having a key, first element will be empty
     * In case of not matching with {@link #OPERATION_PATTERN}, key will be null and will use the default operator "="
     *
     * @param value The key-op-value parameter to be parsed
     * @return KeyOpValue
     */
    public static OpValue<String> parseOpValue(String value) {
        Matcher matcher = OPERATION_PATTERN.matcher(value);

        if (matcher.find()) {
            if (StringUtils.isNotEmpty(matcher.group(1).trim())) {
                throw new VariantQueryException("Malformed op-value. Unexpected key: '" + value + "'");
            }
            return new OpValue<>(matcher.group(2).trim(), matcher.group(3).trim());
        } else {
            return new OpValue<>("=", value.trim());
        }
    }

    /**
     * This method parses a typical key-op-value param such as 'sift<=0.2' in an array ["sift", "<=", "0.2"].
     * In case of not having a key, first element will be empty
     * In case of not matching with {@link #OPERATION_PATTERN}, key will be null and will use the default operator "="
     *
     * @param value The key-op-value parameter to be parsed
     * @return KeyOpValue
     */
    public static KeyOpValue<String, String> parseKeyOpValue(String value) {
        Matcher matcher = OPERATION_PATTERN.matcher(value);

        if (matcher.find()) {
            return new KeyOpValue<>(matcher.group(1).trim(), matcher.group(2).trim(), matcher.group(3).trim());
        } else {
            return new KeyOpValue<>(null, "=", value.trim());
        }
    }

    /*
     * @deprecated Use {@link #parseKeyOpValue(String)}
     */
    @Deprecated
    public static String[] splitOperator(String value) {
        Matcher matcher = OPERATION_PATTERN.matcher(value);
        String key;
        String operator;
        String filter;

        if (matcher.find()) {
            key = matcher.group(1);
            operator = matcher.group(2);
            filter = matcher.group(3);
        } else {
            return new String[]{null, "=", value};
        }

        return new String[]{key.trim(), operator.trim(), filter.trim()};
    }

    public static List<String> buildClinicalCombinations(VariantAnnotation variantAnnotation) {
        if (CollectionUtils.isEmpty(variantAnnotation.getTraitAssociation())) {
            return Collections.emptyList();
        }
        Set<String> clinicalSet = new HashSet<>();
        for (EvidenceEntry ev : variantAnnotation.getTraitAssociation()) {
            if (ev.getSource() != null && StringUtils.isNotEmpty(ev.getSource().getName())) {
                String source = ev.getSource().getName().toLowerCase();
                ClinicalSignificance clinicalSig = null;
                String status = null;

                if ("clinvar".equalsIgnoreCase(ev.getSource().getName())) {
                    if (ev.getVariantClassification() != null
                            && ev.getVariantClassification().getClinicalSignificance() != null) {
                        clinicalSig = ev.getVariantClassification().getClinicalSignificance();
                    }
                    if (ConsistencyStatus.congruent.equals(ev.getConsistencyStatus())) {
                        status = "confirmed";
                    }
                } else if ("cosmic".equalsIgnoreCase(ev.getSource().getName())) {
                    if (CollectionUtils.isNotEmpty(ev.getAdditionalProperties())) {
                        for (Property additionalProperty : ev.getAdditionalProperties()) {
                            if ("FATHMM_PREDICTION".equals(additionalProperty.getId())) {
                                if ("PATHOGENIC".equals(additionalProperty.getValue())) {
                                    clinicalSig = ClinicalSignificance.pathogenic;
                                } else {
                                    if ("NEUTRAL".equals(additionalProperty.getValue())) {
                                        clinicalSig = ClinicalSignificance.benign;
                                    }
                                }
                            }

                            if ("MUTATION_SOMATIC_STATUS".equals(additionalProperty.getId())
                                    || "mutationSomaticStatus_in_source_file".equals(additionalProperty.getName())) {
                                if ("Confirmed somatic variant".equals(additionalProperty.getValue())) {
                                    status = "confirmed";
                                }
                            }
                            // Stop the for
                            if (clinicalSig != null && StringUtils.isNotEmpty(status)) {
                                break;
                            }
                        }
                    }
                }

                // Create all possible combinations in this order from left to right: source, clinicalSig and status
                if (StringUtils.isNotEmpty(source)) {
                    // Let's add the source to filter easily by clinvar or cosmic
                    clinicalSet.add(source);

                    if (clinicalSig != null) {
                        // Add only clinicalSig, this replaces old index
                        clinicalSet.add(clinicalSig.name());
                        clinicalSet.add(source + "_" + clinicalSig);

                        // Combine the three parts
                        if (StringUtils.isNotEmpty(status)) {
                            clinicalSet.add(clinicalSig + "_" + status);
                            clinicalSet.add(source + "_" + clinicalSig + "_" + status);
                        }
                    }

                    // source with status, just in case clinicalSig does not exist
                    if (StringUtils.isNotEmpty(status)) {
                        clinicalSet.add(status);
                        clinicalSet.add(source + "_" + status);
                    }
                }
            }
        }
        return new ArrayList<>(clinicalSet);
    }

    public static <T extends Enum<T>> List<T> getAsEnumList(Query query, QueryParam queryParam, Class<T> enumClass) {
        return getAsEnumValues(query, queryParam, enumClass).getValues();
    }

    public static <T extends Enum<T>> Values<T> getAsEnumValues(Query query, QueryParam queryParam, Class<T> enumClass) {
        Values<String> values = splitValues(query.getString(queryParam.key()));
        return new Values<>(values.getOperation(), values.getValues()
                .stream()
                .map(enumName -> {
                    String simplified = StringUtils.replaceChars(enumName, "_-", "");
                    for (final T each : enumClass.getEnumConstants()) {
                        if (each.name().equalsIgnoreCase(enumName)
                                || StringUtils.replaceChars(each.name(), "_-", "").equalsIgnoreCase(simplified)) {
                            return each;
                        }
                    }
                    throw VariantQueryException.malformedParam(queryParam, enumName, "Unknown value");
                })
                .collect(Collectors.toList()));
    }

    public static void convertExpressionToGeneQuery(Query query, CellBaseUtils cellBaseUtils) {
        if (cellBaseUtils == null) {
            return;
        }
        if (isValidParam(query, VariantQueryParam.ANNOT_EXPRESSION)) {
            String value = query.getString(VariantQueryParam.ANNOT_EXPRESSION.key());
            // Check if comma separated of semi colon separated (AND or OR)
            VariantQueryUtils.QueryOperation queryOperation = checkOperator(value);
            // Split by comma or semi colon
            List<String> expressionValues = splitValue(value, queryOperation);

            if (queryOperation == VariantQueryUtils.QueryOperation.AND) {
                throw VariantQueryException.malformedParam(VariantQueryParam.ANNOT_EXPRESSION, value, "Unimplemented AND operator");
            }
//            query.remove(VariantQueryParam.ANNOT_EXPRESSION.key());
            Set<String> genesByExpression = cellBaseUtils.getGenesByExpression(expressionValues);
            if (genesByExpression.isEmpty()) {
                genesByExpression = Collections.singleton(NONE);
            }
            query.put(ANNOT_EXPRESSION_GENES.key(), genesByExpression);
        }
    }

    public static void convertGoToGeneQuery(Query query, CellBaseUtils cellBaseUtils) {
        if (cellBaseUtils == null) {
            return;
        }
        if (isValidParam(query, VariantQueryParam.ANNOT_GO)) {
            String value = query.getString(VariantQueryParam.ANNOT_GO.key());
            // Check if comma separated of semi colon separated (AND or OR)
            VariantQueryUtils.QueryOperation queryOperation = checkOperator(value);
            // Split by comma or semi colon
            List<String> goValues = splitValue(value, queryOperation);

            if (queryOperation == VariantQueryUtils.QueryOperation.AND) {
                throw VariantQueryException.malformedParam(VariantQueryParam.ANNOT_GO, value, "Unimplemented AND operator");
            }
//            query.remove(VariantQueryParam.ANNOT_GO.key());
            Set<String> genesByGo = cellBaseUtils.getGenesByGo(goValues);
            if (genesByGo.isEmpty()) {
                genesByGo = Collections.singleton(NONE);
            }
            query.put(ANNOT_GO_GENES.key(), genesByGo);
        }
    }

    public static void convertGenesToRegionsQuery(Query query, CellBaseUtils cellBaseUtils) {
        if (isValidParam(query, ANNOT_GENE_REGIONS)) {
            // GENE_REGIONS already present in query!
            return;
        }
        ParsedVariantQuery.VariantQueryXref variantQueryXref = VariantQueryParser.parseXrefs(query);
        List<String> genes = variantQueryXref.getGenes();
        if (!genes.isEmpty()) {

            List<Region> regions = cellBaseUtils.getGeneRegion(genes, query.getBoolean(SKIP_MISSING_GENES, false));

            regions = mergeRegions(regions);

            query.put(ANNOT_GENE_REGIONS.key(), regions);
        }
    }

    public static Region intersectRegions(Region regionLeft, Region regionRight) {
        if (!regionLeft.getChromosome().equals(regionRight.getChromosome())) {
            // Not even the same chromosome
            return null;
        }
        int start = Math.max(regionLeft.getStart(), regionRight.getStart());
        int end = Math.min(regionLeft.getEnd(), regionRight.getEnd());
        if (start >= end) {
            // don't overlap
            return null;
        }
        return new Region(regionLeft.getChromosome(), start, end);
    }

    public static List<Region> mergeRegions(List<Region> regions) {
        if (regions != null && regions.size() > 1) {
            regions = new ArrayList<>(regions);
            regions.sort(Comparator.comparing(Region::getChromosome).thenComparing(Region::getStart));

            Iterator<Region> iterator = regions.iterator();
            Region prevRegion = iterator.next();
            while (iterator.hasNext()) {
                Region region = iterator.next();
                if (prevRegion.overlaps(region.getChromosome(), region.getStart(), region.getEnd())) {
                    // Merge regions
                    prevRegion.setStart(Math.min(prevRegion.getStart(), region.getStart()));
                    prevRegion.setEnd(Math.max(prevRegion.getEnd(), region.getEnd()));
                    iterator.remove();
                } else {
                    prevRegion = region;
                }
            }
        }
        return regions;
    }

    public static String printQuery(Query query) {
        if (query == null) {
            return "{}";
        } else {
            try {
                return QUERY_MAPPER.writeValueAsString(query);
            } catch (JsonProcessingException e) {
                logger.debug("Error writing json variant", e);
                return query.toString();
            }
        }
    }

    public static QueryOptions addDefaultLimit(QueryOptions queryOptions, ObjectMap configuration) {
        return addDefaultLimit(QueryOptions.LIMIT, nonNull(queryOptions),
                configuration.getInt(QUERY_LIMIT_MAX.key(), QUERY_LIMIT_MAX.defaultValue()),
                configuration.getInt(QUERY_LIMIT_DEFAULT.key(), QUERY_LIMIT_DEFAULT.defaultValue()), "variants");
    }

    public static Query addDefaultSampleLimit(Query query, ObjectMap configuration) {
        return addDefaultLimit(SAMPLE_LIMIT.key(), nonNull(query),
                configuration.getInt(QUERY_SAMPLE_LIMIT_MAX.key(), QUERY_SAMPLE_LIMIT_MAX.defaultValue()),
                configuration.getInt(QUERY_SAMPLE_LIMIT_DEFAULT.key(), QUERY_SAMPLE_LIMIT_DEFAULT.defaultValue()),
                "samples");
    }

    private static <T extends ObjectMap> T addDefaultLimit(String limitKey, T objectMap, int limitMax, int limitDefault,
                                                String elementName) {
        // Add default limit
        int limit = objectMap.getInt(limitKey, -1);
        if (limit > limitMax) {
//            logger.info("Unable to return more than {} variants. Change limit from {} to {}", limitMax, limit, limitMax);
            throw VariantQueryException.maxLimitReached(elementName, limit, limitMax);
        }
        limit = (limit >= 0) ? limit : limitDefault;
        objectMap.put(limitKey,  limit);
        return objectMap;
    }


    public static QueryOptions copy(QueryOptions queryOptions) {
        return queryOptions == null ? new QueryOptions() : new QueryOptions(queryOptions);
    }

    public static QueryOptions nonNull(QueryOptions queryOptions) {
        return queryOptions == null ? new QueryOptions() : queryOptions;
    }

    public static Query copy(Query query) {
        return query == null ? new Query() : new Query(query);
    }

    public static Query nonNull(Query query) {
        return query == null ? new Query() : query;
    }

}
