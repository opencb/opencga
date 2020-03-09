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

package org.opencb.opencga.storage.core.variant.adaptors;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.cellbase.core.variant.annotation.VariantAnnotationUtils;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.*;
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
    // LOF does not include missense_variant
    public static final Set<String> LOF_SET = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            VariantAnnotationUtils.FRAMESHIFT_VARIANT,
            VariantAnnotationUtils.INFRAME_DELETION,
            VariantAnnotationUtils.INFRAME_INSERTION,
            VariantAnnotationUtils.START_LOST,
            VariantAnnotationUtils.STOP_GAINED,
            VariantAnnotationUtils.STOP_LOST,
            VariantAnnotationUtils.SPLICE_ACCEPTOR_VARIANT,
            VariantAnnotationUtils.SPLICE_DONOR_VARIANT,
            VariantAnnotationUtils.TRANSCRIPT_ABLATION,
            VariantAnnotationUtils.TRANSCRIPT_AMPLIFICATION,
            VariantAnnotationUtils.INITIATOR_CODON_VARIANT,
            VariantAnnotationUtils.SPLICE_REGION_VARIANT,
            VariantAnnotationUtils.INCOMPLETE_TERMINAL_CODON_VARIANT
    )));
    public static final Set<String> LOF_EXTENDED_SET = Collections.unmodifiableSet(new HashSet<>(
            ListUtils.concat(
                    new ArrayList<>(LOF_SET),
                    Arrays.asList(VariantAnnotationUtils.MISSENSE_VARIANT))));

    public static final Set<VariantQueryParam> MODIFIER_QUERY_PARAMS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            INCLUDE_STUDY,
            INCLUDE_FILE,
            INCLUDE_SAMPLE,
//            INCLUDE_COHORT,
            INCLUDE_FORMAT,
            INCLUDE_GENOTYPE,
            UNKNOWN_GENOTYPE,
            SAMPLE_METADATA,
            SAMPLE_LIMIT,
            SAMPLE_SKIP
    )));

    public static final String SKIP_MISSING_GENES = "skipMissingGenes";

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
            // Do not change the order of the following lines, it must match the Enum values!
            String combination = isValidParam(query, ANNOT_BIOTYPE) ? "BIOTYPE_" : "";
            combination += isValidParam(query, ANNOT_CONSEQUENCE_TYPE) ? "CT_" : "";
            if (isValidParam(query, ANNOT_TRANSCRIPT_FLAG)) {
                List<String> flags = new LinkedList<>(query.getAsStringList(ANNOT_TRANSCRIPT_FLAG.key()));
                flags.remove("basic");
                flags.remove("CCDS");
                // If empty, it means it only contains "basic" or "CCDS"
                if (flags.isEmpty()) {
                    combination += "FLAG";
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
        StringBuilder sb = new StringBuilder();
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

            sb.append(newField);
            sb.append(',');
        }
        if (sb.length() > 0) {
            queryOptions.put(key, sb.toString());
            return true;
        } else {
            return false;
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
        return value.substring(NOT.length());
    }

    public static boolean isNoneOrAll(String value) {
        return value.equals(NONE) || value.equals(ALL);
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
     * ClinVar accession starts with 'RCV' or 'SCV'
     * COSMIC mutationId starts with 'COSM'
     *
     * @param value Value to check
     * @return If is a known accession
     */
    public static boolean isClinicalAccession(String value) {
        return value.startsWith("RCV") || value.startsWith("SCV") || value.startsWith("COSM");
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
                // It may have more colons if is a symbolic alternate like <DUP:TANDEM>
                || count > 3 && StringUtils.contains(value, '<');
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
            if (value.contains(":")) {
                try {
                    variant = new Variant(value);
                } catch (IllegalArgumentException ignore) {
                    variant = null;
                    // TODO: Should this throw an exception?
                    logger.info("Wrong variant " + value, ignore);
                }
            }
        }
        return variant;
    }

    public static VariantQueryFields parseVariantQueryFields(
            Query query, QueryOptions options, VariantStorageMetadataManager metadataManager) {
        Set<VariantField> includeFields = VariantField.getIncludeFields(options);
        List<Integer> includeStudies = VariantQueryUtils.getIncludeStudies(query, options, metadataManager, includeFields);

        Map<Integer, StudyMetadata> studyMetadata = new HashMap<>();

        for (Integer studyId : includeStudies) {
            StudyMetadata sm = metadataManager.getStudyMetadata(studyId);
            if (sm == null) {
                throw VariantQueryException.studyNotFound(studyId, metadataManager.getStudyNames());
            }
            studyMetadata.put(studyId, sm);
        }

        Map<Integer, List<Integer>> sampleIds = VariantQueryUtils.getIncludeSamples(query, options, includeStudies, metadataManager);
        int numTotalSamples = sampleIds.values().stream().mapToInt(List::size).sum();
        skipAndLimitSamples(query, sampleIds);
        int numSamples = sampleIds.values().stream().mapToInt(List::size).sum();
        Map<Integer, Map<Integer, List<Integer>>> multiFileSamples = new HashMap<>();

        Map<Integer, List<Integer>> fileIds = VariantQueryUtils.getIncludeFiles(query, includeStudies, includeFields,
                metadataManager, sampleIds);

        for (Map.Entry<Integer, List<Integer>> entry : sampleIds.entrySet()) {
            Integer studyId = entry.getKey();
            List<Integer> filesInStudy = fileIds.get(studyId);
            Map<Integer, List<Integer>> multiMap = multiFileSamples.computeIfAbsent(studyId, s -> new HashMap<>());
            for (Integer sampleId : entry.getValue()) {
                Set<Integer> filesFromSample = new HashSet<>(metadataManager.getFileIdsFromSampleId(studyId, sampleId));
                multiMap.put(sampleId, new ArrayList<>(filesFromSample.size()));
                if (filesFromSample.size() > 1) {
                    if (VariantStorageEngine.LoadSplitData.MULTI.equals(metadataManager.getLoadSplitData(studyId, sampleId))) {
                        boolean hasAnyFile = false;
                        for (Integer fileFromSample : filesFromSample) {
                            if (filesInStudy.contains(fileFromSample)) {
                                hasAnyFile = true;
                                multiMap.get(sampleId).add(fileFromSample);
                            }
                        }
                        if (!hasAnyFile) {
                            for (Integer fileFromSample : filesFromSample) {
                                multiMap.get(sampleId).add(fileFromSample);
                            }
                        }
                    }
                }
            }
        }

        if (fileIds.values().stream().allMatch(List::isEmpty)) {
            includeFields.remove(VariantField.STUDIES_FILES);
            includeFields.removeAll(VariantField.STUDIES_FILES.getChildren());
        }

        if (sampleIds.values().stream().allMatch(List::isEmpty)) {
            includeFields.remove(VariantField.STUDIES_SAMPLES_DATA);
            includeFields.removeAll(VariantField.STUDIES_SAMPLES_DATA.getChildren());
        }

        Map<Integer, List<Integer>> cohortIds = new HashMap<>();
        if (includeFields.contains(VariantField.STUDIES_STATS)) {
            for (Integer studyId : includeStudies) {
                List<Integer> cohorts = new LinkedList<>();
                for (CohortMetadata cohort : metadataManager.getCalculatedCohorts(studyId)) {
                    cohorts.add(cohort.getId());
                }
//                metadataManager.cohortIterator(studyId).forEachRemaining(cohort -> {
//                    if (cohort.isReady()/* || cohort.isInvalid()*/) {
//                        cohorts.add(cohort.getId());
//                    }
//                });
                cohortIds.put(studyId, cohorts);
            }
        }

        return new VariantQueryFields(includeFields, includeStudies, studyMetadata,
                sampleIds, multiFileSamples, numTotalSamples != numSamples, numSamples, numTotalSamples, fileIds, cohortIds);
    }

    protected static <T> void skipAndLimitSamples(Query query, Map<T, List<T>> sampleIds) {
        if (isValidParam(query, VariantQueryParam.SAMPLE_SKIP)) {
            int skip = query.getInt(VariantQueryParam.SAMPLE_SKIP.key());
            if (skip > 0) {
                for (List<T> value : sampleIds.values()) {
                    if (value.size() < skip) {
                        // Skip all samples from study
                        skip -= value.size();
                        value.clear();
                    } else {
//                        value = value.subList(skip, value.size());
                        value.subList(0, skip).clear();
                        break;
                    }
                }
            }
        }
        if (isValidParam(query, VariantQueryParam.SAMPLE_LIMIT)) {
            int limit = query.getInt(VariantQueryParam.SAMPLE_LIMIT.key());
            if (limit > 0) {
//                numSamples = limit;
                for (List<T> value : sampleIds.values()) {
                    if (limit >= value.size()) {
                        // include all samples from study
                        limit -= value.size();
                    } else if (limit == 0) {
                        value.clear();
                    } else {
//                        value = value.subList(0, limit);
                        value.subList(limit, value.size()).clear();
                        limit = 0;
                    }
                }
            }
        }
    }

    public static String[] splitStudyResource(String value) {
        int idx = value.lastIndexOf(STUDY_RESOURCE_SEPARATOR);
        if (idx <= 0 || idx == value.length() - 1) {
            return new String[]{value};
        } else {
            return new String[]{value.substring(0, idx), value.substring(idx + 1)};
        }
    }

    public static StudyMetadata getDefaultStudy(Query query, QueryOptions options, VariantStorageMetadataManager metadataManager) {
        final StudyMetadata defaultStudy;
        if (isValidParam(query, STUDY)) {
            String value = query.getString(STUDY.key());

            // Check that the study exists
            VariantQueryUtils.QueryOperation studiesOperation = checkOperator(value);
            List<String> studiesNames = splitValue(value, studiesOperation);
            List<Integer> studyIds = metadataManager.getStudyIds(studiesNames); // Non negated studyIds
            if (studyIds.size() == 1) {
                defaultStudy = metadataManager.getStudyMetadata(studyIds.get(0));
            } else {
                defaultStudy = null;
            }
        } else {
            List<String> studyNames = metadataManager.getStudyNames();
            if (studyNames != null && studyNames.size() == 1) {
                defaultStudy = metadataManager.getStudyMetadata(studyNames.get(0));
            } else {
                defaultStudy = null;
            }
        }
        return defaultStudy;
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

    public static List<Integer> getIncludeStudies(Query query, QueryOptions options, VariantStorageMetadataManager metadataManager) {
        return getIncludeStudies(query, options, metadataManager, VariantField.getIncludeFields(options));
    }

    private static List<Integer> getIncludeStudies(Query query, QueryOptions options, VariantStorageMetadataManager metadataManager,
                                                   Set<VariantField> fields) {
        List<String> studiesList = getIncludeStudiesList(query, fields);

        List<Integer> studyIds;
        if (studiesList == null) {
            studyIds = metadataManager.getStudyIds();
            if (studyIds.size() > 1) {
                Map<Integer, List<Integer>> map = null;
                if (isIncludeSamplesDefined(query, fields)) {
                    map = getIncludeSamples(query, options, studyIds, metadataManager);
                } else if (isIncludeFilesDefined(query, fields)) {
                    map = getIncludeFiles(query, studyIds, fields,
                            metadataManager, null);
                }
                if (map != null) {
                    List<Integer> studyIdsFromSubFields = new ArrayList<>();
                    for (Map.Entry<Integer, List<Integer>> entry : map.entrySet()) {
                        if (!entry.getValue().isEmpty()) {
                            studyIdsFromSubFields.add(entry.getKey());
                        }
                    }
                    if (!studyIdsFromSubFields.isEmpty()) {
                        studyIds = studyIdsFromSubFields;
                    }
                }
            }
        } else {
            studyIds = metadataManager.getStudyIds(studiesList);
        }
        return studyIds;
    }

    public static List<String> getIncludeStudiesList(Query query, Set<VariantField> fields) {
        List<String> studies;
        if (!fields.contains(VariantField.STUDIES)) {
            studies = Collections.emptyList();
        } else if (isValidParam(query, INCLUDE_STUDY)) {
            String includeStudy = query.getString(VariantQueryParam.INCLUDE_STUDY.key());
            if (NONE.equals(includeStudy)) {
                studies = Collections.emptyList();
            } else if (ALL.equals(includeStudy)) {
                studies = null;
            } else {
                studies = query.getAsStringList(VariantQueryParam.INCLUDE_STUDY.key());
            }
        } else if (isValidParam(query, STUDY)) {
            String value = query.getString(STUDY.key());
            studies = new ArrayList<>(splitValue(value, checkOperator(value)));
            studies.removeIf(VariantQueryUtils::isNegated);
            // if empty, all the studies
            if (studies.isEmpty()) {
                studies = null;
            }
        } else {
            studies = null;
        }
        return studies;
    }

    public static boolean isIncludeFilesDefined(Query query, Set<VariantField> fields) {
        if (getIncludeFilesList(query, fields) != null) {
            return true;
        }
        return isValidParam(query, SAMPLE, true)
                || isValidParam(query, SAMPLE_MENDELIAN_ERROR, false)
                || isValidParam(query, SAMPLE_DE_NOVO, false)
                || isValidParam(query, INCLUDE_SAMPLE, false)
                || isValidParam(query, GENOTYPE, false);
    }

    /**
     * Get list of returned files for each study.
     * <p>
     * Use {@link VariantQueryParam#INCLUDE_FILE} if defined.
     * If missing, get non negated values from {@link VariantQueryParam#FILE}
     * If missing, get files from samples at {@link VariantQueryParam#SAMPLE}
     * <p>
     * Null for undefined returned files. If null, return ALL files.
     * Return NONE if empty list
     *
     * @param includeSamples
     * @param query                     Query with the QueryParams
     * @param studyIds                  Returned studies
     * @param fields                    Returned fields
     * @return List of fileIds to return.
     */
    private static Map<Integer, List<Integer>> getIncludeFiles(Query query, Collection<Integer> studyIds, Set<VariantField> fields,
                                                               VariantStorageMetadataManager metadataManager,
                                                               Map<Integer, List<Integer>> includeSamples) {

        List<String> includeSamplesList = includeSamples == null ? getIncludeSamplesList(query) : null;
        List<String> includeFilesList = getIncludeFilesList(query, fields);
        boolean returnAllFiles = ALL.equals(query.getString(INCLUDE_FILE.key()));

        Map<Integer, List<Integer>> files = new HashMap<>(studyIds.size());
        for (Integer studyId : studyIds) {
            StudyMetadata sm = metadataManager.getStudyMetadata(studyId);
            if (sm == null) {
                continue;
            }

            List<Integer> fileIds;
            if (includeFilesList != null) {
                fileIds = new ArrayList<>();
                for (String file : includeFilesList) {
                    Integer fileId = metadataManager.getFileId(studyId, file);
                    if (fileId != null) {
                        fileIds.add(fileId);
                    }
                }
            } else if (returnAllFiles) {
                fileIds = new ArrayList<>(metadataManager.getIndexedFiles(studyId));
            } else if (includeSamples != null) {
                List<Integer> sampleIds = includeSamples.get(studyId);
                Set<Integer> fileSet = metadataManager.getFileIdsFromSampleIds(studyId, sampleIds);
                fileIds = new ArrayList<>(fileSet);
            } else if (includeSamplesList != null && !includeSamplesList.isEmpty()) {
                List<Integer> sampleIds = new ArrayList<>();
                for (String sample : includeSamplesList) {
                    Integer sampleId = metadataManager.getSampleId(studyId, sample);
                    if (sampleId == null) {
//                        throw VariantQueryException.sampleNotFound(sample, sm.getName());
                        break;
                    }
                    sampleIds.add(sampleId);
                }
                Set<Integer> fileSet = metadataManager.getFileIdsFromSampleIds(studyId, sampleIds);
                fileIds = new ArrayList<>(fileSet);
            } else {
                // Return all files
                fileIds = new ArrayList<>(metadataManager.getIndexedFiles(studyId));
            }
            files.put(studyId, fileIds);
        }

        return files;
    }

    /**
     * Get list of returned files.
     * <p>
     * Use {@link VariantQueryParam#INCLUDE_FILE} if defined.
     * If missing, get non negated values from {@link VariantQueryParam#FILE}
     * <p>
     * Null for undefined returned files. If null, return ALL files.
     * Return NONE if empty list
     *
     * Does not validate if file names are valid at any study.
     *
     * @param query                     Query with the QueryParams
     * @param fields                    Returned fields
     * @return List of fileIds to return.
     */
    public static List<String> getIncludeFilesList(Query query, Set<VariantField> fields) {
        List<String> includeFiles;
        if (!fields.contains(VariantField.STUDIES_FILES)) {
            includeFiles = Collections.emptyList();
        } else {
            includeFiles = getIncludeFilesList(query);
        }
        return includeFiles;
    }

    public static List<String> getIncludeFilesList(Query query) {
        List<String> includeFiles = null;
        if (query.containsKey(INCLUDE_FILE.key())) {
            String files = query.getString(INCLUDE_FILE.key());
            if (files.equals(ALL)) {
                includeFiles = null;
            } else if (files.equals(NONE)) {
                includeFiles = Collections.emptyList();
            } else {
                includeFiles = query.getAsStringList(INCLUDE_FILE.key());
            }
            return includeFiles;
        }
        if (isValidParam(query, FILE)) {
            String files = query.getString(FILE.key());
            includeFiles = splitValue(files, checkOperator(files))
                    .stream()
                    .filter(value -> !isNegated(value))
                    .collect(Collectors.toList());
        }
        if (isValidParam(query, INFO)) {
            Map<String, String> infoMap = parseInfo(query).getValue();
            if (includeFiles == null) {
                includeFiles = new ArrayList<>(infoMap.size());
            }
            includeFiles.addAll(infoMap.keySet());
        }
        if (CollectionUtils.isEmpty(includeFiles)) {
            includeFiles = null;
        }
        return includeFiles;
    }

    public static boolean isIncludeSamplesDefined(Query query, Set<VariantField> fields) {
        if (getIncludeSamplesList(query, fields) != null) {
            return true;
        }
        return isValidParam(query, FILE, true) || isValidParam(query, INCLUDE_FILE, true);
    }

    public static Map<String, List<String>> getSamplesMetadata(Query query, QueryOptions options,
                                                               VariantStorageMetadataManager metadataManager) {
        if (VariantField.getIncludeFields(options).contains(VariantField.STUDIES)) {
            Map<Integer, List<Integer>> includeSamples = getIncludeSamples(query, options, metadataManager);
            Map<String, List<String>> sampleMetadata = new HashMap<>(includeSamples.size());

            for (Map.Entry<Integer, List<Integer>> entry : includeSamples.entrySet()) {
                Integer studyId = entry.getKey();
                List<Integer> sampleIds = entry.getValue();
                String studyName = metadataManager.getStudyName(studyId);
                ArrayList<String> sampleNames = new ArrayList<>(sampleIds.size());
                for (Integer sampleId : sampleIds) {
                    sampleNames.add(metadataManager.getSampleName(studyId, sampleId));
                }
                sampleMetadata.put(studyName, sampleNames);
            }

            return sampleMetadata;
        } else {
            return Collections.emptyMap();
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
            Map<String, List<String>> samplesMetadata = getSamplesMetadata(query, options, variantStorageMetadataManager);
            if (numTotalSamples < 0 && numSamples < 0) {
                numTotalSamples = samplesMetadata.values().stream().mapToInt(List::size).sum();
                skipAndLimitSamples(query, samplesMetadata);
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

    public static Map<Integer, List<Integer>> getIncludeSamples(Query query, QueryOptions options,
                                                                VariantStorageMetadataManager variantStorageMetadataManager) {
        List<Integer> includeStudies = getIncludeStudies(query, options, variantStorageMetadataManager);
        return getIncludeSamples(query, options, includeStudies, variantStorageMetadataManager);
    }

    public static Map<Integer, List<Integer>> getIncludeSamples(
            Query query, QueryOptions options, Collection<Integer> studyIds,
            VariantStorageMetadataManager metadataManager) {

        List<String> includeFilesList = getIncludeFilesList(query);
        List<String> includeSamplesList = getIncludeSamplesList(query, options);
        boolean includeAllSamples = query.getString(VariantQueryParam.INCLUDE_SAMPLE.key()).equals(ALL);
        boolean includeNoneSamples = query.getString(VariantQueryParam.INCLUDE_SAMPLE.key()).equals(NONE);
        if (!includeNoneSamples) {
            if (includeSamplesList == null && CollectionUtils.isEmpty(includeFilesList)) {
                includeAllSamples = true;
            }
        }

        Map<Integer, List<Integer>> samples = new LinkedHashMap<>(studyIds.size());
        for (Integer studyId : studyIds) {
            StudyMetadata sm = metadataManager.getStudyMetadata(studyId);
            if (sm == null) {
                continue;
            }

            List<Integer> sampleIds;
            if (includeNoneSamples) {
                sampleIds = Collections.emptyList();
            } else if (includeAllSamples) {
                sampleIds = metadataManager.getIndexedSamples(sm.getId());
            } else if (includeSamplesList == null && CollectionUtils.isNotEmpty(includeFilesList)) {
                // Include from files
                Set<Integer> sampleSet = new LinkedHashSet<>();
                for (String file : includeFilesList) {
                    Integer fileId = metadataManager.getFileId(sm.getId(), file, true);
                    if (fileId == null) {
                        continue;
                    }
                    FileMetadata fileMetadata = metadataManager.getFileMetadata(studyId, fileId);
                    if (CollectionUtils.isNotEmpty(fileMetadata.getSamples())) {
                        sampleSet.addAll(fileMetadata.getSamples());
                    }
                }
                sampleIds = new ArrayList<>(sampleSet);
            } else {
                Object includeSampleRaw = query.get(INCLUDE_SAMPLE.key());
                if (includeSampleRaw instanceof Collection
                        && !((Collection) includeSampleRaw).isEmpty()
                        && ((Collection) includeSampleRaw).iterator().next() instanceof Integer) {
                    sampleIds = new ArrayList<>((Collection<Integer>) includeSampleRaw);
                } else {
                    sampleIds = new ArrayList<>(includeSamplesList.size());
                    for (String sample : includeSamplesList) {
                        Integer sampleId = metadataManager.getSampleId(studyId, sample);
                        if (sampleId != null) {
                            sampleIds.add(sampleId);
                        }
                    }
                    /*
                    LinkedHashMap<String, Integer> includeSamplesPosition
                            = metadataManager.getSamplesPosition(sm, includeSamplesSet);

                    sampleIds = Arrays.asList(new Integer[includeSamplesPosition.size()]);
                    for (Map.Entry<String, Integer> entry : includeSamplesPosition.entrySet()) {
                        String sample = entry.getKey();
                        Integer position = entry.getValue();
                        Integer sampleId = metadataManager.getSampleId(studyId, sample);
                        sampleIds.set(position, sampleId);
                    }
                     */
                }
                sampleIds.removeIf(id -> !metadataManager.isSampleIndexed(studyId, id));
            }
            samples.put(studyId, sampleIds);
        }

        return samples;
    }

    public static List<String> getIncludeSamplesList(Query query, QueryOptions options) {
        return getIncludeSamplesList(query, VariantField.getIncludeFields(options));
    }

    public static List<String> getIncludeSamplesList(Query query, Set<VariantField> fields) {
        List<String> samples;
        if (!fields.contains(VariantField.STUDIES_SAMPLES_DATA)) {
            samples = Collections.emptyList();
        } else {
            //Remove the studyName, if any
            samples = getIncludeSamplesList(query);
        }
        return samples;
    }

    /**
     * Get list of returned samples.
     * <p>
     * Null for undefined returned samples. If null, return ALL samples.
     * Return NONE if empty list
     *
     * @param query Query with the QueryParams
     * @return List of samples to return.
     */
    private static List<String> getIncludeSamplesList(Query query) {
        List<String> samples;
        if (isValidParam(query, INCLUDE_SAMPLE)) {
            String samplesString = query.getString(VariantQueryParam.INCLUDE_SAMPLE.key());
            if (samplesString.equals(ALL)) {
                samples = null; // Undefined. All by default
            } else if (samplesString.equals(NONE)) {
                samples = Collections.emptyList();
            } else {
                samples = query.getAsStringList(VariantQueryParam.INCLUDE_SAMPLE.key());
            }
        } else {
            samples = null;
            if (isValidParam(query, SAMPLE)) {
                String value = query.getString(SAMPLE.key());
                samples = splitValue(value, checkOperator(value))
                        .stream()
                        .filter((v) -> !isNegated(v)) // Discard negated
                        .collect(Collectors.toList());
            }
            if (isValidParam(query, GENOTYPE)) {
                HashMap<Object, List<String>> map = new LinkedHashMap<>();
                parseGenotypeFilter(query.getString(GENOTYPE.key()), map);
                if (samples == null) {
                    samples = new ArrayList<>(map.size());
                }
                map.keySet().stream().map(Object::toString).forEach(samples::add);
            }
            if (isValidParam(query, FORMAT)) {
                Map<String, String> formatMap = parseFormat(query).getValue();
                if (samples == null) {
                    samples = new ArrayList<>(formatMap.size());
                }
                samples.addAll(formatMap.keySet());
            }
            if (isValidParam(query, SAMPLE_MENDELIAN_ERROR)) {
                String value = query.getString(SAMPLE_MENDELIAN_ERROR.key());
                if (samples == null) {
                    samples = new ArrayList<>();
                }
                samples.addAll(splitValue(value, checkOperator(value)));
            }
            if (isValidParam(query, SAMPLE_DE_NOVO)) {
                String value = query.getString(SAMPLE_DE_NOVO.key());
                if (samples == null) {
                    samples = new ArrayList<>();
                }
                samples.addAll(splitValue(value, checkOperator(value)));
            }
            if (CollectionUtils.isEmpty(samples)) {
                samples = null;
            }
        }
        if (samples != null) {
            samples = samples.stream()
                    .map(s -> s.contains(":") ? s.split(":")[1] : s)
                    .distinct() // Remove possible duplicates
                    .collect(Collectors.toList());
        }
        return samples;
    }

    /**
     * Gets a list of elements formats to return.
     *
     * @param query Variants Query
     * @return List of formats to include. Null if undefined or all. Empty list if none.
     * @see VariantQueryParam#INCLUDE_FORMAT
     * @see VariantQueryParam#INCLUDE_GENOTYPE
     */
    public static List<String> getIncludeFormats(Query query) {
        final Set<String> formatsSet;
        boolean all = false;
        boolean none = false;
        boolean gt = query.getBoolean(INCLUDE_GENOTYPE.key(), false);

        if (isValidParam(query, INCLUDE_FORMAT)) {
            List<String> includeFormat = query.getAsStringList(INCLUDE_FORMAT.key(), "[,:]");
            if (includeFormat.size() == 1) {
                String format = includeFormat.get(0);
                if (format.equals(NONE)) {
                    none = true;
                    formatsSet = Collections.emptySet();
                } else if (format.equals(ALL)) {
                    all = true;
                    formatsSet = Collections.emptySet();
                } else {
                    if (format.equals(GT)) {
                        gt = true;
                        formatsSet = Collections.emptySet();
                    } else {
                        formatsSet = Collections.singleton(format);
                    }
                }
            } else {
                formatsSet = new LinkedHashSet<>(includeFormat);
                if (formatsSet.contains(GT)) {
                    formatsSet.remove(GT);
                    gt = true;
                }
            }
        } else {
            formatsSet = Collections.emptySet();
        }

        if (none) {
            if (gt) {
                // None but genotype
                return Collections.singletonList(GT);
            } else {
                // Empty list as none elements
                return Collections.emptyList();
            }
        } else if (all || formatsSet.isEmpty() && !gt) {
            // Null as all or undefined
            return null;
        } else {
            // Ensure GT is the first element
            ArrayList<String> formats = new ArrayList<>(formatsSet.size());
            if (gt) {
                formats.add(GT);
            }
            formats.addAll(formatsSet);

            return formats;
        }
    }

    /**
     * Parse INFO param.
     *
     * @param query Query to parse
     * @return a pair with the internal QueryOperation (AND/OR) and a map between Files and INFO filters.
     */
    public static Pair<QueryOperation, Map<String, String>> parseInfo(Query query) {
        if (!isValidParam(query, INFO)) {
            return Pair.of(null, Collections.emptyMap());
        }
        String value = query.getString(INFO.key());
        if (value.contains(IS)) {
            return parseMultiKeyValueFilter(INFO, value);
        } else {
            List<String> files = query.getAsStringList(FILE.key());
            files.removeIf(VariantQueryUtils::isNegated);

            if (files.isEmpty()) {
                files = query.getAsStringList(INCLUDE_FILE.key());
            }

            if (files.isEmpty()) {
                throw VariantQueryException.malformedParam(INFO, value, "Missing \"" + FILE.key() + "\" param.");
            }

            QueryOperation operator = checkOperator(value);

            Map<String, String> map = new LinkedHashMap<>(files.size());
            for (String file : files) {
                map.put(file, value);
            }

            return Pair.of(operator, map);
        }
    }

    /**
     * Parse FORMAT param.
     *
     * @param query Query to parse
     * @return a pair with the internal QueryOperation (AND/OR) and a map between Samples and FORMAT filters.
     */
    public static Pair<QueryOperation, Map<String, String>> parseFormat(Query query) {
        if (!isValidParam(query, FORMAT)) {
            return Pair.of(null, Collections.emptyMap());
        }
        String value = query.getString(FORMAT.key());
        if (value.contains(IS)) {
            return parseMultiKeyValueFilter(FORMAT, value);
        } else {
            QueryOperation operator = checkOperator(value);
            QueryOperation samplesOperator;

            List<String> samples;
            String sampleFilter = query.getString(SAMPLE.key());
            Pair<QueryOperation, List<String>> pair = splitValue(sampleFilter);
            samples = new LinkedList<>(pair.getValue());
            samplesOperator = pair.getKey();
            samples.removeIf(VariantQueryUtils::isNegated);

            if (samples.isEmpty()) {
                HashMap<Object, List<String>> genotypeMap = new HashMap<>();
                samplesOperator = parseGenotypeFilter(query.getString(GENOTYPE.key()), genotypeMap);
                samples = genotypeMap.keySet().stream().map(Object::toString).collect(Collectors.toList());
            }

            if (samples.isEmpty()) {
                samples = getIncludeSamplesList(query);
                samplesOperator = QueryOperation.OR;
                if (samples == null) {
                    samples = Collections.emptyList();
                }
            }

            if (operator == null && samples.size() > 1) {
                operator = samplesOperator;
            }

            if (samples.isEmpty()) {
                throw VariantQueryException.malformedParam(FORMAT, value,
                        "Missing \"" + SAMPLE.key() + "\" or \"" + GENOTYPE.key() + "\" param.");
            }

            Map<String, String> map = new LinkedHashMap<>(samples.size());
            for (String sample : samples) {
                map.put(sample, value);
            }

            return Pair.of(operator, map);
        }
    }

    private static Pair<QueryOperation, Map<String, String>> parseMultiKeyValueFilter(VariantQueryParam param, String stringValue) {
        Map<String, String> map = new LinkedHashMap<>();

        StringTokenizer tokenizer = new StringTokenizer(stringValue, OR + AND, true);
        String key = "";
        String values = "";
        String op = "";
        QueryOperation operation = null;

        while (tokenizer.hasMoreElements()) {
            String token = tokenizer.nextToken();

            if (token.contains(IS)) {
                if (!key.isEmpty()) {
                    // Prev operator is the main operator
                    if (AND.equals(op)) {
                        if (operation == QueryOperation.OR) {
                            throw VariantQueryException.mixedAndOrOperators(param, stringValue);
                        } else {
                            operation = QueryOperation.AND;
                        }
                    } else if (OR.equals(op)) {
                        if (operation == QueryOperation.AND) {
                            throw VariantQueryException.mixedAndOrOperators(param, stringValue);
                        } else {
                            operation = QueryOperation.OR;
                        }
                    }

                    // Add prev key/value to map
                    QueryOperation finalOperation = operation;
                    map.merge(key, values, (v1, v2) -> v1 + finalOperation.separator() + v2);
                }


                int idx = token.lastIndexOf(IS);
                key = token.substring(0, idx);
                values = token.substring(idx + 1);
            } else if (token.equals(OR) || token.equals(AND)) {
                op = token;
            } else {
                if (key.isEmpty()) {
                    throw VariantQueryException.malformedParam(param, stringValue);
                }
                values += op + token;
            }
        }

        if (!key.isEmpty()) {
            QueryOperation finalOperation = operation;
            map.merge(key, values, (v1, v2) -> v1 + finalOperation.separator() + v2);
        }

        return Pair.of(operation, map);
    }

    /**
     * Parse the genotype filter.
     *
     * @param sampleGenotypes Genotypes filter value
     * @param map             Initialized map to be filled with the sample to list of genotypes
     * @return QueryOperation between samples
     */
    public static QueryOperation parseGenotypeFilter(String sampleGenotypes, Map<Object, List<String>> map) {

        Pair<QueryOperation, Map<String, String>> pair = parseMultiKeyValueFilter(GENOTYPE, sampleGenotypes);

        for (Map.Entry<String, String> entry : pair.getValue().entrySet()) {
            List<String> gts = splitValue(entry.getValue(), QueryOperation.OR);
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
            map.put(entry.getKey(), gts);
        }

        return pair.getKey();
    }

    public static List<String> parseConsequenceTypes(List<String> cts) {
        List<String> parsedCts = new ArrayList<>(cts.size());
        for (String ct : cts) {
            if (ct.equalsIgnoreCase(LOF)) {
                parsedCts.addAll(VariantQueryUtils.LOF_SET);
            } else {
                parsedCts.add(ConsequenceTypeMappings.accessionToTerm.get(VariantQueryUtils.parseConsequenceType(ct)));
            }
        }
        return parsedCts;
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
        Pair<QueryOperation, Map<String, String>> formatPair = parseFormat(query);

        if (formatPair.getValue().values().stream().anyMatch(v -> v.contains("GT"))) {
            if (isValidParam(query, SAMPLE) || isValidParam(query, GENOTYPE)) {
                throw VariantQueryException.malformedParam(FORMAT, query.getString(FORMAT.key()),
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
                        throw VariantQueryException.malformedParam(FORMAT, query.getString(FORMAT.key()), "Unable to add GT filter with "
                                + "operator OR (" + OR + ").");
                    } else if (gt.endsWith(AND)) {
                        gt = gt.substring(0, gt.length() - 1);
                    }
                    genotypeBuilder.append(sample).append(IS).append(gt);
                }
            }

            query.put(GENOTYPE.key(), genotypeBuilder.toString());
            query.put(FORMAT.key(), formatBuilder.toString());
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

    /**
     * Splits the string with the specified operation.
     *
     * @param value     Value to split
     * @return List of values, without the delimiter
     */
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
            list = Collections.emptyList();
        } else if (operation == null) {
            if (value.charAt(0) == QUOTE_CHAR && value.charAt(value.length() - 1) == QUOTE_CHAR) {
                list = Collections.singletonList(value.substring(1, value.length() - 1));
            } else {
                list = Collections.singletonList(value);
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
     * This method split a typical key-op-value param such as 'sift<=0.2' in an array ["sift", "<=", "0.2"].
     * In case of not having a key, first element will be empty
     * In case of not matching with {@link #OPERATION_PATTERN}, key will be null and will use the default operator "="
     *
     * @param value The key-op-value parameter to be split
     * @return An array with 3 positions for the key, operator and value
     */
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


    public static void convertExpressionToGeneQuery(Query query, CellBaseUtils cellBaseUtils) {
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
        VariantQueryParser.VariantQueryXref variantQueryXref = VariantQueryParser.parseXrefs(query);
        List<String> genes = variantQueryXref.getGenes();
        if (!genes.isEmpty()) {

            List<Region> regions = cellBaseUtils.getGeneRegion(genes, query.getBoolean(SKIP_MISSING_GENES, false));

            regions = mergeRegions(regions);

            query.put(ANNOT_GENE_REGIONS.key(), regions);
        }
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
        return addDefaultLimit(QueryOptions.LIMIT, queryOptions == null ? new QueryOptions() : queryOptions,
                configuration.getInt(QUERY_LIMIT_MAX.key(), QUERY_LIMIT_MAX.defaultValue()),
                configuration.getInt(QUERY_LIMIT_DEFAULT.key(), QUERY_LIMIT_DEFAULT.defaultValue()), "variants");
    }

    public static Query addDefaultSampleLimit(Query query, ObjectMap configuration) {
        return addDefaultLimit(SAMPLE_LIMIT.key(), query == null ? new Query() : query,
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

}
