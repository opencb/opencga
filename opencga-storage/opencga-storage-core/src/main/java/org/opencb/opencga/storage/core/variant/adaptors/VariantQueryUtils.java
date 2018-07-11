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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

/**
 * Created on 29/01/16 .
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public final class VariantQueryUtils {

    private static final Pattern OPERATION_PATTERN = Pattern.compile("^([^=<>~!]*)(<=?|>=?|!=?|!?=?~|==?)([^=<>~!]+.*)$");
    private static final Pattern GENOTYPE_FILTER_PATTERN = Pattern.compile("(?<sample>[^,;]+):(?<gts>([^:;,]+,?)+)(?<op>[;,.])");

    public static final String OR = ",";
    public static final char OR_CHAR = ',';
    public static final String AND = ";";
    public static final char AND_CHAR = ';';
    public static final String IS = ":";
    public static final String NOT = "!";
    public static final String STUDY_POP_FREQ_SEPARATOR = ":";
    public static final char STUDY_RESOURCE_SEPARATOR = ':';
    public static final char QUOTE_CHAR = '"';

    public static final String NONE = "none";
    public static final String ALL = "all";
    public static final String GT = "GT";

    public static final QueryParam ANNOT_EXPRESSION_GENES = QueryParam.create("annot_expression_genes", "", QueryParam.Type.TEXT_ARRAY);
    public static final QueryParam ANNOT_GO_GENES = QueryParam.create("annot_go_genes", "", QueryParam.Type.TEXT_ARRAY);

    public static final Set<VariantQueryParam> MODIFIER_QUERY_PARAMS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            INCLUDE_STUDY,
            INCLUDE_FILE,
            INCLUDE_SAMPLE,
//            INCLUDE_COHORT,
            INCLUDE_FORMAT,
            INCLUDE_GENOTYPE,
            UNKNOWN_GENOTYPE,
            SAMPLE_METADATA
    )));
    public static final boolean DEFAULT_SKIP_COUNT = true;

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
        Set<VariantQueryParam> params = new HashSet<>(query == null ? 0 : query.size());

        for (VariantQueryParam queryParam : values()) {
            if (isValidParam(query, queryParam)) {
                params.add(queryParam);
            }
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
            if (!field.startsWith(annotation + '.')) {
                newField = annotation + '.' + field;
            } else {
                newField = field;
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
     * ClinVar accession starts with 'RCV'
     * COSMIC mutationId starts with 'COSM'
     *
     * @param value Value to check
     * @return If is a known accession
     */
    public static boolean isClinicalAccession(String value) {
        return value.startsWith("RCV") || value.startsWith("COSM");
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

    public static class VariantQueryXref {
        private final List<String> genes = new LinkedList<>();
        private final List<Variant> variants = new LinkedList<>();
        private final List<String> ids = new LinkedList<>();
        private final List<String> otherXrefs = new LinkedList<>();

        /**
         * @return List of genes found at {@link VariantQueryParam#GENE} and {@link VariantQueryParam#ANNOT_XREF}
         */
        public List<String> getGenes() {
            return genes;
        }

        /**
         * @return List of variants found at {@link VariantQueryParam#ANNOT_XREF} and {@link VariantQueryParam#ID}
         */
        public List<Variant> getVariants() {
            return variants;
        }

        /**
         * @return List of ids found at {@link VariantQueryParam#ID}
         */
        public List<String> getIds() {
            return ids;
        }

        /**
         * @return List of other xrefs found at
         * {@link VariantQueryParam#ANNOT_XREF},
         * {@link VariantQueryParam#ID},
         * {@link VariantQueryParam#ANNOT_CLINVAR},
         * {@link VariantQueryParam#ANNOT_COSMIC}
         */
        public List<String> getOtherXrefs() {
            return otherXrefs;
        }
    }

    /**
     * Parses XREFS related filters, and sorts in different lists.
     *
     * - {@link VariantQueryParam#ID}
     * - {@link VariantQueryParam#GENE}
     * - {@link VariantQueryParam#ANNOT_XREF}
     * - {@link VariantQueryParam#ANNOT_CLINVAR}
     * - {@link VariantQueryParam#ANNOT_COSMIC}
     *
     * @param query Query to parse
     * @return VariantQueryXref with all VariantIds, ids, genes and xrefs
     */
    public static VariantQueryXref parseXrefs(Query query) {
        VariantQueryXref xrefs = new VariantQueryXref();
        if (query == null) {
            return xrefs;
        }
        xrefs.getGenes().addAll(query.getAsStringList(GENE.key(), OR));

        if (isValidParam(query, ID)) {
            List<String> idsList = query.getAsStringList(ID.key(), OR);

            for (String value : idsList) {
                Variant variant = toVariant(value);
                if (variant != null) {
                    xrefs.getVariants().add(variant);
                } else {
                    xrefs.getIds().add(value);
                }
            }
        }

        if (isValidParam(query, ANNOT_XREF)) {
            List<String> xrefsList = query.getAsStringList(ANNOT_XREF.key(), OR);
            for (String value : xrefsList) {
                Variant variant = toVariant(value);
                if (variant != null) {
                    xrefs.getVariants().add(variant);
                } else {
                    if (isVariantAccession(value) || isClinicalAccession(value) || isGeneAccession(value)) {
                        xrefs.getOtherXrefs().add(value);
                    } else {
                        xrefs.getGenes().add(value);
                    }
                }
            }

        }
//        xrefs.getOtherXrefs().addAll(query.getAsStringList(ANNOT_HPO.key(), OR));
        xrefs.getOtherXrefs().addAll(query.getAsStringList(ANNOT_COSMIC.key(), OR));
        xrefs.getOtherXrefs().addAll(query.getAsStringList(ANNOT_CLINVAR.key(), OR));

        return xrefs;
    }

    public static final class SelectVariantElements {
        private final Set<VariantField> fields;
        private final List<Integer> studies;
        private final Map<Integer, StudyConfiguration> studyConfigurations;
        private final Map<Integer, List<Integer>> samples;
        private final Map<Integer, List<Integer>> files;
//        private final Map<Integer, List<Integer>> cohortIds;

        private SelectVariantElements(Set<VariantField> fields, List<Integer> studies, Map<Integer, StudyConfiguration> studyConfigurations,
                                      Map<Integer, List<Integer>> samples, Map<Integer, List<Integer>> files) {
            this.fields = fields;
            this.studies = studies;
            this.studyConfigurations = studyConfigurations;
            this.samples = samples;
            this.files = files;
        }

        public Set<VariantField> getFields() {
            return fields;
        }

        public List<Integer> getStudies() {
            return studies;
        }

        public Map<Integer, StudyConfiguration> getStudyConfigurations() {
            return studyConfigurations;
        }

        public Map<Integer, List<Integer>> getSamples() {
            return samples;
        }

        public Map<Integer, List<Integer>> getFiles() {
            return files;
        }
    }

    public static SelectVariantElements parseSelectElements(
            Query query, QueryOptions options, StudyConfigurationManager studyConfigurationManager) {
        Set<VariantField> includeFields = VariantField.getIncludeFields(options);
        List<Integer> includeStudies = VariantQueryUtils.getIncludeStudies(query, options, studyConfigurationManager, includeFields);

        Map<Integer, StudyConfiguration> studyConfigurations = new HashMap<>();

        for (Integer studyId : includeStudies) {
            StudyConfiguration sc = studyConfigurationManager.getStudyConfiguration(studyId, options).first();
            if (sc == null) {
                throw VariantQueryException.studyNotFound(studyId, studyConfigurationManager.getStudyNames(options));
            }
            studyConfigurations.put(studyId, sc);
        }

        Function<Integer, StudyConfiguration> provider = studyConfigurations::get;
        Map<Integer, List<Integer>> sampleIds = VariantQueryUtils.getIncludeSamples(query, options, includeStudies, provider);
        Map<Integer, List<Integer>> fileIds = VariantQueryUtils.getIncludeFiles(query, includeStudies, includeFields, provider);


        return new SelectVariantElements(includeFields, includeStudies, studyConfigurations, sampleIds, fileIds);
    }

    public static String[] splitStudyResource(String value) {
        int idx = value.lastIndexOf(STUDY_RESOURCE_SEPARATOR);
        if (idx <= 0 || idx == value.length() - 1) {
            return new String[]{value};
        } else {
            return new String[]{value.substring(0, idx), value.substring(idx + 1)};
        }
    }

    public static StudyConfiguration getDefaultStudyConfiguration(Query query, QueryOptions options,
                                                                  StudyConfigurationManager studyConfigurationManager) {
        final StudyConfiguration defaultStudyConfiguration;
        if (isValidParam(query, VariantQueryParam.STUDY)) {
            String value = query.getString(VariantQueryParam.STUDY.key());

            // Check that the study exists
            VariantQueryUtils.QueryOperation studiesOperation = checkOperator(value);
            List<String> studiesNames = splitValue(value, studiesOperation);
            List<Integer> studyIds = studyConfigurationManager.getStudyIds(studiesNames, options); // Non negated studyIds


            if (studyIds.size() == 1) {
                defaultStudyConfiguration = studyConfigurationManager.getStudyConfiguration(studyIds.get(0), null).first();
            } else {
                defaultStudyConfiguration = null;
            }

        } else {
            List<String> studyNames = studyConfigurationManager.getStudyNames(null);
            if (studyNames != null && studyNames.size() == 1) {
                defaultStudyConfiguration = studyConfigurationManager.getStudyConfiguration(studyNames.get(0), new QueryOptions()).first();
            } else {
                defaultStudyConfiguration = null;
            }
        }
        return defaultStudyConfiguration;
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
            String value = query.getString(VariantQueryParam.STUDY.key());
            long numStudies = splitValue(value, checkOperator(value)).stream().filter(s -> !isNegated(s)).count();
            return numStudies > 1;
        } else {
            return studies.size() > 1;
        }
    }

    public static List<Integer> getIncludeStudies(Query query, QueryOptions options, StudyConfigurationManager studyConfigurationManager) {
        return getIncludeStudies(query, options, studyConfigurationManager, VariantField.getIncludeFields(options));
    }

    private static List<Integer> getIncludeStudies(Query query, QueryOptions options, StudyConfigurationManager studyConfigurationManager,
                                                   Set<VariantField> fields) {
        List<String> studiesList = getIncludeStudiesList(query, fields);

        List<Integer> studyIds;
        if (studiesList == null) {
            studyIds = studyConfigurationManager.getStudyIds(options);
        } else {
            studyIds = studyConfigurationManager.getStudyIds(studiesList, options);
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
            String value = query.getString(VariantQueryParam.STUDY.key());
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
        return isValidParam(query, SAMPLE, true) || isValidParam(query, INCLUDE_SAMPLE, false) || isValidParam(query, GENOTYPE, false);
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
     * @param query                     Query with the QueryParams
     * @param studyIds                  Returned studies
     * @param fields                    Returned fields
     * @param studyProvider             StudyConfiguration provider
     * @return List of fileIds to return.
     */
    private static Map<Integer, List<Integer>> getIncludeFiles(
            Query query, Collection<Integer> studyIds, Set<VariantField> fields, Function<Integer, StudyConfiguration> studyProvider) {

        List<String> includeSamplesList = getIncludeSamplesList(query);
        List<String> includeFilesList = getIncludeFilesList(query, fields);
        boolean returnAllFiles = ALL.equals(query.getString(INCLUDE_FILE.key()));

        Map<Integer, List<Integer>> files = new HashMap<>(studyIds.size());
        for (Integer studyId : studyIds) {
            StudyConfiguration sc = studyProvider.apply(studyId);
            if (sc == null) {
                continue;
            }

            List<Integer> fileIds;
            if (includeFilesList != null) {
                fileIds = new ArrayList<>();
                for (String file : includeFilesList) {
                    Integer fileId = StudyConfigurationManager.getFileIdFromStudy(file, sc);
                    if (fileId != null) {
                        fileIds.add(fileId);
                    }
                }
            } else if (returnAllFiles) {
                fileIds = new ArrayList<>(sc.getIndexedFiles());
            } else if (includeSamplesList != null && !includeSamplesList.isEmpty()) {
                Set<Integer> fileSet = new LinkedHashSet<>();
                for (String sample : includeSamplesList) {
                    Integer sampleId = StudyConfigurationManager.getSampleIdFromStudy(sample, sc);
                    if (sampleId != null) {
                        for (Integer indexedFile : sc.getIndexedFiles()) {
                            if (sc.getSamplesInFiles().get(indexedFile).contains(sampleId)) {
                                fileSet.add(indexedFile);
                            }
                        }
                    }
                }
                fileIds = new ArrayList<>(fileSet);
            } else {
                // Return all files
                fileIds = new ArrayList<>(sc.getIndexedFiles());
            }
            files.put(sc.getStudyId(), fileIds);
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
    private static List<String> getIncludeFilesList(Query query, Set<VariantField> fields) {
        List<String> includeFiles;
        if (!fields.contains(VariantField.STUDIES_FILES)) {
            includeFiles = Collections.emptyList();
        } else {
            includeFiles = getIncludeFilesList(query);
        }
        return includeFiles;
    }

    private static List<String> getIncludeFilesList(Query query) {
        List<String> includeFiles;
        if (query.containsKey(INCLUDE_FILE.key())) {
            String files = query.getString(INCLUDE_FILE.key());
            if (files.equals(ALL)) {
                includeFiles = null;
            } else if (files.equals(NONE)) {
                includeFiles = Collections.emptyList();
            } else {
                includeFiles = query.getAsStringList(INCLUDE_FILE.key());
            }
        } else if (query.containsKey(FILE.key())) {
            String files = query.getString(FILE.key());
            includeFiles = splitValue(files, checkOperator(files))
                    .stream()
                    .filter(value -> !isNegated(value))
                    .collect(Collectors.toList());
            if (includeFiles.isEmpty()) {
                includeFiles = null;
            }
        } else {
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

    public static Map<String, List<String>> getSamplesMetadata(Query query, StudyConfigurationManager studyConfigurationManager) {
        List<Integer> includeStudies = getIncludeStudies(query, null, studyConfigurationManager);
        Function<Integer, StudyConfiguration> studyProvider = studyId ->
                studyConfigurationManager.getStudyConfiguration(studyId, null).first();
        return getIncludeSamples(query, null, includeStudies, studyProvider, (sc, s) -> s, StudyConfiguration::getStudyName);
    }

    public static Map<String, List<String>> getSamplesMetadata(Query query, StudyConfiguration studyConfiguration) {
        List<Integer> includeStudies = Collections.singletonList(studyConfiguration.getStudyId());
        Function<Integer, StudyConfiguration> studyProvider = studyId -> studyConfiguration;
        return getIncludeSamples(query, null, includeStudies, studyProvider, (sc, s) -> s, StudyConfiguration::getStudyName);
    }

    public static Map<String, List<String>> getSamplesMetadata(Query query, QueryOptions options,
                                                               StudyConfigurationManager studyConfigurationManager) {
        if (query.getBoolean(SAMPLE_METADATA.key(), false)) {
            if (VariantField.getIncludeFields(options).contains(VariantField.STUDIES)) {
                List<Integer> includeStudies = getIncludeStudies(query, options, studyConfigurationManager);
                Function<Integer, StudyConfiguration> studyProvider = studyId ->
                        studyConfigurationManager.getStudyConfiguration(studyId, options).first();
                return getIncludeSamples(query, options, includeStudies, studyProvider, (sc, s) -> s, StudyConfiguration::getStudyName);
            } else {
                return Collections.emptyMap();
            }
        } else {
            return null;
        }
    }

    public static Map<Integer, List<Integer>> getIncludeSamples(Query query, QueryOptions options,
                                                                StudyConfigurationManager studyConfigurationManager) {
        List<Integer> includeStudies = getIncludeStudies(query, options, studyConfigurationManager);
        return getIncludeSamples(query, options, includeStudies, studyId ->
                studyConfigurationManager.getStudyConfiguration(studyId, options).first());
    }

    public static Map<Integer, List<Integer>> getIncludeSamples(Query query, QueryOptions options,
                                                                Collection<StudyConfiguration> studies) {
        Map<Integer, StudyConfiguration> map = studies.stream()
                .collect(Collectors.toMap(StudyConfiguration::getStudyId, Function.identity()));
        return getIncludeSamples(query, options, map.keySet(), map::get);
    }

    private static Map<Integer, List<Integer>> getIncludeSamples(Query query, QueryOptions options, Collection<Integer> studyIds,
                                                                 Function<Integer, StudyConfiguration> studyProvider) {
        return getIncludeSamples(query, options, studyIds, studyProvider, (sc, s) -> sc.getSampleIds().get(s),
                StudyConfiguration::getStudyId);
    }

    private static <T> Map<T, List<T>> getIncludeSamples(
            Query query, QueryOptions options, Collection<Integer> studyIds,
            Function<Integer, StudyConfiguration> studyProvider,
            BiFunction<StudyConfiguration, String, T> getSample, Function<StudyConfiguration, T> getStudyId) {

        List<String> includeFilesList = getIncludeFilesList(query);
        List<String> includeSamplesList = getIncludeSamplesList(query, options);
        LinkedHashSet<String> includeSamplesSet = includeSamplesList != null ? new LinkedHashSet<>(includeSamplesList) : null;
        boolean includeAllSamples = query.getString(VariantQueryParam.INCLUDE_SAMPLE.key()).equals(ALL);

        Map<T, List<T>> samples = new HashMap<>(studyIds.size());
        for (Integer studyId : studyIds) {
            StudyConfiguration sc = studyProvider.apply(studyId);
            if (sc == null) {
                continue;
            }

            List<T> sampleNames;
            if (includeSamplesSet != null || includeAllSamples || includeFilesList == null || includeFilesList.isEmpty()) {
                LinkedHashMap<String, Integer> includeSamplesPosition
                        = StudyConfiguration.getSamplesPosition(sc, includeSamplesSet);
                @SuppressWarnings("unchecked")
                T[] a = (T[]) new Object[includeSamplesPosition.size()];
                sampleNames = Arrays.asList(a);
                includeSamplesPosition.forEach((sample, position) -> sampleNames.set(position, getSample.apply(sc, sample)));
            } else {
                Set<T> sampleSet = new LinkedHashSet<>();
                for (String file : includeFilesList) {
                    Integer fileId = StudyConfigurationManager.getFileIdFromStudy(file, sc);
                    if (fileId == null) {
                        continue;
                    }
                    LinkedHashSet<Integer> sampleIds = sc.getSamplesInFiles().get(fileId);
                    if (sampleIds != null) {
                        for (Integer sampleId : sampleIds) {
                            sampleSet.add(getSample.apply(sc, sc.getSampleIds().inverse().get(sampleId)));
                        }
                    }
                }
                sampleNames = new ArrayList<T>(sampleSet);
            }
            samples.put(getStudyId.apply(sc), sampleNames);
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
                HashMap<Object, List<String>> map = new HashMap<>();
                parseGenotypeFilter(query.getString(GENOTYPE.key()), map);
                if (samples == null) {
                    samples = new ArrayList<>(map.size());
                }
                map.keySet().stream().map(Object::toString).forEach(samples::add);
            }
            if (samples != null && samples.isEmpty()) {
                samples = null;
            }
        }
        if (samples != null) {
            samples = samples.stream()
                    .map(s -> s.contains(":") ? s.split(":")[1] : s)
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
     * Partes the genotype filter.
     *
     * @param sampleGenotypes Genotypes filter value
     * @param map             Initialized map to be filled with the sample to list of genotypes
     * @return QueryOperation between samples
     */
    public static QueryOperation parseGenotypeFilter(String sampleGenotypes, Map<Object, List<String>> map) {
        Matcher matcher = GENOTYPE_FILTER_PATTERN.matcher(sampleGenotypes + '.');

        QueryOperation operation = null;
        while (matcher.find()) {
            String gts = matcher.group("gts");
            String sample = matcher.group("sample");
            String op = matcher.group("op");
            map.put(sample, Arrays.asList(gts.split(",")));
            if (AND.equals(op)) {
                if (operation == QueryOperation.OR) {
                    throw VariantQueryException.malformedParam(GENOTYPE, sampleGenotypes,
                            "Unable to mix AND (" + AND + ") and OR (" + OR + ") in the same query.");
                } else {
                    operation = QueryOperation.AND;
                }
            } else if (OR.equals(op)) {
                if (operation == QueryOperation.AND) {
                    throw VariantQueryException.malformedParam(GENOTYPE, sampleGenotypes,
                            "Unable to mix AND (" + AND + ") and OR (" + OR + ") in the same query.");
                } else {
                    operation = QueryOperation.OR;
                }
            }
        }

        return operation;
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

    /**
     * Checks that the filter value list contains only one type of operations.
     *
     * @param value List of values to check
     * @return The used operator. Null if no operator is used.
     * @throws VariantQueryException if the list contains different operators.
     */
    public static QueryOperation checkOperator(String value) throws VariantQueryException {
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
            throw new VariantQueryException("Can't merge in the same query filter, AND and OR operators");
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

}
