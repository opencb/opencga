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

import org.apache.commons.lang3.StringUtils;
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
public class VariantQueryUtils {

    public static final Pattern OPERATION_PATTERN = Pattern.compile("^([^=<>~!]*)(<=?|>=?|!=?|!?=?~|==?)([^=<>~!]+.*)$");
    private static final Pattern GENOTYPE_FILTER_PATTERN = Pattern.compile("(?<sample>[^,;]+):(?<gts>([^:;,]+,?)+)(?<op>[;,.])");

    public static final String OR = ",";
    public static final String AND = ";";
    public static final String IS = ":";
    public static final String NOT = "!";
    public static final String STUDY_POP_FREQ_SEPARATOR = ":";

    public static final String NONE = "none";
    public static final String ALL = "all";
    public static final String GT = "GT";

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

    public VariantQueryUtils() {
    }

    /**
     * Check if the object query contains the value param, is not null and, if is an string or a list, is not empty.
     *
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
        Object value = query.getOrDefault(param.key(), null);
        return (value != null)
                && !(value instanceof String && ((String) value).isEmpty()
                || value instanceof Collection && ((Collection) value).isEmpty());
    }

    public static Set<VariantQueryParam> validParams(Query query) {
        Set<VariantQueryParam> params = new HashSet<>(query.size());

        for (VariantQueryParam queryParam : values()) {
            if (isValidParam(query, queryParam)) {
                params.add(queryParam);
            }
        }
        return params;
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
     * @return      If is a known accession
     */
    public static boolean isVariantAccession(String value) {
        return value.startsWith("rs") || value.startsWith("VAR_");
    }

    /**
     * Determines if the given value is a known clinical accession or not.
     *
     * ClinVar accession starts with 'RCV'
     * COSMIC mutationId starts with 'COSM'
     *
     * @param value Value to check
     * @return      If is a known accession
     */
    public static boolean isClinicalAccession(String value) {
        return value.startsWith("RCV") || value.startsWith("COSM");
    }

    /**
     * Determines if the given value is a known gene accession or not.
     *
     * Human Phenotype Ontology (HPO) terms starts with 'HP:'
     * Online Mendelian Inheritance in Man (OMIM) terms starts with 'OMIM:'
     *
     * @param value Value to check
     * @return      If is a known accession
     */
    public static boolean isGeneAccession(String value) {
        return value.startsWith("HP:") || value.startsWith("OMIM:");
    }

    /**
     * Determines if the given value is a variant id or not.
     *
     * chr:pos:ref:alt
     *
     * @param value Value to check
     * @return      If is a variant id
     */
    public static boolean isVariantId(String value) {
        int count = StringUtils.countMatches(value, ':');
        return count == 3;
    }

    /**
     * Determines if the given value is a variant id or not.
     *
     * chr:pos:ref:alt
     *
     * @param value Value to check
     * @return      If is a variant id
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

    public static StudyConfiguration getDefaultStudyConfiguration(Query query, QueryOptions options,
                                                                  StudyConfigurationManager studyConfigurationManager) {
        final StudyConfiguration defaultStudyConfiguration;
        if (isValidParam(query, VariantQueryParam.STUDIES)) {
            String value = query.getString(VariantQueryParam.STUDIES.key());

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

    public static List<Integer> getReturnedStudies(Query query, QueryOptions options, StudyConfigurationManager studyConfigurationManager) {
        Set<VariantField> returnedFields = VariantField.getReturnedFields(options);
        List<Integer> studyIds;
        if (!returnedFields.contains(VariantField.STUDIES)) {
            studyIds = Collections.emptyList();
        } else if (isValidParam(query, RETURNED_STUDIES)) {
            String returnedStudies = query.getString(VariantQueryParam.RETURNED_STUDIES.key());
            if (NONE.equals(returnedStudies)) {
                studyIds = Collections.emptyList();
            } else if (ALL.equals(returnedStudies)) {
                studyIds = studyConfigurationManager.getStudyIds(options);
            } else {
                studyIds = studyConfigurationManager.getStudyIds(query.getAsList(VariantQueryParam.RETURNED_STUDIES.key()), options);
            }
        } else if (isValidParam(query, STUDIES)) {
            String studies = query.getString(VariantQueryParam.STUDIES.key());
            studyIds = studyConfigurationManager.getStudyIds(splitValue(studies, checkOperator(studies)), options);
            // if empty, all the studies
            if (studyIds.isEmpty()) {
                studyIds = studyConfigurationManager.getStudyIds(options);
            }
        } else {
            studyIds = studyConfigurationManager.getStudyIds(options);
        }
        return studyIds;
    }

    /**
     * Get list of returned files.
     *
     * Use {@link VariantQueryParam#RETURNED_FILES} if defined.
     * If missing, get non negated values from {@link VariantQueryParam#FILES}
     * If missing, get files from samples at {@link VariantQueryParam#SAMPLES}
     *
     * Null for undefined returned files. If null, return ALL files.
     * Return NONE if empty list
     *
     *
     * @param query     Query with the QueryParams
     * @param options   Query options
     * @param fields    Returned fields
     * @param studyConfigurationManager    StudyConfigurationManager
     * @return          List of fileIds to return.
     */
    public static List<Integer> getReturnedFiles(Query query, QueryOptions options, Set<VariantField> fields,
                                          StudyConfigurationManager studyConfigurationManager) {
        List<Integer> returnedFiles;
        if (!fields.contains(VariantField.STUDIES_FILES)) {
            returnedFiles = Collections.emptyList();
        } else if (query.containsKey(RETURNED_FILES.key())) {
            String files = query.getString(RETURNED_FILES.key());
            if (files.equals(ALL)) {
                returnedFiles = null;
            } else if (files.equals(NONE)) {
                returnedFiles = Collections.emptyList();
            } else {
                returnedFiles = query.getAsIntegerList(RETURNED_FILES.key());
            }
        } else if (query.containsKey(FILES.key())) {
            String files = query.getString(FILES.key());
            returnedFiles = splitValue(files, checkOperator(files))
                    .stream()
                    .filter((value) -> !isNegated(value)) // Discard negated
                    .map(Integer::parseInt)
                    .collect(Collectors.toList());
            if (returnedFiles.isEmpty()) {
                returnedFiles = null;
            }
        } else {
            List<String> sampleNames = query.getAsStringList(VariantQueryParam.SAMPLES.key());
            StudyConfiguration studyConfiguration = getDefaultStudyConfiguration(query, options, studyConfigurationManager);
            Set<Integer> returnedFilesSet = new LinkedHashSet<>();
            for (String sample : sampleNames) {
                Integer sampleId = studyConfigurationManager.getSampleId(sample, studyConfiguration);
                studyConfiguration.getSamplesInFiles().forEach((fileId, samples) -> {
                    if (samples.contains(sampleId)) {
                        returnedFilesSet.add(fileId);
                    }
                });
            }
            returnedFiles = new ArrayList<>(returnedFilesSet);
            if (returnedFiles.isEmpty()) {
                returnedFiles = null;
            }
        }
        return returnedFiles;
    }

    public static boolean isReturnedSamplesDefined(Query query, Set<VariantField> returnedFields) {
        if (getReturnedSamplesList(query, returnedFields) != null) {
            return true;
        } else if (isValidParam(query, FILES)) {
            String files = query.getString(FILES.key());
            return splitValue(files, checkOperator(files))
                    .stream()
                    .anyMatch((value) -> !isNegated(value)); // Discard negated
        }
        return false;
    }

    public static Map<String, List<String>> getSamplesMetadata(Query query, StudyConfigurationManager studyConfigurationManager) {
        List<Integer> returnedStudies = getReturnedStudies(query, null, studyConfigurationManager);
        Function<Integer, StudyConfiguration> studyProvider = studyId ->
                studyConfigurationManager.getStudyConfiguration(studyId, null).first();
        return getReturnedSamples(query, null, returnedStudies, studyProvider, (sc, s) -> s, StudyConfiguration::getStudyName);
    }

    public static Map<String, List<String>> getSamplesMetadata(Query query, StudyConfiguration studyConfiguration) {
        List<Integer> returnedStudies = Collections.singletonList(studyConfiguration.getStudyId());
        Function<Integer, StudyConfiguration> studyProvider = studyId -> studyConfiguration;
        return getReturnedSamples(query, null, returnedStudies, studyProvider, (sc, s) -> s, StudyConfiguration::getStudyName);
    }

    public static Map<String, List<String>> getSamplesMetadata(Query query, QueryOptions options,
                                                        StudyConfigurationManager studyConfigurationManager) {
        if (query.getBoolean(SAMPLES_METADATA.key(), false)) {
            if (VariantField.getReturnedFields(options).contains(VariantField.STUDIES)) {
                List<Integer> returnedStudies = getReturnedStudies(query, options, studyConfigurationManager);
                Function<Integer, StudyConfiguration> studyProvider = studyId ->
                        studyConfigurationManager.getStudyConfiguration(studyId, options).first();
                return getReturnedSamples(query, options, returnedStudies, studyProvider, (sc, s) -> s, StudyConfiguration::getStudyName);
            } else {
                return Collections.emptyMap();
            }
        } else {
            return null;
        }
    }

    public static Map<Integer, List<Integer>> getReturnedSamples(Query query, QueryOptions options,
                                                          StudyConfigurationManager studyConfigurationManager) {
        List<Integer> returnedStudies = getReturnedStudies(query, options, studyConfigurationManager);
        return getReturnedSamples(query, options, returnedStudies, studyId ->
                studyConfigurationManager.getStudyConfiguration(studyId, options).first());
    }

    public static Map<Integer, List<Integer>> getReturnedSamples(Query query, QueryOptions options,
                                                                 Collection<StudyConfiguration> studies) {
        Map<Integer, StudyConfiguration> map = studies.stream()
                .collect(Collectors.toMap(StudyConfiguration::getStudyId, Function.identity()));
        return getReturnedSamples(query, options, map.keySet(), map::get);
    }

    public static Map<Integer, List<Integer>> getReturnedSamples(Query query, QueryOptions options, Collection<Integer> studyIds,
                                                                 Function<Integer, StudyConfiguration> studyProvider) {
        return getReturnedSamples(query, options, studyIds, studyProvider, (sc, s) -> sc.getSampleIds().get(s),
                StudyConfiguration::getStudyId);
    }

    private static <T> Map<T, List<T>> getReturnedSamples(
            Query query, QueryOptions options, Collection<Integer> studyIds,
            Function<Integer, StudyConfiguration> studyProvider,
            BiFunction<StudyConfiguration, String, T> getSample, Function<StudyConfiguration, T> getStudyId) {

        List<Integer> fileIds = null;
        if (isValidParam(query, FILES)) {
            String files = query.getString(FILES.key());
            fileIds = splitValue(files, checkOperator(files))
                    .stream()
                    .filter((value) -> !isNegated(value)) // Discard negated
                    .map(Integer::parseInt)
                    .collect(Collectors.toList());
        }

        List<String> returnedSamples = getReturnedSamplesList(query, options);
        LinkedHashSet<String> returnedSamplesSet = returnedSamples != null ? new LinkedHashSet<>(returnedSamples) : null;
        boolean returnAllSamples = query.getString(VariantQueryParam.RETURNED_SAMPLES.key()).equals(ALL);

        Map<T, List<T>> samples = new HashMap<>(studyIds.size());
        for (Integer studyId : studyIds) {
            StudyConfiguration sc = studyProvider.apply(studyId);
            if (sc == null) {
                continue;
            }

            List<T> sampleNames;
            if (returnedSamplesSet != null || returnAllSamples || fileIds == null) {
                LinkedHashMap<String, Integer> returnedSamplesPosition
                        = StudyConfiguration.getReturnedSamplesPosition(sc, returnedSamplesSet);
                @SuppressWarnings("unchecked")
                T[] a = (T[]) new Object[returnedSamplesPosition.size()];
                sampleNames = Arrays.asList(a);
                returnedSamplesPosition.forEach((sample, position) -> sampleNames.set(position, getSample.apply(sc, sample)));
            } else {
                Set<T> sampleSet = new LinkedHashSet<>();
                for (Integer fileId : fileIds) {
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

    public static List<String> getReturnedSamplesList(Query query, QueryOptions options) {
        return getReturnedSamplesList(query, VariantField.getReturnedFields(options));
    }

    public static List<String> getReturnedSamplesList(Query query, Set<VariantField> returnedFields) {
        List<String> samples;
        if (!returnedFields.contains(VariantField.STUDIES_SAMPLES_DATA)) {
            samples = Collections.emptyList();
        } else {
            //Remove the studyName, if any
            samples = getReturnedSamplesList(query);
        }
        return samples;
    }

    /**
     * Get list of returned samples.
     *
     * Null for undefined returned samples. If null, return ALL samples.
     * Return NONE if empty list
     *
     *
     * @param query     Query with the QueryParams
     * @return          List of samples to return.
     */
    public static List<String> getReturnedSamplesList(Query query) {
        List<String> samples;
        if (isValidParam(query, RETURNED_SAMPLES)) {
            String samplesString = query.getString(VariantQueryParam.RETURNED_SAMPLES.key());
            if (samplesString.equals(ALL)) {
                samples = null; // Undefined. All by default
            } else if (samplesString.equals(NONE)) {
                samples = Collections.emptyList();
            } else {
                samples = query.getAsStringList(VariantQueryParam.RETURNED_SAMPLES.key());
            }
        } else if (isValidParam(query, SAMPLES)) {
            samples = query.getAsStringList(VariantQueryParam.SAMPLES.key());
        } else {
            samples = null;
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
     * @see VariantQueryParam#INCLUDE_FORMAT
     * @see VariantQueryParam#INCLUDE_GENOTYPE
     *
     * @param query Variants Query
     * @return List of formats to include. Null if undefined or all. Empty list if none.
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
     * @param sampleGenotypes   Genotypes filter value
     * @param map               Initialized map to be filled with the sample to list of genotypes
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
     * @return  The used operator. Null if no operator is used.
     * @throws VariantQueryException if the list contains different operators.
     */
    public static QueryOperation checkOperator(String value) throws VariantQueryException {
        boolean containsOr = value.contains(OR);
        boolean containsAnd = value.contains(AND);
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
     * @param operation Operation that defines the split delimiter
     * @return          List of values, without the delimiter
     */
    public static List<String> splitValue(String value, QueryOperation operation) {
        List<String> list;
        if (value == null || value.isEmpty()) {
            list = Collections.emptyList();
        } else if (operation == null) {
            list = Collections.singletonList(value);
        } else if (operation == QueryOperation.AND) {
            list = Arrays.asList(value.split(QueryOperation.AND.separator()));
        } else {
            list = Arrays.asList(value.split(QueryOperation.OR.separator()));
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
            query.remove(VariantQueryParam.ANNOT_EXPRESSION.key());
            List<String> genes = new ArrayList<>(query.getAsStringList(VariantQueryParam.GENE.key()));
            Set<String> genesByExpression = cellBaseUtils.getGenesByExpression(expressionValues);
            if (genesByExpression.isEmpty()) {
                genes.add("none");
            } else {
                genes.addAll(genesByExpression);
            }
            query.put(VariantQueryParam.GENE.key(), genes);
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
            query.remove(VariantQueryParam.ANNOT_GO.key());
            List<String> genes = new ArrayList<>(query.getAsStringList(VariantQueryParam.GENE.key()));
            Set<String> genesByGo = cellBaseUtils.getGenesByGo(goValues);
            if (genesByGo.isEmpty()) {
                genes.add("none");
            } else {
                genes.addAll(genesByGo);
            }
            query.put(VariantQueryParam.GENE.key(), genes);
        }
    }

}
