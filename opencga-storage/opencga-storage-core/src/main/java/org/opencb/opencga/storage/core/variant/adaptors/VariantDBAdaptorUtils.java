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

package org.opencb.opencga.storage.core.variant.adaptors;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.cellbase.core.api.GeneDBAdaptor;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created on 29/01/16 .
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantDBAdaptorUtils {

    public static final Pattern OPERATION_PATTERN = Pattern.compile("^([^=<>~!]*)(<=?|>=?|!=?|!?=?~|==?)([^=<>~!]+.*)$");

    public static final String OR = ",";
    public static final String AND = ";";
    public static final String IS = ":";
    public static final String STUDY_POP_FREQ_SEPARATOR = ":";
//    public static final Map<String, String> PROJECT_FIELD_ALIAS;

    private static final int GENE_EXTRA_REGION = 5000;
    private static Logger logger = LoggerFactory.getLogger(VariantDBAdaptorUtils.class);

    private VariantDBAdaptor adaptor;

    public enum QueryOperation {
        AND(VariantDBAdaptorUtils.AND),
        OR(VariantDBAdaptorUtils.OR);

        private final String separator;

        QueryOperation(String separator) {
            this.separator = separator;
        }

        public String separator() {
            return separator;
        }
    }

    public VariantDBAdaptorUtils(VariantDBAdaptor variantDBAdaptor) {
        adaptor = variantDBAdaptor;
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
    public static boolean isValidParam(Query query, VariantDBAdaptor.VariantQueryParams param) {
        Object value = query.getOrDefault(param.key(), null);
        return (value != null)
                && !(value instanceof String && ((String) value).isEmpty()
                || value instanceof Collection && ((Collection) value).isEmpty());
    }

    /**
     * Determines if the filter is negated.
     *
     * @param value Value to check
     * @return If the value is negated
     */
    public static boolean isNegated(String value) {
        return value.startsWith("!");
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
     * ClinVar accession starts with RCV
     * COSMIC mutationId starts with RCV
     *
     * @param value Value to check
     * @return      If is a known accession
     */
    public static boolean isClinicalAccession(String value) {
        return value.startsWith("RCV") || value.startsWith("COSM");
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

    public StudyConfigurationManager getStudyConfigurationManager() {
        return adaptor.getStudyConfigurationManager();
    }

    public List<Integer> getStudyIds(QueryOptions options) {
        return getStudyConfigurationManager().getStudyIds(options);
    }

    /**
     * Get studyIds from a list of studies.
     * Replaces studyNames for studyIds.
     * Excludes those studies that starts with '!'
     *
     * @param studiesNames  List of study names or study ids
     * @param options       Options
     * @return              List of study Ids
     */
    public List<Integer> getStudyIds(List studiesNames, QueryOptions options) {
        return getStudyIds(studiesNames, getStudyConfigurationManager().getStudies(options));
    }

    /**
     * Get studyIds from a list of studies.
     * Replaces studyNames for studyIds.
     * Excludes those studies that starts with '!'
     *
     * @param studiesNames  List of study names or study ids
     * @param studies       Map of available studies. See {@link StudyConfigurationManager#getStudies}
     * @return              List of study Ids
     */
    public List<Integer> getStudyIds(List studiesNames, Map<String, Integer> studies) {
        List<Integer> studiesIds;
        if (studiesNames == null) {
            return Collections.emptyList();
        }
        studiesIds = new ArrayList<>(studiesNames.size());
        for (Object studyObj : studiesNames) {
            Integer studyId = getStudyId(studyObj, true, studies);
            if (studyId != null) {
                studiesIds.add(studyId);
            }
        }
        return studiesIds;
    }

    public Integer getStudyId(Object studyObj, QueryOptions options) {
        return getStudyId(studyObj, options, true);
    }

    public Integer getStudyId(Object studyObj, QueryOptions options, boolean skipNegated) {
        if (studyObj instanceof Integer) {
            return ((Integer) studyObj);
        } else if (studyObj instanceof String && StringUtils.isNumeric((String) studyObj)) {
            return Integer.parseInt((String) studyObj);
        } else {
            return getStudyId(studyObj, skipNegated, getStudyConfigurationManager().getStudies(options));
        }
    }

    public Integer getStudyId(Object studyObj, boolean skipNegated, Map<String, Integer> studies) {
        Integer studyId;
        if (studyObj instanceof Integer) {
            studyId = ((Integer) studyObj);
        } else {
            String studyName = studyObj.toString();
            if (isNegated(studyName)) { //Skip negated studies
                if (skipNegated) {
                    return null;
                } else {
                    studyName = studyName.substring(1);
                }
            }
            if (StringUtils.isNumeric(studyName)) {
                studyId = Integer.parseInt(studyName);
            } else {
                Integer value = studies.get(studyName);
                if (value == null) {
                    throw VariantQueryException.studyNotFound(studyName, studies.keySet());
                }
                studyId = value;
            }
        }
        if (!studies.containsValue(studyId)) {
            throw VariantQueryException.studyNotFound(studyId, studies.keySet());
        }
        return studyId;
    }

    /**
     * Given a study reference (name or id) and a default study, returns the associated StudyConfiguration.
     *
     *
     * @param study     Study reference (name or id)
     * @param defaultStudyConfiguration Default studyConfiguration
     * @return          Assiciated StudyConfiguration
     * @throws    VariantQueryException is the study does not exists
     */
    public StudyConfiguration getStudyConfiguration(String study, StudyConfiguration defaultStudyConfiguration)
            throws VariantQueryException {
        StudyConfiguration studyConfiguration;
        if (StringUtils.isEmpty(study)) {
            studyConfiguration = defaultStudyConfiguration;
            if (studyConfiguration == null) {
                throw VariantQueryException.studyNotFound(study, getStudyConfigurationManager().getStudyNames(null));
            }
        } else if (StringUtils.isNumeric(study)) {
            int studyInt = Integer.parseInt(study);
            if (defaultStudyConfiguration != null && studyInt == defaultStudyConfiguration.getStudyId()) {
                studyConfiguration = defaultStudyConfiguration;
            } else {
                studyConfiguration = getStudyConfigurationManager().getStudyConfiguration(studyInt, null).first();
            }
            if (studyConfiguration == null) {
                throw VariantQueryException.studyNotFound(studyInt, getStudyConfigurationManager().getStudyNames(null));
            }
        } else {
            if (defaultStudyConfiguration != null && defaultStudyConfiguration.getStudyName().equals(study)) {
                studyConfiguration = defaultStudyConfiguration;
            } else {
                studyConfiguration = getStudyConfigurationManager().getStudyConfiguration(study, null).first();
            }
            if (studyConfiguration == null) {
                throw VariantQueryException.studyNotFound(study, getStudyConfigurationManager().getStudyNames(null));
            }
        }
        return studyConfiguration;
    }

    public int getSampleId(Object sampleObj, StudyConfiguration defaultStudyConfiguration) {
        int sampleId;
        if (sampleObj instanceof Number) {
            sampleId = ((Number) sampleObj).intValue();
        } else {
            String sampleStr = sampleObj.toString();
            if (StringUtils.isNumeric(sampleStr)) {
                sampleId = Integer.parseInt(sampleStr);
            } else {
                if (defaultStudyConfiguration != null) {
                    if (!defaultStudyConfiguration.getSampleIds().containsKey(sampleStr)) {
                        throw VariantQueryException.sampleNotFound(sampleStr, defaultStudyConfiguration.getStudyName());
                    }
                    sampleId = defaultStudyConfiguration.getSampleIds().get(sampleStr);
                } else {
                    //Unable to identify that sample!
                    List<String> studyNames = getStudyConfigurationManager().getStudyNames(null);
                    throw VariantQueryException.missingStudyForSample(sampleStr, studyNames);
                }
            }
        }
        return sampleId;
    }

    public List<Integer> getReturnedStudies(Query query, QueryOptions options) {
        List<Integer> studyIds = getStudyIds(query.getAsList(VariantDBAdaptor.VariantQueryParams.RETURNED_STUDIES.key()), options);
        if (studyIds.isEmpty()) {
            studyIds = getStudyIds(getStudyConfigurationManager().getStudyNames(options), options);
        }
        return studyIds;
    }

    public Map<Integer, List<Integer>> getReturnedSamples(Query query, QueryOptions options) {
        List<Integer> returnedStudies = getReturnedStudies(query, options);
        return getReturnedSamples(query, options, returnedStudies, studyId -> getStudyConfigurationManager()
                .getStudyConfiguration(studyId, options).first());

    }

    public static Map<Integer, List<Integer>> getReturnedSamples(Query query, QueryOptions options, StudyConfiguration... studies) {
        return getReturnedSamples(query, options, Arrays.asList(studies));
    }

    public static Map<Integer, List<Integer>> getReturnedSamples(Query query, QueryOptions options,
                                                                 Collection<StudyConfiguration> studies) {
        Map<Integer, StudyConfiguration> map = studies.stream()
                .collect(Collectors.toMap(StudyConfiguration::getStudyId, Function.identity()));
        return getReturnedSamples(query, options, map.keySet(), map::get);
    }

    public static Map<Integer, List<Integer>> getReturnedSamples(Query query, QueryOptions options, Collection<Integer> studyIds,
                                                          Function<Integer, StudyConfiguration> studyProvider) {
        List<String> returnedSamples = getReturnedSamplesList(query, options);
        LinkedHashSet<String> returnedSamplesSet = new LinkedHashSet<>(returnedSamples);

        Map<Integer, List<Integer>> samples = new HashMap<>(studyIds.size());
        for (Integer studyId : studyIds) {
            StudyConfiguration sc = studyProvider.apply(studyId);
            if (sc == null) {
                continue;
            }
            LinkedHashMap<String, Integer> returnedSamplesPosition = StudyConfiguration.getReturnedSamplesPosition(sc, returnedSamplesSet);
            List<Integer> sampleNames = Arrays.asList(new Integer[returnedSamplesPosition.size()]);
            returnedSamplesPosition.forEach((sample, position) -> sampleNames.set(position, sc.getSampleIds().get(sample)));
            samples.put(studyId, sampleNames);
        }

        return samples;
    }

    public static List<String> getReturnedSamplesList(Query query, QueryOptions options) {
        return getReturnedSamplesList(query, VariantField.getReturnedFields(options));
    }

    public static List<String> getReturnedSamplesList(Query query, Set<VariantField> returnedFields) {
        if (!returnedFields.contains(VariantField.STUDIES_SAMPLES_DATA)) {
            return Collections.singletonList("none");
        } else {
            //Remove the studyName, if any
            return getReturnedSamplesList(query);
        }
    }

    public static List<String> getReturnedSamplesList(Query query) {
        return query.getAsStringList(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key())
                .stream()
                .map(s -> s.contains(":") ? s.split(":")[1] : s)
                .collect(Collectors.toList());
    }

    /**
     * Finds the cohortId from a cohort reference.
     *
     * @param cohort    Cohort reference (name or id)
     * @param studyConfiguration  Default study configuration
     * @return  Cohort id
     * @throws VariantQueryException if the cohort does not exist
     */
    public int getCohortId(String cohort, StudyConfiguration studyConfiguration) throws VariantQueryException {
        int cohortId;
        if (StringUtils.isNumeric(cohort)) {
            cohortId = Integer.parseInt(cohort);
            if (!studyConfiguration.getCohortIds().containsValue(cohortId)) {
                throw VariantQueryException.cohortNotFound(cohortId, studyConfiguration.getStudyId(),
                        studyConfiguration.getCohortIds().keySet());
            }
        } else {
            Integer cohortIdNullable = studyConfiguration.getCohortIds().get(cohort);
            if (cohortIdNullable == null) {
                throw VariantQueryException.cohortNotFound(cohort, studyConfiguration.getStudyId(),
                        studyConfiguration.getCohortIds().keySet());
            }
            cohortId = cohortIdNullable;
        }
        return cohortId;
    }

    public Region getGeneRegion(String geneStr) {
        QueryOptions params = new QueryOptions(QueryOptions.INCLUDE, "name,chromosome,start,end");
        try {
            Gene gene = adaptor.getCellBaseClient().getGeneClient().get(Collections.singletonList(geneStr), params).firstResult();
            if (gene != null) {
                int start = Math.max(0, gene.getStart() - GENE_EXTRA_REGION);
                int end = gene.getEnd() + GENE_EXTRA_REGION;
                return new Region(gene.getChromosome(), start, end);
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Set<String> getGenesByGo(List<String> goValues) {
        Set<String> genes = new HashSet<>();
        QueryOptions params = new QueryOptions(QueryOptions.INCLUDE, "name,chromosome,start,end");
        try {
            List<QueryResult<Gene>> responses = adaptor.getCellBaseClient().getGeneClient().get(goValues, params)
                    .getResponse();
            for (QueryResult<Gene> response : responses) {
                for (Gene gene : response.getResult()) {
                    genes.add(gene.getName());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return genes;
    }

    public Set<String> getGenesByExpression(List<String> expressionValues) {
        Set<String> genes = new HashSet<>();
        QueryOptions params = new QueryOptions(QueryOptions.INCLUDE, "name,chromosome,start,end");

        // The number of results for each expression value may be huge. Query one by one
        for (String expressionValue : expressionValues) {
            try {
                String[] split = expressionValue.split(":");
                expressionValue = split[0];
                // TODO: Add expression value {UP, DOWN}. See https://github.com/opencb/cellbase/issues/245
                Query cellbaseQuery = new Query(GeneDBAdaptor.QueryParams.ANNOTATION_EXPRESSION_TISSUE.key(), expressionValue);
                List<QueryResult<Gene>> responses = adaptor.getCellBaseClient().getGeneClient().search(cellbaseQuery, params)
                        .getResponse();
                for (QueryResult<Gene> response : responses) {
                    for (Gene gene : response.getResult()) {
                        genes.add(gene.getName());
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return genes;
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
                throw VariantQueryException.malformedParam(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE, so,
                        "Not a valid SO number");
            }
            if (!ConsequenceTypeMappings.accessionToTerm.containsKey(soAccession)) {
                throw VariantQueryException.malformedParam(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE, so,
                        "Not a valid SO number");
            }
        } else {
            if (!ConsequenceTypeMappings.termToAccession.containsKey(so)) {
                throw VariantQueryException.malformedParam(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE, so,
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
        if (operation == null) {
            list = Collections.singletonList(value);
        } else if (operation == QueryOperation.AND) {
            list = Arrays.asList(value.split(QueryOperation.AND.separator()));
        } else {
            list = Arrays.asList(value.split(QueryOperation.OR.separator()));
        }
        return list;
    }

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

}
