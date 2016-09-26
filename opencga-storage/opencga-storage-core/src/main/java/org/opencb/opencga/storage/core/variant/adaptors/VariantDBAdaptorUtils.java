package org.opencb.opencga.storage.core.variant.adaptors;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Gene;
import org.opencb.cellbase.core.api.GeneDBAdaptor;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
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
    public static final Map<String, String> PROJECT_FIELD_ALIAS;

    public static final String SAMPLES_FIELD = "samples";
    public static final String STUDIES_FIELD = "studies";
    public static final String STATS_FIELD = "stats";
    public static final String ANNOTATION_FIELD = "annotation";

    static {
        Map<String, String> map =  new HashMap<>();
        map.put("studies.samplesData", SAMPLES_FIELD);
        map.put("samplesData", SAMPLES_FIELD);
        map.put(SAMPLES_FIELD, SAMPLES_FIELD);
        map.put("sourceEntries", STUDIES_FIELD);
        map.put("studies.cohortStats", STATS_FIELD);
        map.put("studies.stats", STATS_FIELD);
        map.put("sourceEntries.stats", STATS_FIELD);
        map.put(STATS_FIELD, STATS_FIELD);
        map.put(STUDIES_FIELD, STUDIES_FIELD);
        map.put(ANNOTATION_FIELD, ANNOTATION_FIELD);
        PROJECT_FIELD_ALIAS = Collections.unmodifiableMap(map);
    }

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
        Map<String, Integer> studies = getStudyConfigurationManager().getStudies(options);
        List<Integer> studiesIds;
        if (studiesNames == null) {
            return Collections.emptyList();
        }
        studiesIds = new ArrayList<>(studiesNames.size());
        for (Object studyObj : studiesNames) {
            Integer studyId = getStudyId(studyObj, options, true, studies);
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
            return getStudyId(studyObj, options, skipNegated, getStudyConfigurationManager().getStudies(options));
        }
    }

    private Integer getStudyId(Object studyObj, QueryOptions options, boolean skipNegated, Map<String, Integer> studies) {
        Integer studyId;
        if (studyObj instanceof Integer) {
            studyId = ((Integer) studyObj);
        } else {
            String studyName = studyObj.toString();
            if (studyName.startsWith("!")) { //Skip negated studies
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
                    throw VariantQueryException.studyNotFound(studyName);
                }
                studyId = value;
            }
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

    public Set<String> getReturnedFields(QueryOptions options) {
        Set<String> returnedFields;

        List<String> includeList = options.getAsStringList(QueryOptions.INCLUDE);
        if (includeList != null && !includeList.isEmpty()) {
//            System.out.println("includeList = " + includeList);
            returnedFields = new HashSet<>();
            for (String include : includeList) {
                returnedFields.add(PROJECT_FIELD_ALIAS.get(include));
            }
            if (returnedFields.contains(STUDIES_FIELD)) {
                returnedFields.add(SAMPLES_FIELD);
                returnedFields.add(STATS_FIELD);
            } else if (returnedFields.contains(SAMPLES_FIELD) || returnedFields.contains(STATS_FIELD)) {
                returnedFields.add(STUDIES_FIELD);
            }

        } else {
            List<String> excludeList = options.getAsStringList(QueryOptions.EXCLUDE);
            if (excludeList != null && !excludeList.isEmpty()) {
//                System.out.println("excludeList = " + excludeList);
                returnedFields = new HashSet<>(PROJECT_FIELD_ALIAS.values());
                for (String exclude : excludeList) {
                    returnedFields.remove(PROJECT_FIELD_ALIAS.get(exclude));
                }
            } else {
                returnedFields = new HashSet<>(PROJECT_FIELD_ALIAS.values());
            }
        }
//        System.out.println("returnedFields = " + returnedFields);
        return returnedFields;
    }

    public List<String> getReturnedSamples(Query query, QueryOptions options) {
        if (!getReturnedFields(options).contains(SAMPLES_FIELD)) {
            return Collections.singletonList("none");
        } else {
            //Remove the studyName, if any
            return getReturnedSamples(query);
        }
    }

    public List<String> getReturnedSamples(Query query) {
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


    public Set<String> getGenesByGo(List<String> goValues) {
        System.out.println("goValues = " + goValues);
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
