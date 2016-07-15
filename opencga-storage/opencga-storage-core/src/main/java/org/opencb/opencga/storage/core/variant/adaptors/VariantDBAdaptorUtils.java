package org.opencb.opencga.storage.core.variant.adaptors;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;

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

    public static final Pattern OPERATION_PATTERN = Pattern.compile("^([^=<>~!]*)(<=?|>=?|!=|!?=?~|==?)([^=<>~!]+.*)$");

    public static final String OR = ",";
    public static final String AND = ";";
    public static final String IS = ":";

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
        } else if (containsAnd && !containsOr) {
            return QueryOperation.AND;
        } else if (containsOr && !containsAnd) {
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
