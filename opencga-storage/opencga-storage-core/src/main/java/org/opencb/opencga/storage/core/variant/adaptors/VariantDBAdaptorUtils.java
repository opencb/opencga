package org.opencb.opencga.storage.core.variant.adaptors;

import org.apache.commons.lang3.StringUtils;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        return getStudyIds(getStudyConfigurationManager().getStudyNames(options), options);
    }
    public List<Integer> getStudyIds(List studiesNames, QueryOptions options) {
        List<Integer> studiesIds;
        if (studiesNames == null) {
            return Collections.emptyList();
        }
        studiesIds = new ArrayList<>(studiesNames.size());
        for (Object studyObj : studiesNames) {
            Integer studyId = getStudyId(studyObj, options);
            if (studyId != null) {
                studiesIds.add(studyId);
            }
        }
        return studiesIds;
    }

    public Integer getStudyId(Object studyObj, QueryOptions options) {
        return getStudyId(studyObj, options, true);
    }

    public Integer getStudyId(Object studyObj, QueryOptions options, boolean skipNull) {
        Integer studyId;
        if (studyObj instanceof Integer) {
            studyId = ((Integer) studyObj);
        } else {
            String studyName = studyObj.toString();
            if (skipNull && studyName.startsWith("!")) { //Skip negated studies
                studyId = null;
            } else {
                if (StringUtils.isNumeric(studyName)) {
                    studyId = Integer.parseInt(studyName);
                } else {
                    QueryResult<StudyConfiguration> result = getStudyConfigurationManager()
                            .getStudyConfiguration(studyName, options);
                    if (result.getResult().isEmpty()) {
                        throw VariantQueryException.studyNotFound(studyName);
                    }
                    studyId = result.first().getStudyId();
                }
            }
        }
        return studyId;
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
