/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.catalog.stats.solr;

import org.apache.commons.collections.map.LinkedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.params.CommonParams;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.solr.FacetQueryParser;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * Created by wasim on 09/07/18.
 */

public class CatalogSolrQueryParser {

    protected static Logger logger = LoggerFactory.getLogger(CatalogSolrQueryParser.class);

    public static final Pattern OPERATION_PATTERN = Pattern.compile("^(<=?|>=?|!==?|!?=?~|==?=?)([^=<>~!]+.*)$");

    private Map<String, String> aliasMap = new HashMap<>();

    enum QueryParams {

        // Common
        STUDY("study", TEXT),
        RELEASE("release", INTEGER),
        CREATION_YEAR("creationYear", INTEGER),
        CREATION_MONTH("creationMonth", TEXT),
        CREATION_DAY("creationDay", INTEGER),
        CREATION_DAY_OF_WEEK("creationDayOfWeek", TEXT),
        STATUS("status", TEXT),
        ANNOTATIONS(Constants.ANNOTATION, TEXT),
        ACL("acl", TEXT),

        // Shared
        VERSION("version", INTEGER),
        TYPE("type", TEXT),
        PHENOTYPES("phenotypes", TEXT_ARRAY),
        NUM_SAMPLES("numSamples", INTEGER),

        // Sample
        SOURCE("source", TEXT),
        SOMATIC("somatic", BOOLEAN),

        // Family
        NUM_MEMBERS("numMembers", INTEGER),
        EXPECTED_SIZE("expectedSize", INTEGER),

        // File
        NAME("name", TEXT),
        FORMAT("format", TEXT),
        BIOFORMAT("bioformat", TEXT),
        EXTERNAL("external", BOOLEAN),
        SIZE("size", LONG),
        SOFTWARE("software", TEXT),
        EXPERIMENT("experiment", TEXT),
        NUM_RELATED_FILES("numRelatedFiles", INTEGER),

        // Individual
        HAS_FATHER("hasFather", BOOLEAN),
        HAS_MOTHER("hasMother", BOOLEAN),
        SEX("sex", TEXT),
        KARYOTYPIC_SEX("karyotypicSex", TEXT),
        ETHNICITY("ethnicity", TEXT),
        POPULATION("population", TEXT),
        LIFE_STATUS("lifeStatus", TEXT),
        AFFECTATION_STATUS("affectationStatus", TEXT),
        PARENTAL_CONSANGUINITY("parentalConsanguinity", BOOLEAN);

        private static Map<String, QueryParams> map;
        static {
            map = new LinkedMap();
            for (QueryParams params : QueryParams.values()) {
                map.put(params.key(), params);
            }
        }

        private final String key;
        private QueryParam.Type type;

        QueryParams(String key, QueryParam.Type type) {
            this.key = key;
            this.type = type;
        }

        public String key() {
            return key;
        }

        public QueryParam.Type type() {
            return type;
        }

        public static Map<String, QueryParams> getMap() {
            return map;
        }

        public static QueryParams getParam(String key) {
            return map.get(key);
        }
    }

    public CatalogSolrQueryParser() {
    }

    /**
     * Create a SolrQuery object from Query and QueryOptions.
     *
     * @param query        Query
     * @param queryOptions Query Options
     * @param variableSetList List of variable sets available for the study whose query is going to be parsed.
     * @throws CatalogException CatalogException
     * @return SolrQuery
     */
    public SolrQuery parse(Query query, QueryOptions queryOptions, List<VariableSet> variableSetList) throws CatalogException {

        Map<String, String> filterList = new HashMap<>();
        SolrQuery solrQuery = new SolrQuery();

        ObjectMap variableMap = generateVariableMap(variableSetList);

        //-------------------------------------
        // Facet processing
        //-------------------------------------
        if (queryOptions.containsKey(QueryOptions.FACET) && StringUtils.isNotEmpty(queryOptions.getString(QueryOptions.FACET))) {
            replaceAnnotationFormat(queryOptions, QueryOptions.FACET, variableMap);
            FacetQueryParser facetQueryParser = new FacetQueryParser();
            String facetQuery;
            try {
                facetQuery = facetQueryParser.parse(queryOptions.getString(QueryOptions.FACET));
            } catch (Exception e) {
                throw new CatalogException("Solr parse exception: " + e.getMessage(), e);
            }
            if (StringUtils.isNotEmpty(facetQuery)) {
                solrQuery.set("json.facet", facetQuery);
            }
        }

        query.entrySet().forEach(entry -> {
            QueryParams queryParam = QueryParams.getParam(entry.getKey());
            if (queryParam != null) {
                if (queryParam == QueryParams.STUDY) {
                    filterList.put("studyId", query.getString(queryParam.key()).replace(":", "__"));
                } else if (queryParam == QueryParams.ACL) {
                    filterList.put(entry.getKey(), (String) entry.getValue());
                } else if (queryParam == QueryParams.ANNOTATIONS) {
                    try {
                        filterList.putAll(parseAnnotationQueryField(query.getString(Constants.ANNOTATION),
                                query.get(Constants.PRIVATE_ANNOTATION_PARAM_TYPES, ObjectMap.class), variableSetList));
                    } catch (CatalogException e) {
                        logger.warn("Error parsing annotation info: {}", e.getMessage(), e);
                    }
                } else {
                    try {
                        filterList.put(entry.getKey(), getValues(Arrays.asList(String.valueOf(entry.getValue()).split(",")),
                                queryParam.type()));
                    } catch (CatalogException e) {
                        logger.warn("Error parsing parameter {}: {}", entry.getKey(), e.getMessage(), e);
                    }
                }
            }
        });

        solrQuery.setQuery("*:*");
        // We only want stats, so we avoid retrieving the first 10 results
        solrQuery.add(CommonParams.ROWS, "0");
        filterList.forEach((queryParamter, filter) -> {
            solrQuery.addFilterQuery(queryParamter.concat(":").concat(filter));
            logger.debug("Solr fq: {}\n", filter);
        });

        return solrQuery;

    }

    public Map<String, String> getAliasMap() {
        return aliasMap;
    }

    private void replaceAnnotationFormat(QueryOptions queryOptions, String facetType, ObjectMap variableMap) {
        // variableMap format: Map of full variable path -> Map
        //                                                      type
        //                                                      variableSetId
        String facet = queryOptions.getString(facetType);
        final String prefix = "annotation.";

        // Look for annotation. in facet string
        int index = facet.indexOf(prefix);
        String copy = facet;
        while (index != -1) {
            int lastIndex = getMin(copy.indexOf(",", index), Integer.MAX_VALUE);
            lastIndex = getMin(copy.indexOf(";", index), lastIndex);
            lastIndex = getMin(copy.indexOf("<", index), lastIndex);
            lastIndex = getMin(copy.indexOf(">", index), lastIndex);
            lastIndex = getMin(copy.indexOf("=", index), lastIndex);

            String annotation;
            if (lastIndex == Integer.MAX_VALUE) {
                // + 11 because we exclude annotation. prefix
                annotation = copy.substring(index + 11);

                // We replace the user input for the annotation key format stored in solr
                String annotationReplacement = getInternalAnnotationKey(annotation, variableMap);
                facet = facet.replace(prefix + annotation, annotationReplacement);

                // We add the field we have replaced to the map
                aliasMap.put(annotationReplacement, "annotation." + annotation);

                index = -1;
            } else {
                // + 11 because we exclude annotation. prefix
                annotation = copy.substring(index + 11, lastIndex);

                // We replace the user input for the annotation key format stored in solr
                String annotationReplacement = getInternalAnnotationKey(annotation, variableMap);
                facet = facet.replace(prefix + annotation, annotationReplacement);

                // We add the field we have replaced to the map
                aliasMap.put(annotationReplacement, "annotation." + annotation);

                copy = copy.substring(lastIndex);
                index = copy.indexOf(prefix);
            }
        }

        queryOptions.put(facetType, facet);
    }

    private String getInternalAnnotationKey(String annotation, ObjectMap variableMap) {
        ObjectMap annotationMap = (ObjectMap) variableMap.get(annotation);
        if (annotationMap == null || annotationMap.isEmpty() && annotation.contains(".")) {
            String dynamicAnnotation = annotation.substring(0, annotation.lastIndexOf(".")) + ".*";
            annotationMap = (ObjectMap) variableMap.get(dynamicAnnotation);
        }
        if (annotationMap == null || annotationMap.isEmpty()) {
            logger.error("Cannot parse " + annotation + " string to internal annotation format");
            return "";
        }

        try {
            return "annotations" + getAnnotationType((QueryParam.Type) annotationMap.get("type"))
                    + annotationMap.getString("variableSetId") + "." + annotation;
        } catch (CatalogException e) {
            logger.error("Cannot parse " + annotation + " string to internal annotation format");
            return "";
        }
    }

    private int getMin(int value, int currentValue) {
        if (value == -1) {
            return currentValue;
        }
        return value < currentValue ? value : currentValue;
    }

    private ObjectMap generateVariableMap(List<VariableSet> variableSetList) {
        if (variableSetList == null || variableSetList.isEmpty()) {
            return new ObjectMap();
        }
        // Full variable path -> Map
        //                         type:
        //                         variableSetId:
        ObjectMap variableMap = new ObjectMap();
        for (VariableSet variableSet : variableSetList) {
            Queue<ObjectMap> queue = new LinkedList<>();
            for (Variable variable : variableSet.getVariables()) {
                // We add the current variable to the queue
                ObjectMap auxiliarVariable = new ObjectMap()
                        .append("variable", variable)
                        .append("fullVariablePath", "")
                        .append("isParentArray", false);
                queue.add(auxiliarVariable);
            }

            while (!queue.isEmpty()) {
                ObjectMap auxiliarVariable = queue.remove();
                Variable variable = auxiliarVariable.get("variable", Variable.class);
                boolean isParentArray = auxiliarVariable.getBoolean("isParentArray");
                String fullVariablePath = auxiliarVariable.getString("fullVariablePath");
                if (StringUtils.isEmpty(fullVariablePath)) {
                    fullVariablePath = variable.getId();
                } else {
                    fullVariablePath = fullVariablePath + "." + variable.getId();
                }

                ObjectMap auxVariableMap = new ObjectMap("variableSetId", variableSet.getId());
                switch (variable.getType()) {
                    case BOOLEAN:
                        auxVariableMap.put("type", isParentArray || variable.isMultiValue()
                                ? QueryParam.Type.BOOLEAN_ARRAY
                                : QueryParam.Type.BOOLEAN);
                        variableMap.put(fullVariablePath, auxVariableMap);
                        break;
                    case CATEGORICAL:
                    case STRING:
                        auxVariableMap.put("type", isParentArray || variable.isMultiValue()
                                ? QueryParam.Type.TEXT_ARRAY
                                : QueryParam.Type.TEXT);
                        variableMap.put(fullVariablePath, auxVariableMap);
                        break;
                    case INTEGER:
                        auxVariableMap.put("type", isParentArray || variable.isMultiValue()
                                ? QueryParam.Type.INTEGER_ARRAY
                                : QueryParam.Type.INTEGER);
                        variableMap.put(fullVariablePath, auxVariableMap);
                        break;
                    case DOUBLE:
                        auxVariableMap.put("type", isParentArray || variable.isMultiValue()
                                ? QueryParam.Type.DECIMAL_ARRAY
                                : QueryParam.Type.DECIMAL);
                        variableMap.put(fullVariablePath, auxVariableMap);
                        break;
                    case OBJECT:
                        if (variable.getVariableSet() != null && !variable.getVariableSet().isEmpty()) {
                            for (Variable nestedVariable : variable.getVariableSet()) {
                                ObjectMap nestedAuxiliarVariable = new ObjectMap()
                                        .append("variable", nestedVariable)
                                        .append("fullVariablePath", fullVariablePath)
                                        .append("isParentArray", isParentArray || variable.isMultiValue());
                                queue.add(nestedAuxiliarVariable);
                            }
                        }
                        break;
                    case MAP_BOOLEAN:
                        auxVariableMap.put("type", isParentArray || variable.isMultiValue() ? BOOLEAN_ARRAY : BOOLEAN);
                        variableMap.put(fullVariablePath + ".*", auxVariableMap);
                        break;
                    case MAP_INTEGER:
                        auxVariableMap.put("type", isParentArray || variable.isMultiValue() ? INTEGER_ARRAY : INTEGER);
                        variableMap.put(fullVariablePath + ".*", auxVariableMap);
                        break;
                    case MAP_DOUBLE:
                        auxVariableMap.put("type", isParentArray || variable.isMultiValue() ? DECIMAL_ARRAY : DECIMAL);
                        variableMap.put(fullVariablePath + ".*", auxVariableMap);
                        break;
                    case MAP_STRING:
                        auxVariableMap.put("type", isParentArray || variable.isMultiValue() ? TEXT_ARRAY : TEXT);
                        variableMap.put(fullVariablePath + ".*", auxVariableMap);
                        break;
                    default:
                        break;
                }
            }
        }

        return variableMap;
    }

    private Map<String, String> parseAnnotationQueryField(String annotations, ObjectMap variableTypeMap, List<VariableSet> variableSetList)
            throws CatalogException {

//        Map<Long, String> variableSetUidIdMap = new HashMap<>();
//        variableSetList.forEach(variableSet -> variableSetUidIdMap.put(variableSet.getUid(), variableSet.getId()));

        Map<String, String> annotationMap = new HashMap<>();

        if (StringUtils.isNotEmpty(annotations)) {
            // Annotation Filter
            final String sepAnd = ";";
            String[] annotationArray = StringUtils.split(annotations, sepAnd);

            for (String annotation : annotationArray) {
                Matcher matcher = AnnotationUtils.ANNOTATION_PATTERN.matcher(annotation);

                if (matcher.find()) {
                    String valueString = matcher.group(4);

                    if (annotation.startsWith(Constants.ANNOTATION_SET_NAME)) {
                        annotationMap.put("annotationSets", getValues(Arrays.asList(valueString.split(",")), QueryParam.Type.TEXT));
                    } else { // annotation
                        // Split the annotation by key - value
                        // Remove the : at the end of the variableSet
                        String variableSet = matcher.group(2).replace(":", "");
                        // long variableSetUid = Long.valueOf(matcher.group(1).replace(":", ""));
                        String key = matcher.group(3);

                        if (variableTypeMap == null || variableTypeMap.isEmpty()) {
                            logger.error("Internal error: The variableTypeMap is null or empty {}", variableTypeMap);
                            throw new CatalogException("Internal error. Could not build the annotation query");
                        }
                        QueryParam.Type type = variableTypeMap.get(variableSet + ":" + key, QueryParam.Type.class);
                        if (type == null) {
                            logger.error("Internal error: Could not find the type of the variable {}:{}", variableSet, key);
                            throw new CatalogException("Internal error. Could not find the type of the variable " + variableSet + ":"
                                    + key);
                        }

                        annotationMap.put("annotations" + getAnnotationType(type) + variableSet + "." + key,
                                getValues(Arrays.asList(valueString.split(",")), type));
                    }
                } else {
                    throw new CatalogDBException("Annotation " + annotation + " could not be parsed to a query.");
                }
            }
        }

        return annotationMap;
    }

    private String getAnnotationType(QueryParam.Type type) throws CatalogException {
        switch (type) {
            case TEXT:
                return "__s__";
            case TEXT_ARRAY:
                return "__sm__";
            case INTEGER:
                return "__i__";
            case INTEGER_ARRAY:
                return "__im__";
            case DECIMAL:
                return "__d__";
            case DECIMAL_ARRAY:
                return "__dm__";
            case BOOLEAN:
                return "__b__";
            case BOOLEAN_ARRAY:
                return "__bm__";
            default:
                throw new CatalogException("Unexpected variable type " + type);

        }
    }

    private String getValues(List<String> values, QueryParam.Type type) throws CatalogException {
        ArrayList<String> or = new ArrayList<>(values.size());
        for (String option : values) {
            Matcher matcher = OPERATION_PATTERN.matcher(option);
            String operator;
            String filter;
            if (!matcher.find()) {
                operator = "";
                filter = option;
            } else {
                operator = matcher.group(1);
                filter = matcher.group(2);
            }
            switch (type) {
                case INTEGER:
                case INTEGER_ARRAY:
                    try {
                        int intValue = Integer.parseInt(filter);
                        or.add(addNumberOperationQueryFilter(operator, intValue, intValue - 1, intValue + 1));
                    } catch (NumberFormatException e) {
                        throw new CatalogDBException("Expected an integer value - " + e.getMessage(), e);
                    }
                    break;
                case DECIMAL:
                case DECIMAL_ARRAY:
                    try {
                        double doubleValue = Double.parseDouble(filter);
                        or.add(addNumberOperationQueryFilter(operator, doubleValue, doubleValue - 1, doubleValue + 1));
                    } catch (NumberFormatException e) {
                        throw new CatalogDBException("Expected a double value - " + e.getMessage(), e);
                    }
                    break;
                case TEXT:
                case TEXT_ARRAY:
                    or.add(addStringOperationQueryFilter(operator, filter));
                    break;
                case BOOLEAN:
                case BOOLEAN_ARRAY:
                    or.add(addBooleanOperationQueryFilter(operator, Boolean.parseBoolean(filter)));
                    break;
                default:
                    break;
            }
        }
        if (or.isEmpty()) {
            return "";
        } else if (or.size() == 1) {
            return or.get(0);
        } else {
            return "(" + StringUtils.join(or, " OR ") + ")";
        }
    }

    private String addBooleanOperationQueryFilter(String operator, boolean value) throws CatalogException {
        String query;
        switch (operator) {
            case "!=":
                query = "(" + !value + ")";
                break;
            case "":
            case "=":
            case "==":
                query = "(" + value + ")";
                break;
            default:
                throw new CatalogException("Unknown boolean query operation " + operator);
        }
        return query;
    }

    private String addStringOperationQueryFilter(String operator, String filter) throws CatalogException {
        String query;
        switch (operator) {
            case "!=":
                query = "(*:* -" + filter + ")";
                break;
            case "":
            case "=":
            case "==":
                query = "(" + filter + ")";
                break;
            default:
                throw new CatalogException("Unknown string query operation " + operator);
        }
        return query;
    }

    private String addNumberOperationQueryFilter(String operator, Number value, Number valueMinus1, Number valuePlus1)
            throws CatalogException {
        String query;
        switch (operator) {
            case "<":
                query = "[* TO " + valueMinus1 + "]";
                break;
            case "<=":
                query = "[* TO " + value + "]";
                break;
            case ">":
                query = "[" + valuePlus1 + " TO *]";
                break;
            case ">=":
                query = "[" + value + " TO *]";
                break;
            case "!=":
                query = "-[" + value + " TO " + value + "]";
                break;
            case "":
            case "=":
            case "==":
                query = "[" + value + " TO " + value + "]";
                break;
            default:
                throw new CatalogException("Unknown string query operation " + operator);
        }
        return query;
    }


}
