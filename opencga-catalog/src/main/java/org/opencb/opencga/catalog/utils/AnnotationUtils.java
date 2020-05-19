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

package org.opencb.opencga.catalog.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;

import javax.annotation.Nullable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.managers.AnnotationSetManager.*;
import static org.opencb.opencga.core.common.JacksonUtils.getDefaultObjectMapper;

/**
 * Created by jacobo on 14/12/14.
 */
public class AnnotationUtils {

    public static final Pattern ANNOTATION_PATTERN = Pattern.compile("^([^:=^<>~!$]+:)?([^=^<>~!:$]+)([=^<>~!$]+.+)$");
    public static final Pattern OPERATION_PATTERN = Pattern.compile("^()(<=?|>=?|!==?|!?=?~|==?=?)([^=<>~!]+.*)$");

    public static void checkVariableSet(VariableSet variableSet) throws CatalogException {
        Set<String> variableIdSet = new HashSet<>();
        for (Variable variable : variableSet.getVariables()) {
            if (variableIdSet.contains(variable.getId())) {
                throw new CatalogException("Duplicated variable Id");
            }
            variableIdSet.add(variable.getId());
        }
        for (Variable variable : variableSet.getVariables()) {
            checkVariable(variable);
        }
    }

    public static void checkVariable(Variable variable) throws CatalogException {
        List<String> acceptedValues = new LinkedList<>();
        if (variable.getAllowedValues() != null) {
            acceptedValues.addAll(variable.getAllowedValues());
//            for (String acceptedValue : variable.getAllowedValues()) {
//                if(acceptedValue.startsWith("\"") && acceptedValue.endsWith("\"")) {
//                    acceptedValue = acceptedValue.substring(1, acceptedValue.length()-1);
//                }
//                Collections.addAll(acceptedValues, acceptedValue.split(","));
//            }
        }
        variable.setAllowedValues(acceptedValues);

        if (variable.getType() == null) {
            throw new CatalogException("VariableType is null");
        }
        if (ListUtils.isNotEmpty(variable.getVariableSet()) && variable.getType() != Variable.VariableType.OBJECT) {
            throw new CatalogException("Only variables with type \"OBJECT\" can define an internal variableSet");
        }

        if (variable.getType() == Variable.VariableType.TEXT) {
            variable.setType(Variable.VariableType.STRING);
        }

        //Check default values
        switch (variable.getType()) {
            case BOOLEAN:
                if (!variable.getAllowedValues().isEmpty()) {
                    throw new CatalogException("Variable type boolean can not contain accepted values");
                }
                break;
            case CATEGORICAL:
                break;
            case INTEGER:
            case DOUBLE: {
                //Check accepted values
                if (!variable.getAllowedValues().isEmpty()) {
                    for (String range : variable.getAllowedValues()) {
                        String[] split = range.split(":", -1);
                        if (split.length != 2) {
                            throw new CatalogException("Invalid numerical range. Expected <min>:<max>");
                        }
                        Double min;
                        Double max;
                        try {
                            min = split[0].isEmpty() ? Double.MIN_VALUE : Double.valueOf(split[0]);
                            max = split[1].isEmpty() ? Double.MAX_VALUE : Double.valueOf(split[1]);
                        } catch (NumberFormatException e) {
                            throw new CatalogException("Invalid numerical range. Expected <min>:<max> where min and max are numerical.", e);
                        }
                        if (min > max) {
                            throw new CatalogException("Invalid numerical range. Expected <min>:<max> where min <= max");
                        }
                    }
                }
                break;
            }
            case STRING:
                break;
            case OBJECT:
                if (variable.getVariableSet() != null) {
                    for (Variable v : variable.getVariableSet()) {
                        checkVariable(v);
                    }
                }
                break;
            case MAP_BOOLEAN:
            case MAP_INTEGER:
            case MAP_DOUBLE:
            case MAP_STRING:
                if (variable.getVariableSet() != null && !variable.getVariableSet().isEmpty()) {
                    throw new CatalogException("Variable " + variable.getId() + " of type " + variable.getType().name() + " cannot "
                            + "have an internal array of VariableSets");
                }
                break;
            default:
                throw new CatalogException("Unknown VariableType " + variable.getType().name());
        }

        //Check default value
        variable.setDefaultValue(getValue(variable.getType(), variable.getDefaultValue()));
        if (variable.getDefaultValue() != null) {
            checkAllowedValue(variable, variable.getDefaultValue(), "Default");
        }
    }

    /**
     * Checks whether a variable id is defined in the given VariableSet.
     * Example: variableId = a.b.c will check if there is an object variable a containing an object variable b containing a variable c.
     * @param variableId Variable id to be checked.
     * @param variableSet Variable set where the variable id will be checked.
     * @return a boolean indicating whether the variable id is found in the given variable set or not.
     */
    public static boolean checkVariableIdInVariableSet(String variableId, Set<Variable> variableSet) {
        if (variableSet == null) {
            return false;
        }

        String[] split = StringUtils.split(variableId, ".");
        if (split.length == 1) {
            for (Variable variable : variableSet) {
                if (variable.getId().equals(split[0])) {
                    return true;
                }
            }
        } else {
            for (Variable variable : variableSet) {
                if (variable.getId().equals(split[0])) {
                    String subVariableId = StringUtils.split(variableId, ".", 2)[1];
                    return checkVariableIdInVariableSet(subVariableId, variable.getVariableSet());
                }
            }
        }
        return false;
    }

    /**
     * Check if an annotationSet is valid.
     *
     * @param variableSet    VariableSet that describes the annotationSet.
     * @param annotationSet  AnnotationSet to check
     * @param annotationSets All the AnnotationSets of the sample
     * @param addDefaultValues Boolean indicating whether to add missing annotations if there is a default value set for the variable.
     * @throws CatalogException CatalogException
     */
    public static void checkAnnotationSet(VariableSet variableSet, AnnotationSet annotationSet,
                                          @Nullable List<AnnotationSet> annotationSets, boolean addDefaultValues) throws CatalogException {
        if (!variableSet.getId().equals(annotationSet.getVariableSetId())) {
            throw new CatalogException("VariableSet does not match with the AnnotationSet");
        }

        //Check unique variableSet.
        if (variableSet.isUnique() && annotationSets != null) {
            for (AnnotationSet set : annotationSets) {
                if (set.getVariableSetId().equals(annotationSet.getVariableSetId())) {
                    throw new CatalogException("Repeated annotation for a unique VariableSet");
                }
            }
        }

        //Get annotationSetName set and variableId map
        Set<String> annotatedVariables = annotationSet.getAnnotations().entrySet()
                .stream()
                .map(annotation -> annotation.getKey())
                .collect(Collectors.toSet());
        Map<String, Variable> variableMap = new HashMap<>();
        for (Variable variable : variableSet.getVariables()) {
            variableMap.put(variable.getId(), variable);
        }

//        //Remove null values
//        for (Iterator<Map.Entry<String, Object>> iterator = annotationSet.getAnnotations().entrySet().iterator(); iterator.hasNext();) {
//            Map.Entry<String, Object> annotation = iterator.next();
//            if (annotation.getValue() == null) {
//                iterator.remove();
//            }
//        }

        //Check for missing values
        Map<String, Object> defaultAnnotations = new HashMap<>();
        for (Variable variable : variableSet.getVariables()) {
            if (!annotatedVariables.contains(variable.getId())) {
                if (addDefaultValues) {
                    if (!addDefaultAnnotation(variable, defaultAnnotations)) {
                        if (variable.isRequired()) {
                            throw new CatalogException("Missing required variable " + variable.getId());
                        }
                    }
                    annotatedVariables.add(variable.getId());
                } else {
                    // We don't attempt to add missing annotations
                    if (variable.isRequired()) {
                        throw new CatalogException("Missing required variable " + variable.getId());
                    }
                }
            }
        }
        annotationSet.getAnnotations().putAll(defaultAnnotations);

        //Check annotations
        checkAnnotations(variableMap, annotationSet.getAnnotations());

    }

    public static void checkAnnotations(Map<String, Variable> variableMap, Map<String, Object> annotations)
            throws CatalogException {
        for (Map.Entry<String, Object> entry : annotations.entrySet()) {
            String id = entry.getKey();
            if (!variableMap.containsKey(id)) {
                throw new CatalogException("Annotation id '" + id + "' is not an accepted id");
            } else {
                Variable variable = variableMap.get(id);
                if (variable.getType() == Variable.VariableType.MAP_BOOLEAN || variable.getType() == Variable.VariableType.MAP_STRING
                        || variable.getType() == Variable.VariableType.MAP_INTEGER
                        || variable.getType() == Variable.VariableType.MAP_DOUBLE) {
                    if (variable.getAllowedKeys() != null && !variable.getAllowedKeys().isEmpty()) {
                        List values;
                        if (entry.getValue() instanceof Collection) {
                            values = ((List) entry.getValue());
                        } else {
                            values = Collections.singletonList(entry.getValue());
                        }
                        for (Object value : values) {
                            Map<String, Object> valueMap = getMapValue(value);
                            Set<String> allowedKeys = new HashSet<>(variable.getAllowedKeys());
                            for (String key : valueMap.keySet()) {
                                if (!allowedKeys.contains(key)) {
                                    throw new CatalogException("Annotation id '" + key + "' is not an accepted key in the variable '"
                                            + variable.getId() + "'");
                                }
                            }
                        }
                    }
                }
                annotations.put(id, getValue(variable.getType(), entry.getValue()));
                checkAllowedValue(variable, entry.getValue(), "Annotation");
            }
        }
    }

//    public static void checkAnnotation(Map<String, Variable> variableMap, Annotation annotation) throws CatalogException {
//        String id = annotation.getName();
//        if (!variableMap.containsKey(id)) {
//            throw new CatalogException("Annotation id '" + annotation + "' is not an accepted id");
//        } else {
//            Variable variable = variableMap.get(id);
//            annotation.setValue(getValue(variable.getType(), annotation.getValue()));
//            checkAllowedValue(variable, annotation.getValue(), "Annotation");
//        }
//
//    }

    /**
     * Adds the default annotation of a variable if present and returns true if it has been added to the annotation object, false otherwise.
     *
     * @param variable Variable from where the default annotation will be taken.
     * @param annotation Annotation map.
     * @return True if a default annotation value could be found, False otherwise.
     * @throws CatalogException if there is any other kind of error.
     */
    public static boolean addDefaultAnnotation(Variable variable, Map<String, Object> annotation) throws CatalogException {
        Object defaultValue = variable.getDefaultValue();
        switch (variable.getType()) {
            case BOOLEAN:
                Boolean booleanValue = getBooleanValue(defaultValue);
                if (booleanValue != null) {
                    annotation.put(variable.getId(), booleanValue);
                    return true;
                }
            case DOUBLE:
                Double numericValue = getNumericValue(defaultValue);
                if (numericValue != null) {
                    annotation.put(variable.getId(), numericValue);
                    return true;
                }
                break;
            case INTEGER:
                Integer integerValue = getIntegerValue(defaultValue);
                if (integerValue != null) {
                    annotation.put(variable.getId(), integerValue);
                    return true;
                }
                break;
            case CATEGORICAL:
            case STRING:
                String stringValue = getStringValue(defaultValue);
                if (stringValue != null) {
                    annotation.put(variable.getId(), stringValue);
                    return true;
                }
                break;
            case OBJECT:
            case MAP_BOOLEAN:
            case MAP_INTEGER:
            case MAP_DOUBLE:
            case MAP_STRING:
                if (variable.getDefaultValue() != null) {
                    annotation.put(variable.getId(), variable.getDefaultValue());
                    return true;
                }
                break;
            default:
                throw new CatalogException("Unknown VariableType " + variable.getType().name());
        }
        return false;
    }

    private static void checkAllowedValue(Variable variable, Object value, String message) throws CatalogException {

        List listValues;
        Object realValue = getValue(variable.getType(), value);
        if (realValue == null) {
            if (variable.isRequired()) {
                throw new CatalogException(message + " value '" + value + "' is a required value for " + variable);
            } else {
                return;
            }
        }
        if (realValue instanceof Collection) {
            listValues = new ArrayList((Collection) realValue);
            if (!variable.isMultiValue()) {
                throw new CatalogException(message + " value '" + value + "' does not accept multiple values for " + variable);
            }
        } else {
            listValues = Collections.singletonList(realValue);
        }

        if (listValues.isEmpty()) {
            // Check if variable has any children with required params
            if (variable.getVariableSet() != null) {
                for (Variable tmpVariable : variable.getVariableSet()) {
                    if (tmpVariable.isRequired()) {
                        throw new CatalogException("Missing required variable " + tmpVariable.getId());
                    }
                }
            }
        }

        switch (variable.getType()) {
            case BOOLEAN:
                break;
            case CATEGORICAL: {
                for (Object object : listValues) {
                    String stringValue = (String) object;
                    if (variable.getAllowedValues() != null && !variable.getAllowedValues().contains(stringValue)) {
                        throw new CatalogException(message + " value '" + value + "' is not an allowed value for " + variable);
                    }
                }
                break;
            }
            case INTEGER:
                for (Object object : listValues) {
                    int numericValue = (int) object;

                    if (variable.getAllowedValues() != null && !variable.getAllowedValues().isEmpty()) {
                        boolean valid = false;
                        for (String range : variable.getAllowedValues()) {
                            String[] split = range.split(":", -1);
                            int min = split[0].isEmpty() ? Integer.MIN_VALUE : Integer.valueOf(split[0]);
                            int max = split[1].isEmpty() ? Integer.MAX_VALUE : Integer.valueOf(split[1]);
                            if (numericValue >= min && numericValue <= max) {
                                valid = true;
                                break;
                            }
                        }
                        if (!valid) {
                            throw new CatalogException(message + " value '" + value + "' is not an allowed value for " + variable + ". It"
                                    + " is in any range.");
                        }
                    }
                    //If there is no "allowedValues", accept any number
                }
                break;
            case DOUBLE:
                for (Object object : listValues) {
                    Double numericValue = (Double) object;

                    if (variable.getAllowedValues() != null && !variable.getAllowedValues().isEmpty()) {
                        boolean valid = false;
                        for (String range : variable.getAllowedValues()) {
                            String[] split = range.split(":", -1);
                            Double min = split[0].isEmpty() ? Double.MIN_VALUE : Double.valueOf(split[0]);
                            Double max = split[1].isEmpty() ? Double.MAX_VALUE : Double.valueOf(split[1]);
                            if (numericValue >= min && numericValue <= max) {
                                valid = true;
                                break;
                            }
                        }
                        if (!valid) {
                            throw new CatalogException(message + " value '" + value + "' is not an allowed value for " + variable + ". It"
                                    + " is in any range.");
                        }
                    }
                    //If there is no "allowedValues", accept any number
                }
                break;
            case STRING: {
                //Check regex?
                return;
            }
            case OBJECT: {
                //Check variableSet
                for (Object object : listValues) {
                    if (variable.getVariableSet() != null && !variable.getVariableSet().isEmpty()) {
                        Map objectMap = (Map) object;
                        checkAnnotationSet(new VariableSet(variable.getId(), variable.getId(), false, false, variable.getDescription(),
                                variable.getVariableSet(), null, 1, null), new AnnotationSet("", variable.getId(), objectMap, null, 1,
                                null), null, true);
                    }
                }
                break;
            }
            case MAP_BOOLEAN:
                // Check types
                for (Object object : listValues) {
                    if (!(object instanceof Map)) {
                        throw new CatalogException("Expected a boolean map for variable " + variable.getId());
                    }
                    Map<String, Object> objectMap = (Map<String, Object>) object;
                    for (Map.Entry<String, Object> entry : objectMap.entrySet()) {
                        if (!(entry.getValue() instanceof Boolean)) {
                            throw new CatalogException(entry.getKey() + " does not seem to be boolean. Expected a boolean map for variable "
                                    + variable.getId());
                        }
                    }
                }
                break;
            case MAP_INTEGER:
                // Check types
                for (Object object : listValues) {
                    if (!(object instanceof Map)) {
                        throw new CatalogException("Expected an integer map for variable " + variable.getId());
                    }
                    Map<String, Object> objectMap = (Map<String, Object>) object;
                    for (Map.Entry<String, Object> entry : objectMap.entrySet()) {
                        if (!(entry.getValue() instanceof Integer)) {
                            throw new CatalogException(entry.getKey() + " does not seem to be integer. Expected an integer map for "
                                    + "variable " + variable.getId());
                        }
                    }
                }
                break;
            case MAP_DOUBLE:
                // Check types
                for (Object object : listValues) {
                    if (!(object instanceof Map)) {
                        throw new CatalogException("Expected a double map for variable " + variable.getId());
                    }
                    Map<String, Object> objectMap = (Map<String, Object>) object;
                    for (Map.Entry<String, Object> entry : objectMap.entrySet()) {
                        if (!(entry.getValue() instanceof Double)) {
                            throw new CatalogException(entry.getKey() + " does not seem to be double. Expected a double map for variable "
                                    + variable.getId());
                        }
                    }
                }
                break;
            case MAP_STRING:
                // Check types
                for (Object object : listValues) {
                    if (!(object instanceof Map)) {
                        throw new CatalogException("Expected a text map for variable " + variable.getId());
                    }
                    Map<String, Object> objectMap = (Map<String, Object>) object;
                    for (Map.Entry<String, Object> entry : objectMap.entrySet()) {
                        if (!(entry.getValue() instanceof String)) {
                            throw new CatalogException(entry.getKey() + " does not seem to be text. Expected a text map for variable "
                                    + variable.getId());
                        }
                    }
                }
                break;
            default:
                throw new CatalogException("Unknown VariableType " + variable.getType().name());
        }
    }

    private static Object getValue(Variable.VariableType variableType, Object value) throws CatalogException {
        Collection valueCollection;
        if (value instanceof Collection) {
            valueCollection = ((Collection) value);
            ArrayList<Object> list = new ArrayList<>(valueCollection.size());
            for (Object o : valueCollection) {
                list.add(getValue(variableType, o));
            }
            return list;
        }
        switch (variableType) {
            case BOOLEAN:
                return getBooleanValue(value);
            case STRING:
            case CATEGORICAL:
                return getStringValue(value);
            case DOUBLE:
                return getNumericValue(value);
            case INTEGER:
                return getIntegerValue(value);
            case OBJECT:
            case MAP_BOOLEAN:
            case MAP_INTEGER:
            case MAP_DOUBLE:
            case MAP_STRING:
                return getMapValue(value);
            default:
                throw new CatalogException("Unknown VariableType " + variableType.name());
        }
    }

    /**
     * Get StringValue. If empty or null, return null;
     *
     * @param value
     * @return
     */
    private static String getStringValue(Object value) {
        if (value == null) {
            return null;
        } else {
            String stringValue = value.toString();
            if (stringValue.isEmpty()) {
                return null;
            } else {
                return stringValue;
            }
        }
    }

    /**
     * Try to cast to Boolean. If not possible, return null;
     *
     * @param value
     * @return
     */
    private static Boolean getBooleanValue(Object value) throws CatalogException {
        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof String) {
            if (((String) value).equalsIgnoreCase("true")) {
                return true;
            } else if (((String) value).equalsIgnoreCase("false")) {
                return false;
            } else if (((String) value).isEmpty()) {
                return null;
            }
        }

        try {
            Double numericValue = getNumericValue(value);
            if (numericValue == null) {
                return null;    //Empty string
            } else {
                return numericValue != 0;
            }
        } catch (CatalogException e) {
            throw new CatalogException("Value " + value + " is not a valid Boolean", e);
        }
    }

    /**
     * Try to cast to Integer. If not possible, return null;
     *
     * @param value
     * @return
     */
    private static Integer getIntegerValue(Object value) throws CatalogException {
        Integer numericValue = null;
        if (value == null) {
            return null;
        } else if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof String) {
            if (((String) value).isEmpty()) {
                numericValue = null;    //Empty string
            } else {
                try {
                    numericValue = Integer.parseInt((String) value);
                } catch (NumberFormatException e) {
                    throw new CatalogException("Value " + value + " is not an integer number", e);
                }
            }
        } else if (value instanceof Boolean) {
            return (Boolean) value ? 1 : 0;
        }
        return numericValue;
    }

    /**
     * Try to cast to Double. If not possible, return null;
     *
     * @param value
     * @return
     */
    private static Double getNumericValue(Object value) throws CatalogException {
        Double numericValue = null;
        if (value == null) {
            return null;
        } else if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof String) {
            if (((String) value).isEmpty()) {
                numericValue = null;    //Empty string
            } else {
                try {
                    numericValue = Double.parseDouble((String) value);
                } catch (NumberFormatException e) {
                    throw new CatalogException("Value " + value + " is not a number", e);
                }
            }
        } else if (value instanceof Boolean) {
            return (double) ((Boolean) value ? 1 : 0);
        }
        return numericValue;
    }

    private static Map getMapValue(Object value) throws CatalogException {
        if (value == null) {
            return null;
        }
        if (value instanceof Map) {
            return ((Map) value);
        }
        try {
            return new ObjectMap(getDefaultObjectMapper().writeValueAsString(value));
        } catch (JsonProcessingException e) {
            throw new CatalogException(e);
        }
    }

    public static void mergeNewAnnotations(AnnotationSet annotationSet, Map<String, Object> newAnnotations) {
        annotationSet.getAnnotations().putAll(newAnnotations);
//        Map<String, Object> annotations = annotationSet.getAnnotations();
//
//        for (Map.Entry<String, Object> entry : newAnnotations.entrySet()) {
//            if (entry.getValue() != null) {
//                // Replace the annotation
//                annotations.put(entry.getKey(), entry.getValue());
//            } else {
//                //Remove the old value (if present)
//                annotations.remove(entry.getKey());
//            }
//        }
    }


    /**
     * OpenCGA supports several ways of defining include/exclude of annotation fields:
     *  - annotationSets.annotations.a.b.c where a.b.c would be variables that will be included/excluded.
     *    annotation.a.b.c would be a shortcut used for the same purpose defined above.
     *  - annotationSets.variableSetId.cancer where cancer would correspond with the variable set id to be included/excluded.
     *    variableSet.cancer would be the shortcut used for the same purpose defined above.
     *  - annotationSets.name.cancer1 where cancer1 would correspond with the annotationSetName to be included/excluded.
     *    annotationSet.cancer1 would be the shortcut used for the same purpose defined above.
     *
     * This method will modify all the possible full way annotation fields to the corresponding shortcuts.
     *
     * @param options QueryOptions object containing
     */
    public static void fixQueryOptionAnnotation(QueryOptions options) {
        List<String> includeList = options.getAsStringList(QueryOptions.INCLUDE, ",");
        List<String> excludeList = options.getAsStringList(QueryOptions.EXCLUDE, ",");

        options.putIfNotEmpty(QueryOptions.INCLUDE, fixQueryOptionAnnotation(includeList));
        options.putIfNotEmpty(QueryOptions.EXCLUDE, fixQueryOptionAnnotation(excludeList));
    }

    private static String fixQueryOptionAnnotation(List<String> projectionList) {
        if (projectionList == null || projectionList.isEmpty()) {
            return null;
        }

        List<String> returnedProjection = new ArrayList<>(projectionList.size());
        for (String projection : projectionList) {
            if (projection.startsWith(ANNOTATION_SETS_ANNOTATIONS + ".")) {
                returnedProjection.add(projection.replace(ANNOTATION_SETS_ANNOTATIONS + ".", Constants.ANNOTATION + "."));
            } else if (projection.startsWith(ANNOTATION_SETS_VARIABLE_SET_ID + ".")) {
                returnedProjection.add(projection.replace(ANNOTATION_SETS_VARIABLE_SET_ID + ".", Constants.VARIABLE_SET + "."));
            } else if (projection.startsWith(ANNOTATION_SETS_ID + ".")) {
                returnedProjection.add(projection.replace(ANNOTATION_SETS_ID + ".", Constants.ANNOTATION_SET_NAME + "."));
            } else {
                returnedProjection.add(projection);
            }
        }

        return StringUtils.join(returnedProjection, ",");
    }

    /**
     * Fixes any field that might be missing from the annotation built by the user so it is perfectly ready for the dbAdaptors to be parsed.
     *
     * @param study study corresponding to the entry that is being queried. Study should contain the variableSets field filled in.
     * @param query query object containing the annotation.
     * @throws CatalogException if there are unknown variables being queried, non-existing variable sets...
     */
    public static void fixQueryAnnotationSearch(Study study, Query query) throws CatalogException {
        fixQueryAnnotationSearch(study, null, query, null);
    }

    /**
     * Fixes any field that might be missing from the annotation built by the user so it is perfectly ready for the dbAdaptors to be parsed.
     *
     * @param study study corresponding to the entry that is being queried. Study should contain the variableSets field filled in.
     * @param user for which the confidential permission should be checked.
     * @param query query object containing the annotation.
     * @param authorizationManager Authorization manager to check for confidential permissions. If null, permissions won't be checked.
     * @throws CatalogException if there are unknown variables being queried, non-existing variable sets...
     */
    public static void fixQueryAnnotationSearch(Study study, String user, Query query, AuthorizationManager authorizationManager)
            throws CatalogException {
        if (query == null || query.isEmpty() || !query.containsKey(Constants.ANNOTATION)) {
            return;
        }
        if (study.getVariableSets() == null) {
            throw new CatalogException("Internal error. VariableSets should be provided in the study parameter.");
        }

        List<String> originalAnnotationList = query.getAsStringList(Constants.ANNOTATION, ";");
        Map<String, VariableSet> variableSetMap = null;
        Map<String, Map<String, QueryParam.Type>> variableTypeMap = new HashMap<>();

        List<String> annotationList = new ArrayList<>(originalAnnotationList.size());
        ObjectMap queriedVariableTypeMap = new ObjectMap();

        boolean confidentialPermissionChecked = false;

        for (String annotation : originalAnnotationList) {
            if (variableSetMap == null) {
                variableSetMap = getVariableSetMap(study);
                for (VariableSet variableSet : variableSetMap.values()) {
                    variableTypeMap.put(variableSet.getId(), getVariableMap(variableSet));
                }
            }

            if (annotation.startsWith(Constants.ANNOTATION_SET_NAME)) {
                annotationList.add(annotation);
                continue;
            }

            // Split the annotation by key - value
            Matcher matcher = ANNOTATION_PATTERN.matcher(annotation);
            if (matcher.find()) {

                if (annotation.startsWith(Constants.VARIABLE_SET)) {
                    String variableSetString = matcher.group(3);
                    // Obtain the operator to take it out and get only the actual value
                    String operator = getOperator(variableSetString);
                    variableSetString = variableSetString.replace(operator, "");

                    VariableSet variableSet = variableSetMap.get(variableSetString);
                    if (variableSet == null) {
                        throw new CatalogException("Variable set " + variableSetString + " not found");
                    }

                    if (authorizationManager != null && !confidentialPermissionChecked && variableSet.isConfidential()) {
                        // We only check the confidential permission if needed once
                        authorizationManager.checkStudyPermission(study.getUid(), user,
                                StudyAclEntry.StudyPermissions.CONFIDENTIAL_VARIABLE_SET_ACCESS);
                        confidentialPermissionChecked = true;
                    }
                    annotationList.add(Constants.VARIABLE_SET + operator + variableSet.getUid());
                    continue;
                }

                String variableSetString = matcher.group(1);
                String key = matcher.group(2);
                String valueString = matcher.group(3);

                if (StringUtils.isEmpty(variableSetString)) {
                    // Obtain the variable set for the annotations
                    variableSetString = searchVariableSetForVariable(variableTypeMap, key);
                } else {
                    VariableSet variableSet = variableSetMap.get(variableSetString.replace(":", ""));
                    if (variableSet == null) {
                        throw new CatalogException("Variable set " + variableSetString + " not found");
                    }

                    // Remove the : at the end of the variableSet and convert the id into the uid
                    //variableSetString = String.valueOf(variableSet.getUid());
                    variableSetString = variableSet.getId();

                    // Check if the variable set and the variable exist
                    if (!variableTypeMap.containsKey(variableSetString)) {
                        throw new CatalogException("The variable " + variableSetString + " does not exist in the study " + study.getFqn());
                    }
                    if (!variableTypeMap.get(variableSetString).containsKey(key)) {
                        throw new CatalogException("Variable " + key + " from variableSet " + variableSetString + " does not exist. Cannot "
                                + "perform query " + annotation);
                    }
                }

                if (authorizationManager != null  && !confidentialPermissionChecked && variableSetMap.get(variableSetString)
                        .isConfidential()) {
                    // We only check the confidential permission if needed once
                    authorizationManager.checkStudyPermission(study.getUid(), user,
                            StudyAclEntry.StudyPermissions.CONFIDENTIAL_VARIABLE_SET_ACCESS);
                    confidentialPermissionChecked = true;
                }

                annotationList.add(variableSetString + ":" + key + valueString);
                queriedVariableTypeMap.put(variableSetString + ":" + key, variableTypeMap.get(variableSetString).get(key));
                queriedVariableTypeMap.put(variableSetString, variableSetMap.get(variableSetString).getUid());
            } else {
                throw new CatalogException("Annotation format from " + annotation + " not accepted. Supported format contains "
                        + "[variableSet:]variable=value");
            }
        }

        if (!annotationList.isEmpty()) {
            query.put(Constants.ANNOTATION, StringUtils.join(annotationList, ";"));
        }
        if (!queriedVariableTypeMap.isEmpty()) {
            query.put(Constants.PRIVATE_ANNOTATION_PARAM_TYPES, queriedVariableTypeMap);
        }
    }

    private static String getOperator(String queryValue) {
        Matcher matcher = OPERATION_PATTERN.matcher(queryValue);
        if (matcher.find()) {
            return matcher.group(2);
        }
        return null;
    }

    private static String searchVariableSetForVariable(Map<String, Map<String, QueryParam.Type>> variableTypeMap, String variableKey)
            throws CatalogException {
        String variableId = null;
        for (Map.Entry<String, Map<String, QueryParam.Type>> variableTypeMapEntry : variableTypeMap.entrySet()) {
            Map<String, QueryParam.Type> variableMap = variableTypeMapEntry.getValue();
            if (variableMap.containsKey(variableKey)) {
                if (variableId == null) {
                    variableId = variableTypeMapEntry.getKey();
                } else {
                    throw new CatalogException("Found more than one Variable Set for the variable " + variableKey);
                }
            }

            Map<String, QueryParam.Type> dynamicTypes = new HashMap<>();
            for (String key : variableMap.keySet()) {
                if (key.endsWith(".*")) {
                    // It is a dynamic map
                    if (variableKey.contains(key.substring(0, key.length() - 1))) {
                        // We update the variable map so it associated the current dynamic query with the expected type
                        dynamicTypes.put(variableKey, variableMap.get(key));

                        if (variableId == null) {
                            variableId = variableTypeMapEntry.getKey();
                        } else {
                            throw new CatalogException("Found more than one Variable Set for the variable " + variableKey);
                        }
                    }
                }
            }
            // We add any dynamic types we might have found
            variableMap.putAll(dynamicTypes);
        }

        if (variableId == null) {
            throw new CatalogException("Cannot find a Variable Set to match the variable " + variableKey);
        }

        return variableId;
    }

    public static Map<String, VariableSet> getVariableSetMap(Study study) {
        Map<String, VariableSet> variableSetMap = new HashMap<>();
        List<VariableSet> variableSets = study.getVariableSets();
        if (variableSets != null) {
            for (VariableSet variableSet : variableSets) {
                variableSetMap.put(variableSet.getId(), variableSet);
            }
        }

        return variableSetMap;
    }

    public static Map<String, QueryParam.Type> getVariableMap(VariableSet variableSet) throws CatalogDBException {
        Map<String, QueryParam.Type> variableTypeMap = new HashMap<>();

        Queue<VariableDepthMap> queue = new LinkedList<>();

        // We first insert all the variables
        for (Variable variable : variableSet.getVariables()) {
            queue.add(new VariableDepthMap(variable, Collections.emptyList()));
        }

        // We iterate while there are elements in the queue
        while (!queue.isEmpty()) {
            VariableDepthMap variableDepthMap = queue.remove();
            Variable variable = variableDepthMap.getVariable();

            if (variable.getType() == Variable.VariableType.OBJECT) {
                if (variable.getVariableSet() != null) {
                    // We add the new nested variables to the queue
                    for (Variable nestedVariable : variable.getVariableSet()) {
                        List<String> keys = new ArrayList<>(variableDepthMap.getKeys());
                        keys.add(variable.getId());
                        if (variable.isMultiValue()) {
                            // If the parent is multivalue, we will change the multivalue field from the nested variables to indicate
                            // that we will have an array of whichever type internally stored
                            nestedVariable.setMultiValue(variable.isMultiValue());
                        }
                        queue.add(new VariableDepthMap(nestedVariable, keys));
                    }
                }
            } else {
                QueryParam.Type type;
                switch (variable.getType()) {
                    case BOOLEAN:
                    case MAP_BOOLEAN:
                        if (variable.isMultiValue()) {
                            type = QueryParam.Type.BOOLEAN_ARRAY;
                        } else {
                            type = QueryParam.Type.BOOLEAN;
                        }
                        break;
                    case CATEGORICAL:
                    case STRING:
                    case MAP_STRING:
                        if (variable.isMultiValue()) {
                            type = QueryParam.Type.TEXT_ARRAY;
                        } else {
                            type = QueryParam.Type.TEXT;
                        }
                        break;
                    case INTEGER:
                    case MAP_INTEGER:
                        if (variable.isMultiValue()) {
                            type = QueryParam.Type.INTEGER_ARRAY;
                        } else {
                            type = QueryParam.Type.INTEGER;
                        }
                        break;
                    case DOUBLE:
                    case MAP_DOUBLE:
                        if (variable.isMultiValue()) {
                            type = QueryParam.Type.DECIMAL_ARRAY;
                        } else {
                            type = QueryParam.Type.DECIMAL;
                        }
                        break;
                    case OBJECT:
                    default:
                        throw new CatalogDBException("Unexpected variable type detected: " + variable.getType());
                }
                List<String> keys = new ArrayList<>(variableDepthMap.getKeys());
                keys.add(variable.getId());
                if (variable.getType() == Variable.VariableType.MAP_BOOLEAN
                        || variable.getType() == Variable.VariableType.MAP_INTEGER
                        || variable.getType() == Variable.VariableType.MAP_DOUBLE
                        || variable.getType() == Variable.VariableType.MAP_STRING) {
                    keys.add("*");
                }
                variableTypeMap.put(StringUtils.join(keys, "."), type);
            }
        }

        return variableTypeMap;
    }

    private static class VariableDepthMap {
        private Variable variable;
        private List<String> keys;

        VariableDepthMap(Variable variable, List<String> keys) {
            this.variable = variable;
            this.keys = keys;
        }

        public Variable getVariable() {
            return variable;
        }

        public List<String> getKeys() {
            return keys;
        }
    }
}
