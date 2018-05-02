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

package org.opencb.opencga.catalog.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.AnnotationSet;
import org.opencb.opencga.core.models.Variable;
import org.opencb.opencga.core.models.VariableSet;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by jacobo on 14/12/14.
 */
public class CatalogAnnotationsValidator {

    public static void checkVariableSet(VariableSet variableSet) throws CatalogException {
        Set<String> variableIdSet = new HashSet<>();
        for (Variable variable : variableSet.getVariables()) {
            if (variableIdSet.contains(variable.getName())) {
                throw new CatalogException("Duplicated variable Id");
            }
            variableIdSet.add(variable.getName());
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
        if (variable.getVariableSet() != null && variable.getType() != Variable.VariableType.OBJECT) {
            throw new CatalogException("Only variables with type \"OBJECT\" can define an internal variableSet");
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
            case TEXT:
                break;
            case OBJECT:
                if (variable.getVariableSet() != null) {
                    for (Variable v : variable.getVariableSet()) {
                        checkVariable(v);
                    }
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
     * Check if an annotationSet is valid.
     *
     * @param variableSet    VariableSet that describes the annotationSet.
     * @param annotationSet  AnnotationSet to check
     * @param annotationSets All the AnnotationSets of the sample
     * @throws CatalogException CatalogException
     */
    public static void checkAnnotationSet(VariableSet variableSet, AnnotationSet annotationSet,
                                          @Nullable List<AnnotationSet> annotationSets) throws CatalogException {
        if (variableSet.getId() != annotationSet.getVariableSetId()) {
            throw new CatalogException("VariableSet does not match with the AnnotationSet");
        }

        //Check unique variableSet.
        if (variableSet.isUnique() && annotationSets != null) {
            for (AnnotationSet set : annotationSets) {
                if (set.getVariableSetId() == annotationSet.getVariableSetId()) {
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
            variableMap.put(variable.getName(), variable);
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
            if (!annotatedVariables.contains(variable.getName())) {
                if (!addDefaultAnnotation(variable, defaultAnnotations)) {
                    if (variable.isRequired()) {
                        throw new CatalogException("Missing required variable " + variable.getName());
                    }
                }
                annotatedVariables.add(variable.getName());
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
                    annotation.put(variable.getName(), booleanValue);
                    return true;
                }
            case DOUBLE:
                Double numericValue = getNumericValue(defaultValue);
                if (numericValue != null) {
                    annotation.put(variable.getName(), numericValue);
                    return true;
                }
                break;
            case INTEGER:
                Integer integerValue = getIntegerValue(defaultValue);
                if (integerValue != null) {
                    annotation.put(variable.getName(), integerValue);
                    return true;
                }
                break;
            case CATEGORICAL:
            case TEXT:
                String stringValue = getStringValue(defaultValue);
                if (stringValue != null) {
                    annotation.put(variable.getName(), stringValue);
                    return true;
                }
                break;
            case OBJECT:
                if (variable.getDefaultValue() != null) {
                    annotation.put(variable.getName(), variable.getDefaultValue());
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
            case TEXT: {
                //Check regex?
                return;
            }
            case OBJECT: {
                //Check variableSet
                for (Object object : listValues) {
                    if (variable.getVariableSet() != null && !variable.getVariableSet().isEmpty()) {
                        Map objectMap = (Map) object;
                        checkAnnotationSet(new VariableSet(0, variable.getName(), false, false, variable.getDescription(),
                                variable.getVariableSet(), 1, null), new AnnotationSet("", 0, objectMap, null, 1, null), null);
                    }
                }
                break;
            }
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
            case TEXT:
            case CATEGORICAL:
                return getStringValue(value);
            case DOUBLE:
                return getNumericValue(value);
            case INTEGER:
                return getIntegerValue(value);
            case OBJECT:
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
            return new ObjectMap(new ObjectMapper().writeValueAsString(value));
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
}
