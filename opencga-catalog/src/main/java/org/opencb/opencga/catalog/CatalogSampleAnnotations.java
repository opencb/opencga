package org.opencb.opencga.catalog;

import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.beans.Annotation;
import org.opencb.opencga.catalog.beans.AnnotationSet;
import org.opencb.opencga.catalog.beans.Variable;
import org.opencb.opencga.catalog.beans.VariableSet;

import java.util.*;

/**
 * Created by jacobo on 14/12/14.
 */
public class CatalogSampleAnnotations {

    public static void checkVariableSet(VariableSet variableSet) throws CatalogException {
        Set<String> variableIdSet = new HashSet<>();
        for (Variable variable : variableSet.getVariables()) {
            if(variableIdSet.contains(variable.getId())) {
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
        if (variable.getAcceptedValues() != null) {
            for (String acceptedValue : variable.getAcceptedValues()) {
                Collections.addAll(acceptedValues, acceptedValue.split(","));
            }
        }
        variable.setAcceptedValues(acceptedValues);

        switch (variable.getType()) {
            case BOOLEAN: {
                if (!variable.getAcceptedValues().isEmpty()) {
                    throw new CatalogException("Variable type boolean can not contain accepted values");
                }
                variable.setDefaultValue(getBooleanValue(variable.getDefaultValue()));
                break;
            }
            case CATEGORICAL: {
                //Check accepted values


                //Check default value
                String stringValue = getStringValue(variable.getDefaultValue());
                variable.setDefaultValue(stringValue);
                break;
            }
            case NUMERIC: {
                //Check accepted values
                if (!variable.getAcceptedValues().isEmpty()) {
                    for (String range : variable.getAcceptedValues()) {
                        String[] split = range.split(":", -1);
                        if (split.length != 2) {
                            throw new CatalogException("Invalid numerical range. Expected <number>:<number>");
                        }
                        if (Double.valueOf(split[0]) > Double.valueOf(split[1])) {
                            throw new CatalogException("Invalid numerical range. Expected <number1>:<number2> where number1 <= number2");
                        }
                    }
                }
                //Check default value
                Double numericValue = getNumericValue(variable.getDefaultValue());
                variable.setDefaultValue(numericValue);
                break;
            }
            case TEXT: {
                //Check default value
                String stringValue = getStringValue(variable.getDefaultValue());
                variable.setDefaultValue(stringValue);

                break;
            }
        }

        //Check default value
        if (variable.getDefaultValue() != null) {
            if (isAcceptedValue(variable, variable.getDefaultValue())) {
                throw new CatalogException("Default value '" + variable.getDefaultValue() + "' is not an accepted value");
            }
        }
    }

    public static void checkAnnotationSet(VariableSet variableSet, AnnotationSet annotationSet) throws CatalogException {
        if(variableSet.getId() != annotationSet.getVariableSetId()) {
            throw new CatalogException("VariableSet does not match with the AnnotationSet");
        }

        //Check annotations
        Set<String> annotatedVariables = new HashSet<>();
        Map<String, Variable> variableMap = new HashMap<>();

        for (Variable variable : variableSet.getVariables()) {
            variableMap.put(variable.getId(), variable);
        }
        for (Annotation annotation : annotationSet.getAnnotations()) {
            if (annotatedVariables.contains(annotation.getId())) {
                throw new CatalogException("Duplicated annotation " + annotation);
            }
            annotatedVariables.add(annotation.getId());
            checkAnnotation(variableMap, annotation);
        }

        List<Annotation> defaultAnnotations = new LinkedList<>();

        //Check for missing values
        for (Variable variable : variableSet.getVariables()) {
            if(variable.isRequired() && !annotatedVariables.contains(variable.getId())) {
                Annotation defaultAnnotation = getDefaultAnnotation(variable);
                if(defaultAnnotation == null) {
                    throw new CatalogException("Variable " + variable + " is required.");
                } else {
                    defaultAnnotations.add(defaultAnnotation);
                }
            }
        }
        annotationSet.getAnnotations().addAll(defaultAnnotations);
    }

    public static void checkAnnotation(Map<String, Variable> variableMap, Annotation annotation) throws CatalogException {
        String id = annotation.getId();
        if(!variableMap.containsKey(id)) {
            throw new CatalogException("Annotation id '" + annotation + "' is not an accepted id");
        } else {
            Variable variable = variableMap.get(id);
            if (!isAcceptedValue(variable, annotation.getValue())) {
                throw new CatalogException("Annotation value '" + annotation + "' is not an accepted value");
            }
        }

    }

    public static Annotation getDefaultAnnotation(Variable variable) {
        Object defaultValue = variable.getDefaultValue();
        switch (variable.getType()) {
            case BOOLEAN: {
                Boolean booleanValue = getBooleanValue(defaultValue);
                return booleanValue == null ? null : new Annotation(variable.getId(), booleanValue);
            }
            case NUMERIC: {
                Double numericValue = getNumericValue(defaultValue);
                return numericValue == null ? null : new Annotation(variable.getId(), numericValue);
            }
            case CATEGORICAL:
            case TEXT: {
                String stringValue = getStringValue(defaultValue);
                return stringValue == null ? null : new Annotation(variable.getId(), stringValue);
            }
        }
        return null;
    }

    private static boolean isAcceptedValue(Variable variable, Object value) {
        switch (variable.getType()) {
            case BOOLEAN:
                Boolean booleanValue = getBooleanValue(value);
                if (booleanValue == null) {
                    return !variable.isRequired();
                } else {
                    return true;
                }
            case CATEGORICAL:
                String stringValue = getStringValue(value);
                if (stringValue == null) {
                    return !variable.isRequired();
                } else {
                    return variable.getAcceptedValues().contains(stringValue);
                }
            case NUMERIC:
                Double numericValue = getNumericValue(value);
                if(numericValue == null) {
                    return !variable.isRequired();
                } else {
                    if (!variable.getAcceptedValues().isEmpty()) {
                        for (String range : variable.getAcceptedValues()) {
                            String[] split = range.split(":", -1);
                            Double min = split[0].isEmpty() ? Double.MIN_VALUE : Double.valueOf(split[0]);
                            Double max = split[1].isEmpty() ? Double.MAX_VALUE : Double.valueOf(split[1]);
                            if (numericValue >= min && numericValue <= max) {
                                return true;
                            }
                        }
                        return false;   //Was in any range. Invalid value
                    } else {
                        return true;    //If there is no "acceptedValues", any number
                    }
                }
            case TEXT:
                //Check regex?

                return !variable.isRequired();
            default:
                return false;
        }
    }

    /**
     * Get StringValue. If empty or null, return null;
     * @param value
     * @return
     */
    private static String getStringValue (Object value) {
        if (value == null ) {
            return null;
        } else {
            String stringValue = value.toString();
            if(stringValue.isEmpty()) {
                return null;
            } else {
                return stringValue;
            }
        }
    }

    /**
     * Try to cast to Boolean. If imposible, return null;
     * @param value
     * @return
     */
    private static Boolean getBooleanValue (Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return ((Boolean) value);
        } else {
            Double numericValue = getNumericValue(value);
            if (numericValue == null) {
                String string = getStringValue(value);
                if (!string.isEmpty()) {
                    return Boolean.valueOf(string);
                } else {
                    return null;    //Empty string
                }
            } else {
                return numericValue != 0;
            }
        }
    }

    /**
     * Try to cast to Double. If imposible, return null;
     * @param value
     * @return
     */
    private static Double getNumericValue(Object value) {
        Double numericValue = null;
        if(value == null) {
            return null;
        } else if(value instanceof Integer) {
            return ((Integer) value).doubleValue();
        } else if(value instanceof Short) {
            return ((Short) value).doubleValue();
        } else if(value instanceof Long) {
            return ((Long) value).doubleValue();
        } else if(value instanceof Float) {
            return ((Float) value).doubleValue();
        } else if( value instanceof Double) {
            return (Double) value;
        } else if (value instanceof String) {
            if (((String) value).isEmpty()) {
                numericValue = null;    //Empty string
            } else {
                try {
                    numericValue = Double.parseDouble((String) value);
                } catch (NumberFormatException e) {
                    return null;    //Not a number. //TODO: Throw Error?
                }
            }
        } else if (value instanceof Boolean) {
            return (double) ((Boolean) value ? 1 : 0);
        }
        return numericValue;
    }

}
