package org.opencb.opencga.catalog;

import org.opencb.opencga.catalog.beans.Annotation;
import org.opencb.opencga.catalog.beans.AnnotationSet;
import org.opencb.opencga.catalog.beans.Variable;
import org.opencb.opencga.catalog.beans.VariableSet;

import java.util.*;

/**
 * Created by jacobo on 14/12/14.
 */
public class CatalogSampleAnnotationsValidator {

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

        if(variable.getType() == null) {
            throw new CatalogException("VariableType is null");
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
            case NUMERIC: {
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
                        } catch (NumberFormatException e ) {
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
     * @param variableSet           VariableSet that describes the annotationSet.
     * @param annotationSet         AnnotationSet to check
     * @param annotationSets        All the AnnotationSets of the sample
     * @throws CatalogException
     */
    public static void checkAnnotationSet(VariableSet variableSet, AnnotationSet annotationSet, List<AnnotationSet> annotationSets) throws CatalogException {
        if(variableSet.getId() != annotationSet.getVariableSetId()) {
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

        //Get annotationSetId set and variableId map
        Set<String> annotatedVariables = new HashSet<>();
        Map<String, Variable> variableMap = new HashMap<>();
        for (Variable variable : variableSet.getVariables()) {
            variableMap.put(variable.getId(), variable);
        }

        //Check Duplicated
        for (Annotation annotation : annotationSet.getAnnotations()) {
            if (annotatedVariables.contains(annotation.getId())) {
                throw new CatalogException("Duplicated annotation " + annotation);
            }
            annotatedVariables.add(annotation.getId());
        }

        //Check for missing values
        List<Annotation> defaultAnnotations = new LinkedList<>();
        for (Variable variable : variableSet.getVariables()) {
            if(!annotatedVariables.contains(variable.getId())) {
                Annotation defaultAnnotation = getDefaultAnnotation(variable);
                if (defaultAnnotation == null) {
                    if (variable.isRequired()) {
                        throw new CatalogException("Variable " + variable + " is required.");
                    }
                } else {
                    defaultAnnotations.add(defaultAnnotation);
                    annotatedVariables.add(defaultAnnotation.getId());
                }
            }
        }
        annotationSet.getAnnotations().addAll(defaultAnnotations);

        //Check annotations
        for (Annotation annotation : annotationSet.getAnnotations()) {
            checkAnnotation(variableMap, annotation);
        }


    }

    public static void checkAnnotation(Map<String, Variable> variableMap, Annotation annotation) throws CatalogException {
        String id = annotation.getId();
        if(!variableMap.containsKey(id)) {
            throw new CatalogException("Annotation id '" + annotation + "' is not an accepted id");
        } else {
            Variable variable = variableMap.get(id);
            annotation.setValue(getValue(variable.getType(), annotation.getValue()));
            checkAllowedValue(variable, annotation.getValue(), "Annotation");
        }

    }

    public static Annotation getDefaultAnnotation(Variable variable) throws CatalogException {
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
            default:
                throw new CatalogException("Unknown VariableType " + variable.getType().name());
        }
    }

    private static void checkAllowedValue(Variable variable, Object value, String message) throws CatalogException {

        Object realValue = getValue(variable.getType(), value);
        if (realValue == null) {
            if (variable.isRequired()) {
                throw new CatalogException(message + " value '" + value + "' is a required value for " + variable);
            } else {
                return;
            }
        }

        switch (variable.getType()) {
            case BOOLEAN:
                return;
            case CATEGORICAL: {
                String stringValue = (String)realValue;
                if(variable.getAllowedValues().contains(stringValue)) {
                    return;
                } else {
                    throw new CatalogException(message + " value '" + value + "' is not an allowed value for " + variable);
                }
            }
            case NUMERIC:
                Double numericValue = (Double)realValue;

                if (!variable.getAllowedValues().isEmpty()) {
                    for (String range : variable.getAllowedValues()) {
                        String[] split = range.split(":", -1);
                        Double min = split[0].isEmpty() ? Double.MIN_VALUE : Double.valueOf(split[0]);
                        Double max = split[1].isEmpty() ? Double.MAX_VALUE : Double.valueOf(split[1]);
                        if (numericValue >= min && numericValue <= max) {
                            return;
                        }
                    }
                    throw new CatalogException(message + " value '" + value + "' is not an allowed value for " + variable + ". It is in any range.");
                } else {
                    return;    //If there is no "allowedValues", any number
                }

            case TEXT: {
                //Check regex?
                return;
            }
            default:
                throw new CatalogException("Unknown VariableType " + variable.getType().name());
        }
    }

    private static Object getValue(Variable.VariableType variableType, Object value) throws CatalogException {
        switch(variableType) {
            case BOOLEAN:
                return getBooleanValue(value);
            case TEXT:
            case CATEGORICAL:
                return getStringValue(value);
            case NUMERIC:
                return getNumericValue(value);
            default:
                throw new CatalogException("Unknown VariableType " + variableType.name());
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
     * Try to cast to Boolean. If not possible, return null;
     * @param value
     * @return
     */
    private static Boolean getBooleanValue (Object value) throws CatalogException {
        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof String) {
            if(((String) value).equalsIgnoreCase("true")) {
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
     * Try to cast to Double. If not possible, return null;
     * @param value
     * @return
     */
    private static Double getNumericValue(Object value) throws CatalogException {
        Double numericValue = null;
        if(value == null) {
            return null;
        } else if( value instanceof Double) {
            return (Double) value;
        } else if(value instanceof Integer) {
            return ((Integer) value).doubleValue();
        } else if(value instanceof Short) {
            return ((Short) value).doubleValue();
        } else if(value instanceof Long) {
            return ((Long) value).doubleValue();
        } else if(value instanceof Float) {
            return ((Float) value).doubleValue();
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

}
