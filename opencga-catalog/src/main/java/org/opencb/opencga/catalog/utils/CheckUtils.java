package org.opencb.opencga.catalog.utils;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CheckUtils {

    public static final Pattern MAIN_PATTERN = Pattern.compile("^(\\w+)\\((.+)\\)$");
    public static final Pattern THREE_FIELDS = Pattern.compile("^\\w+\\((.+),\\s*(\\S+),\\s*(\\S+)\\)$");
    public static final Pattern TWO_FIELDS = Pattern.compile("^\\w+\\((.*),\\s*(\\S+)\\)$");

    public enum CheckMethods {
        COMPARE_STRING(3, new HashSet<>(Arrays.asList("=", "==", "!="))),
        COMPARE_BOOLEAN(3, new HashSet<>(Arrays.asList("=", "==", "!="))),
        COMPARE_NUMBER(3, new HashSet<>(Arrays.asList("=", "==", "!=", "<", "<=", ">", ">="))),
        COMPARE_LIST_SIZE(3, new HashSet<>(Arrays.asList("=", "==", "!=", "<", "<=", ">", ">="))),
        IS_EMPTY_OBJECT(2, new HashSet<>(Arrays.asList("true", "false"))),
        EXISTS(2, new HashSet<>(Arrays.asList("true", "false")));

        private final int fields;
        private final Set<String> operators;

        CheckMethods(int fields, Set<String> operators) {
            this.fields = fields;
            this.operators = operators;
        }

        public int getFields() {
            return fields;
        }

        public Set<String> getOperators() {
            return operators;
        }
    }

    public static boolean check(String sentence) throws CatalogException {
        ParamUtils.checkParameter(sentence, "CompareString");

        // Validate main pattern
        Matcher matcher = MAIN_PATTERN.matcher(sentence);
        if (!matcher.find()) {
            throw new CatalogParameterException("Unknown format: '" + sentence + "'. Expected format is: \"FUNCTION(x, y[, z])\"");
        }
        String method = matcher.group(1);
        CheckMethods checkMethods = parseMethod(method);

        // Use necessary pattern
        int fields = checkMethods.getFields();
        Pattern pattern;
        if (fields == 2) {
            pattern = TWO_FIELDS;
        } else if (fields == 3) {
            pattern = THREE_FIELDS;
        } else {
            throw new CatalogParameterException("Internal error. Unsupported number of fields: " + fields);
        }

        matcher = pattern.matcher(sentence);
        if (!matcher.find()) {
            throw new CatalogParameterException("Unknown format: '" + sentence + "'. Expected format is: \"FUNCTION(x, y[, z])\"");
        }
        // Validate operators
        String operator = matcher.group(fields);
        if (!checkMethods.getOperators().contains(operator)) {
            throw new CatalogParameterException("Unknown operator '" + operator + "'. Supported operators are: "
                    + String.join(", ", checkMethods.getOperators()));
        }

        switch (checkMethods) {
            case COMPARE_STRING:
                return compareString(matcher.group(1), matcher.group(2), operator);
            case COMPARE_BOOLEAN:
                return compareBoolean(matcher.group(1), matcher.group(2), operator);
            case COMPARE_NUMBER:
                return compareNumber(matcher.group(1), matcher.group(2), operator);
            case COMPARE_LIST_SIZE:
                return compareListSize(matcher.group(1), matcher.group(2), operator);
            case IS_EMPTY_OBJECT:
                return isEmptyObject(matcher.group(1), operator);
            case EXISTS:
                return exists(matcher.group(1), operator);
            default:
                throw new CatalogException("Internal error. Found " + checkMethods + " method");
        }
    }

    private static boolean exists(String valueStr, String operator) throws CatalogParameterException {
        boolean exists = false;
        if (!"null".equalsIgnoreCase(valueStr)) {
            exists = !StringUtils.isEmpty(valueStr);
        }
        boolean comparator = parseBoolean(operator);

        return (exists && comparator) || (!exists && !comparator);
    }

    private static boolean isEmptyObject(String valueStr, String operator) throws CatalogParameterException {
        if (!valueStr.startsWith("{") || !valueStr.endsWith("}")) {
            throw new CatalogParameterException("Cannot cast '" + valueStr + "' to a map.");
        }
        String value = valueStr.replace("{", "").replace("}", "");
        boolean isEmpty = StringUtils.isEmpty(value);
        boolean comparator = parseBoolean(operator);

        return (isEmpty && comparator) || (!isEmpty && !comparator);
    }

    private static boolean compareListSize(String valueStr, String expectedValueStr, String operator) throws CatalogParameterException {
        if (!valueStr.startsWith("[") || !valueStr.endsWith("]")) {
            throw new CatalogParameterException("Cannot cast '" + valueStr + "' to a list.");
        }
        List<String> myList = Arrays.asList(valueStr.replace("[", "").replace("]", "").split(","));
        return compareNumber(String.valueOf(myList.size()), expectedValueStr, operator);
    }

    private static boolean compareNumber(String valueStr, String expectedValueStr, String operator) throws CatalogParameterException {
        long value;
        long expectedValue;
        try {
            value = NumberFormat.getInstance().parse(valueStr).longValue();
        } catch (NumberFormatException | ParseException e) {
            throw new CatalogParameterException("Cannot cast '" + valueStr + "' to number", e);
        }
        try {
             expectedValue = NumberFormat.getInstance().parse(expectedValueStr).longValue();
        } catch (NumberFormatException | ParseException e) {
            throw new CatalogParameterException("Cannot cast '" + expectedValueStr + "' to number", e);
        }

        switch (operator) {
            case "=":
            case "==":
                return value == expectedValue;
            case "!=":
                return value != expectedValue;
            case ">":
                return value > expectedValue;
            case ">=":
                return value >= expectedValue;
            case "<":
                return value < expectedValue;
            case "<=":
                return value <= expectedValue;
            default:
                throw new CatalogParameterException("Unexpected operator '" + operator + "'");
        }
    }

    private static boolean compareBoolean(String valueStr, String expectedValueStr, String operator) throws CatalogParameterException {
        boolean value = parseBoolean(valueStr);
        boolean expectedValue = parseBoolean(expectedValueStr);
        switch (operator) {
            case "=":
            case "==":
                return value == expectedValue;
            case "!=":
                return value != expectedValue;
            default:
                throw new CatalogParameterException("Unexpected operator '" + operator + "'");
        }
    }

    private static boolean parseBoolean(String value) throws CatalogParameterException {
        if ("true".equalsIgnoreCase(value)) {
            return true;
        } else if ("false".equalsIgnoreCase(value)) {
            return false;
        } else {
            throw new CatalogParameterException("Cannot cast '" + value + "' to boolean");
        }
    }

    private static boolean compareString(String value, String expectedValue, String operator) throws CatalogParameterException {
        switch (operator) {
            case "=":
            case "==":
                return value.equals(expectedValue);
            case "!=":
                return !value.equals(expectedValue);
            default:
                throw new CatalogParameterException("Unexpected operator '" + operator + "'");
        }
    }

    private static CheckMethods parseMethod(String methodStr) throws CatalogParameterException {
        CheckMethods method;
        try {
            method = CheckMethods.valueOf(methodStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new CatalogParameterException("Unknown function '" + methodStr + "'. Known methods are: "
                    + EnumSet.allOf(CheckMethods.class).stream().map(Enum::name).collect(Collectors.joining(", ")));
        }
        return method;
    }

}
