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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ParamUtils {

    public static void checkAllParametersExist(Iterator<String> parameterIterator, Function<String, Boolean> exist)
            throws CatalogParameterException {
        while (parameterIterator.hasNext()) {
            String parameter = parameterIterator.next();
            if (!exist.apply(parameter)) {
                throw new CatalogParameterException("Parameter " + parameter + " not supported");
            }
        }
    }

    public static void checkParameter(String param, String name) throws CatalogParameterException {
        if (StringUtils.isEmpty(param) || param.equals("null")) {
            throw new CatalogParameterException("Error in parameter: parameter '" + name + "' is null or empty: " + param + ".");
        }
    }

    public static void checkParameters(String... args) throws CatalogParameterException {
        if (args.length % 2 == 0) {
            for (int i = 0; i < args.length; i += 2) {
                checkParameter(args[i], args[i + 1]);
            }
        } else {
            throw new CatalogParameterException("Error in parameter: parameter list is not multiple of 2");
        }
    }

    public static void checkUpdateParametersMap(ObjectMap parameters) throws CatalogParameterException {
        if (parameters == null || parameters.isEmpty()) {
            throw new CatalogParameterException("Missing parameters to update");
        }
    }

    public static void checkObj(Object obj, String name) throws CatalogParameterException {
        if (obj == null) {
            throw CatalogParameterException.isNull(name);
        }
    }

    public static void checkNotEmptyArray(List<?> obj, String name) throws CatalogParameterException {
        if (CollectionUtils.isEmpty(obj)) {
            throw CatalogParameterException.isNull(name);
        }
    }

    public static void checkRegion(String regionStr, String name) throws CatalogParameterException {
        if (Pattern.matches("^([a-zA-Z0-9])+:([0-9])+-([0-9])+$", regionStr)) { //chr:start-end
            throw new CatalogParameterException("region '" + name + "' is not valid");
        }
    }

    public static void checkPath(String path, String paramName) throws CatalogParameterException {
        if (path == null) {
            throw CatalogParameterException.isNull(paramName);
        }
        checkPath(Paths.get(path), paramName);
    }

    public static void checkFileName(String fileName, String paramName) throws CatalogParameterException {
        checkParameter(fileName, paramName);
        if (fileName.contains("/")) {
            throw new CatalogParameterException("Error in " + paramName + ": '" + fileName + "' can not contain '/' character");
        }
    }

    public static void checkPath(Path path, String paramName) throws CatalogParameterException {
        checkObj(path, paramName);
        // FIXME: What if the path contains ":" ?
        if (path.isAbsolute()) {
            throw new CatalogParameterException("Error in path: Path '" + path + "' can't be absolute");
        } else if (path.toString().matches("\\.|\\.\\.")) {
            throw new CatalogParameterException("Error in path: Path '" + path + "' can't have relative names '.' or '..'");
        }
    }

    public static void checkValidUserId(String userId) throws CatalogParameterException {
        if (userId == null || userId.isEmpty()) {
            throw new CatalogParameterException("Missing user id.");
        }
        if (userId.equals(ParamConstants.ANONYMOUS_USER_ID) || userId.equals(ParamConstants.REGISTERED_USERS)) {
            throw new CatalogParameterException("User id cannot be one of the reserved OpenCGA users.");
        }
        if (!userId.matches("^[A-Za-z0-9]([-_.]?[A-Za-z0-9])*$")) {
            throw new CatalogParameterException("Invalid user id. Id needs to start by any character and might contain single '-', '_', "
                    + "'.', symbols followed by any character or number.");
        }
    }

    public static void checkAlias(String alias, String name) throws CatalogParameterException {
        if (alias == null || alias.isEmpty()) {
            throw CatalogParameterException.isNull(name);
        }
        if (!alias.matches("^[A-Za-z0-9]([-_.]?[A-Za-z0-9])*$")) {
            throw new CatalogParameterException("Invalid " + name + ". It needs to start by any character "
                    + "or number and might contain single '-', '_', '.', symbols followed by any character or number.");
        }
    }

    public static void checkIdentifier(String identifier, String name) throws CatalogParameterException {
        if (identifier == null || identifier.isEmpty() || !identifier.matches("^[A-Za-z0-9][A-Za-z0-9-_.]*[A-Za-z0-9]$")) {
            throw new CatalogParameterException("Error in identifier: Invalid identifier format for '" + name + "'.");
        }
    }

    public static void checkGroupId(String groupId) throws CatalogParameterException {
        if (groupId == null || groupId.isEmpty() || !groupId.matches("^[@]?[A-Za-z]([-_.]?[A-Za-z0-9])*$")) {
            throw new CatalogParameterException("Error in identifier: Invalid group identifier format");
        }
    }

    public static void checkIsSingleID(String id) throws CatalogParameterException {
        if (StringUtils.isNotEmpty(id)) {
            if (id.contains(",")) {
                throw new CatalogParameterException("More than one id found. Only one ID is allowed.");
            }
        } else {
            throw new CatalogParameterException("ID is null or Empty");
        }
    }

    public static void checkDateFormat(String creationDate, String param) throws CatalogParameterException {
        if (StringUtils.isEmpty(creationDate)) {
            throw CatalogParameterException.isNull(param);
        } else {
            // Validate creationDate can be parsed and has the proper format
            Date date = TimeUtils.toDate(creationDate);
            if (date == null || creationDate.length() != 14) {
                throw new CatalogParameterException("Unexpected '" + param + "' format. Expected format is 'yyyyMMddHHmmss'");
            }
        }
    }

    public static String checkDateOrGetCurrentDate(String date, String param) throws CatalogParameterException {
        if (StringUtils.isEmpty(date)) {
            return TimeUtils.getTime();
        } else {
            checkDateFormat(date, param);
            return date;
        }
    }

    public static long getAsLong(Object value) throws CatalogException {
        try {
            return (Long) value;
        } catch (ClassCastException e) {
            try {
                return Long.valueOf((Integer) value);
            } catch (ClassCastException e1) {
                throw new CatalogException(e);
            }
        }
    }

    public static String defaultString(String string, String defaultValue) {
        if (string == null || string.isEmpty()) {
            string = defaultValue;
        }
        return string;
    }

    public static <O> O defaultObject(O object, O defaultObject) {
        if (object == null) {
            object = defaultObject;
        }
        return object;
    }

    public static <O> O defaultObject(O object, Supplier<O> supplier) {
        if (object == null) {
            object = supplier.get();
        }
        return object;
    }

    public enum SaveInterpretationAs {
        PRIMARY,                    // Write the interpretation as the primary and move the current primary one (if any) as a secondary
        SECONDARY                   // Add interpretation as a secondary one
    }

    public enum AddRemoveAction {
        ADD,
        REMOVE;

        public static AddRemoveAction from(Map<String, ?> map, String key) {
            return from(map, key, null);
        }

        public static AddRemoveAction from(Map<String, ?> map, String key, AddRemoveAction defaultValue) {
            return getEnumFromMap(map, key, defaultValue, AddRemoveAction.class);
        }
    }

    public enum AddRemoveForceRemoveAction {
        ADD,
        REMOVE,
        FORCE_REMOVE;

        public static AddRemoveForceRemoveAction from(Map<String, ?> map, String key) {
            return from(map, key, null);
        }

        public static AddRemoveForceRemoveAction from(Map<String, ?> map, String key, AddRemoveForceRemoveAction defaultValue) {
            return getEnumFromMap(map, key, defaultValue, AddRemoveForceRemoveAction.class);
        }
    }

    public enum AddRemoveReplaceAction {
        ADD,
        REMOVE,
        REPLACE;

        public static AddRemoveReplaceAction from(Map<String, ?> map, String key) {
            return from(map, key, null);
        }

        public static AddRemoveReplaceAction from(Map<String, ?> map, String key, AddRemoveReplaceAction defaultValue) {
            return getEnumFromMap(map, key, defaultValue, AddRemoveReplaceAction.class);
        }
    }

    public enum BasicUpdateAction {
        ADD,
        SET,
        REMOVE;

        public static BasicUpdateAction from(Map<String, ?> map, String key) {
            return from(map, key, null);
        }

        public static BasicUpdateAction from(Map<String, ?> map, String key, BasicUpdateAction defaultValue) {
            return getEnumFromMap(map, key, defaultValue, BasicUpdateAction.class);
        }
    }

    public enum UpdateAction {
        ADD,
        SET,
        REMOVE,
        REPLACE;

        public static UpdateAction from(Map<String, ?> map, String key) {
            return from(map, key, null);
        }

        public static UpdateAction from(Map<String, ?> map, String key, UpdateAction defaultValue) {
            return getEnumFromMap(map, key, defaultValue, UpdateAction.class);
        }
    }

    public enum AclAction {
        SET,
        ADD,
        REMOVE,
        RESET;

        public static AclAction from(Map<String, ?> map, String key) {
            return from(map, key, null);
        }

        public static AclAction from(Map<String, ?> map, String key, AclAction defaultValue) {
            return getEnumFromMap(map, key, defaultValue, AclAction.class);
        }
    }

    public enum CompleteUpdateAction {
        ADD,
        SET,
        REMOVE,
        RESET,
        REPLACE;

        public static CompleteUpdateAction from(Map<String, ?> map, String key) {
            return from(map, key, null);
        }

        public static CompleteUpdateAction from(Map<String, ?> map, String key, CompleteUpdateAction defaultValue) {
            return getEnumFromMap(map, key, defaultValue, CompleteUpdateAction.class);
        }
    }

    public static <T extends Enum<T>> T getEnumFromMap(Map<String, ?> map, String key, T defaultValue, Class<T> enumClass) {
        if (map == null) {
            return defaultValue;
        }
        Object o = map.get(key);
        return getEnum(o, enumClass, defaultValue);
    }

    public static <T extends Enum<T>> T getEnum(Object value, Class<T> enumClass, T defaultValue) {
        if (value == null) {
            return defaultValue;
        } else {
            if (enumClass.isInstance(value)) {
                return enumClass.cast(value);
            } else {
                try {
                    return Enum.valueOf(enumClass, value.toString());
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Unknown value '" + value.toString() + "'. "
                            + "Accepted values are: " + EnumUtils.getEnumList(enumClass), e);
                }
            }
        }
    }

    public static void processDynamicVariables(ObjectMap fullMap, Pattern pattern) {
        processDynamicVariables(fullMap, fullMap, pattern);
    }

    /**
     * Process targetMap to look for the pattern and replace that pattern with the corresponding values from sourceMap.
     * Example:
     * sourceMap: {id: "myId", nested: {other: "myOtherValue"}}
     * targetMap: {a: 1, b: ${ABC.id}, c: ${ABC.nested.other}}
     * pattern: ${ABC.x.y.z}
     * <p>
     * Expected result:
     * targetMap: {a: 1, b: "myId", c: "myOtherValue"}
     *
     * @param sourceMap Map from which variables will be extracted.
     * @param targetMap Map that needs to be filled with the expected variables.
     * @param pattern   Pattern used to filter the dynamic variables.
     */
    public static void processDynamicVariables(ObjectMap sourceMap, Map<String, Object> targetMap, Pattern pattern) {
        // Loop over all the entries
        for (Map.Entry<String, Object> entry : targetMap.entrySet()) {
            if (entry.getValue() instanceof Map) {
                // Process nested map
                processDynamicVariables(sourceMap, (Map) entry.getValue(), pattern);
            } else if (entry.getValue() instanceof List) {
                List<?> values = (List<?>) entry.getValue();
                if (!values.isEmpty()) {
                    if (values.get(0) instanceof Map) {
                        // Loop over all map entries
                        for (Map tmpMap : ((Collection<Map>) entry.getValue())) {
                            // Process nested map
                            processDynamicVariables(sourceMap, tmpMap, pattern);
                        }
                    } else if (values.get(0) instanceof String) {
                        List<String> newValues = new ArrayList(values.size());
                        // Loop over all the String elements to see if needs any value replacing
                        for (Object value : values) {
                            String newValue = getStringPropertyValue(sourceMap, String.valueOf(value), pattern);
                            newValues.add(newValue);
                        }
                        entry.setValue(newValues);
                    }
                }
            }
            if (entry.getValue() instanceof String) {
                // Process String
                String originalValue = String.valueOf(entry.getValue());
                String newValue = getStringPropertyValue(sourceMap, originalValue, pattern);
                entry.setValue(newValue);
            }
        }
    }

    private static String getStringPropertyValue(ObjectMap fullMap, String originalValue, Pattern pattern) {
        String newValue = originalValue;
        Matcher matcher = pattern.matcher(originalValue);
        while (matcher.find()) {
            String variable = matcher.group(2);
            Object objectValue = fullMap.get(variable);
            if (objectValue != null) {
                // Only replace if it is not null, otherwise we will keep the property
                String value = String.valueOf(objectValue);
                newValue = newValue.replace(matcher.group(1), value);
            }
        }
        return newValue;
    }
}
