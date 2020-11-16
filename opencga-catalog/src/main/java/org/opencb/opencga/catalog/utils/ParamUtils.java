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

import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
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
            throw new CatalogParameterException("Parameter '" + name + "' is missing or null.");
        }
    }

    public static void checkRegion(String regionStr, String name) throws CatalogParameterException {
        if (Pattern.matches("^([a-zA-Z0-9])+:([0-9])+-([0-9])+$", regionStr)) { //chr:start-end
            throw new CatalogParameterException("region '" + name + "' is not valid");
        }
    }

    public static void checkPath(String path, String paramName) throws CatalogParameterException {
        if (path == null) {
            throw new CatalogParameterException("parameter '" + paramName + "' is null.");
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
        if (!userId.matches("^[A-Za-z0-9]([-_.]?[A-Za-z0-9])*$")) {
            throw new CatalogParameterException("Invalid user id. Id needs to start by any character and might contain single '-', '_', "
                    + "'.', symbols followed by any character or number.");
        }
    }

    public static void checkAlias(String alias, String name) throws CatalogParameterException {
        if (alias == null || alias.isEmpty()) {
            throw new CatalogParameterException("Missing id.");
        }
        if (!alias.matches("^[A-Za-z0-9]([-_.]?[A-Za-z0-9])*$")) {
            throw new CatalogParameterException("Invalid id for '" + name + "'. Alias needs to start by any character "
                    + "or number and might contain single '-', '_', '.', symbols followed by any character or number.");
        }
    }

    public static void checkIdentifier(String identifier, String name) throws CatalogParameterException {
        if (identifier == null || identifier.isEmpty() || !identifier.matches("^[A-Za-z]([-_.]?[A-Za-z0-9])*$")) {
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
}
