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

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ParamUtils {
    public static void checkId(long id, String name) throws CatalogParameterException {
        if (id < 0) {
            throw new CatalogParameterException("Error in id: '" + name + "' is not valid: " + id + ".");
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

    public static void checkObj(Object obj, String name) throws CatalogParameterException {
        if (obj == null) {
            throw new CatalogParameterException("parameter '" + name + "' is null.");
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
        if (path.isAbsolute()) {
            throw new CatalogParameterException("Error in path: Path '" + path + "' can't be absolute");
        } else if (path.toString().matches("\\.|\\.\\.")) {
            throw new CatalogParameterException("Error in path: Path '" + path + "' can't have relative names '.' or '..'");
        }
    }

    public static void checkAlias(String alias, String name, long offset) throws CatalogParameterException {
        if (alias == null || alias.isEmpty() || !alias.matches("^[_A-Za-z0-9-.\\+]+$")) {
            throw new CatalogParameterException("Error in alias: Invalid alias for '" + name + "'.");
        }
        if (StringUtils.isNumeric(alias) && Long.parseLong(alias) >= offset) {
            throw new CatalogParameterException("Error in alias: Invalid alias for '" + name + "'. Alias cannot be a numeric value above "
                    + offset);
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
}
