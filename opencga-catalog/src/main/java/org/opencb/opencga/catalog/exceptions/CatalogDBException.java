/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.catalog.exceptions;

import org.apache.commons.lang3.StringUtils;

/**
 * Created by imedina on 11/09/14.
 */
public class CatalogDBException extends CatalogException {

    public CatalogDBException(String msg) {
        super(msg);
    }

    public CatalogDBException(Throwable cause) {
        super(cause);
    }

    public CatalogDBException(String message, Throwable cause) {
        super(message, cause);
    }

    public static CatalogDBException newInstance(String message, Object... arguments) {
        for (Object argument : arguments) {
            message = StringUtils.replace(message, "{}", String.valueOf(argument), 1);
        }
        return new CatalogDBException(message);
    }

    public static CatalogDBException idNotFound(String name, String id) {
        return new CatalogDBException(name + " { id: \"" + id + "\" } not found.");
    }

    public static CatalogDBException idNotFound(String name, long id) {
        return new CatalogDBException(name + " { id: " + id + " } not found.");
    }

    public static CatalogDBException alreadyExists(String name, String key, String value) {
        return new CatalogDBException(name + " { " + key + ":\"" + value + "\"} already exists");
    }

    public static CatalogDBException alreadyExists(String name, long id) {
        return new CatalogDBException(name + " { id:" + id + "} already exists");
    }

    public static CatalogDBException updateError(String name, long id) {
        return new CatalogDBException(name + " {id: " + id + "} could not be updated.");
    }

    public static CatalogDBException deleteError(String name) {
        return new CatalogDBException(name + ": It has been impossible to delete the object(s) from the database.");
    }

    public static CatalogDBException removeError(String name) {
        return new CatalogDBException(name + ": It has been impossible to remove the object(s) from the database.");
    }

    public static CatalogDBException alreadyDeletedOrRemoved(String name) {
        return new CatalogDBException(name + ": The object(s) were already marked as deleted or removed.");
    }

    public static CatalogDBException queryNotFound(String name) {
        return new CatalogDBException(name + ": The query used to delete did not report any result.");
    }

    public static CatalogDBException queryParamNotFound(String name, String resource) {
        return new CatalogDBException("The query param " + name + " does not exist for searching over " + resource + ".");
    }

    public static CatalogDBException fileInUse(long id, long count) {
        return new CatalogDBException("The file { id: " + id + "} cannot be removed as it is being used as input in " + count + " job(s).");
    }

    public static CatalogDBException sampleIdIsParentOfOtherIndividual(long id) {
        return new CatalogDBException("The sample { id: " + id + "} cannot be removed as it is already the parent of other individual(s).");
    }

}
