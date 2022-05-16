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

    public static CatalogDBException uidNotFound(String name, long id) {
        return new CatalogDBException(name + " { uid: " + id + " } not found.");
    }

    public static CatalogDBException alreadyExists(String name, String key, String value) {
        return generalAlreadyExists(name, -1, key, value, null);
    }

    public static CatalogDBException alreadyExists(String name, String key, String value, Exception e) {
        return generalAlreadyExists(name, -1, key, value, e);
    }

    public static CatalogDBException alreadyExists(String name, long fromStudyId, String key, String value) {
        return generalAlreadyExists(name, fromStudyId, key, value, null);
    }

    public static CatalogDBException alreadyExists(String name, long fromStudyId, String key, String value, Exception e) {
        return generalAlreadyExists(name, fromStudyId, key, value, e);
    }

    public static CatalogDBException alreadyExists(String name, long id) {
        return generalAlreadyExists(name, -1, "id", id, null);
    }

    public static CatalogDBException alreadyExists(String name, long id, Exception e) {
        return generalAlreadyExists(name, -1, "id", id, e);
    }

    private static CatalogDBException generalAlreadyExists(String name, long fromStudyId, String key, Object value, Exception e) {
        StringBuilder sb = new StringBuilder(name);
        sb.append(" { ").append(key).append(" : ");
        if (value == null) {
            sb.append("null");
        } else if (value instanceof Number) {
            sb.append(value);
        } else {
            sb.append("\"").append(value.toString()).append("\"");
        }
        sb.append(" }");
        if (fromStudyId >= 0) {
            sb.append(" from study { id : ").append(fromStudyId).append(" }");
        }
        sb.append(" already exists.");

        if (e == null) {
            return new CatalogDBException(sb.toString());
        } else {
            return new CatalogDBException(sb.toString(), e);
        }
    }

    public static CatalogDBException updateError(String name, long id) {
        return new CatalogDBException(name + " { id: " + id + " } could not be updated.");
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

    public static CatalogDBException cannotUpdateMultipleEntries(String field, String entry) {
        return new CatalogDBException("Update " + entry + ": Cannot update '" + field + "' parameter for multiple entries");
    }

    public static CatalogDBException jwtSecretKeyException() {
        return new CatalogDBException("JWT secret key should be at least 30 characters long and contain at least 1 upper case, 1 lower "
                + "case, 1 digit and 1 special character ");
    }

}
