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

package org.opencb.opencga.catalog.exceptions;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogAuthorizationException extends CatalogException {
    public CatalogAuthorizationException(String message) {
        super(message);
    }

    public CatalogAuthorizationException(String message, Throwable cause) {
        super(message, cause);
    }

    public CatalogAuthorizationException(Throwable cause) {
        super(cause);
    }

    public static CatalogAuthorizationException cantRead(String userId, String resource, String id, String name) {
        return deny(userId, "read", resource, id, name);
    }

    public static CatalogAuthorizationException cantWrite(String userId, String resource, String id, String name) {
        return deny(userId, "write", resource, id, name);
    }

    public static CatalogAuthorizationException cantModify(String userId, String resource, String id, String name) {
        return deny(userId, "modify", resource, id, name);
    }

    public static CatalogAuthorizationException cantExecute(String userId, String resource, String id, String name) {
        return deny(userId, "execute", resource, id, name);
    }

    public static CatalogAuthorizationException adminOnlySupportedOperation() {
        return new CatalogAuthorizationException("Operation only support for 'admin' user");
    }

    @Deprecated
    public static CatalogAuthorizationException deny(String userId, String permission, String resource, long id, String name) {
        return new CatalogAuthorizationException("Permission denied. "
                + (userId == null || userId.isEmpty() ? "" : "User '" + userId + "'")
                + " cannot " + permission + " "
                + resource + " { id: " + id + (name == null || name.isEmpty() ? "" : ", name: \"" + name + "\"") + " }");
    }

    public static CatalogAuthorizationException deny(String userId, String permission, String resource, String id, String name) {
        return new CatalogAuthorizationException("Permission denied. "
                + (userId == null || userId.isEmpty() ? "" : "User '" + userId + "'")
                + " cannot " + permission + " "
                + resource + " { id: " + id + (name == null || name.isEmpty() ? "" : ", name: \"" + name + "\"") + " }");
    }

}
