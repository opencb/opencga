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
import org.opencb.commons.datastore.core.result.Error;

import java.util.List;

/**
 * Created by jacobo on 12/12/14.
 */
public class CatalogException extends Exception {

    public CatalogException(String message) {
        super(message);
    }

    public CatalogException(String message, Throwable cause) {
        super(message, cause);
    }

    public CatalogException(Throwable cause) {
        super(cause);
    }

    public static CatalogException notFound(String entity, List<String> entries) {
        return new CatalogException("Missing " + entity + ": " + StringUtils.join(entries, ", ") + " not found.");
    }

    public static CatalogException appendMessage(CatalogException e, String message) {
        String fullMessage = message + e.getMessage();
        if (e instanceof CatalogAuthorizationException) {
            return new CatalogAuthorizationException(fullMessage, e.getCause());
        } else if (e instanceof CatalogAuthenticationException) {
            return new CatalogAuthenticationException(fullMessage, e.getCause());
        } else {
            return new CatalogException(fullMessage, e.getCause());
        }
    }

    public Error getError() {
        return new Error(0, "", this.getMessage());
    }
}
