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

/**
 * Created by pfurio on 26/07/17.
 */
public class CatalogAuthenticationException extends CatalogException {

    public CatalogAuthenticationException(String message) {
        super(message);
    }

    public CatalogAuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }

    public CatalogAuthenticationException(Throwable cause) {
        super(cause);
    }

    public static CatalogAuthenticationException tokenExpired(String token) {
        return new CatalogAuthenticationException("Authentication token is expired : " + token);
    }

    public static CatalogAuthenticationException invalidAuthenticationToken(String token) {
        return new CatalogAuthenticationException("Invalid authentication token : " + token);
    }

    public static CatalogAuthenticationException unknownJwtException(String token) {
        return new CatalogAuthenticationException("Unknown Jwt exception: " + token);
    }

    public static CatalogAuthenticationException invalidAuthenticationEncodingToken(String token) {
        return new CatalogAuthenticationException("Invalid authentication token encoding : " + token);
    }

    public static CatalogAuthenticationException incorrectUserOrPassword() {
        return new CatalogAuthenticationException("Incorrect user or password.");
    }

    public static CatalogAuthenticationException incorrectUserOrPassword(String domain, Exception e) {
        return new CatalogAuthenticationException(domain + ": Incorrect user or password.", e);
    }

    public static CatalogAuthenticationException userNotAllowed() {
        return new CatalogAuthenticationException("User not allowed to access the system.");
    }

}
