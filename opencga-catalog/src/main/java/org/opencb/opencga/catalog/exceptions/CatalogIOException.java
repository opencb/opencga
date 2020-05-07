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


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class CatalogIOException extends CatalogException {

    private static final long serialVersionUID = 1L;

    public CatalogIOException(String msg) {
        super(msg);
    }

    public CatalogIOException(String message, Throwable cause) {
        super(message, cause);
    }

    public static CatalogIOException uriSyntaxException(String name, URISyntaxException e) {
        return new CatalogIOException("Uri syntax error while parsing \"" + name + "\"", e);
    }

    public static CatalogIOException ioManagerException(URI uri, IOException e) {
        return new CatalogIOException("Could not get corresponding IOManager for URI '" + uri + "': " + e.getMessage(), e);
    }

}
