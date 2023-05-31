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

package org.opencb.opencga.core.common;

import org.apache.http.client.utils.URIBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.regex.Pattern;

/**
 * Created by hpccoll1 on 11/05/15.
 */
public class UriUtils {

    private static final Pattern HAS_SCHEMA = Pattern.compile("^[^/?#]+:/.*$");

    public static void checkUri(URI uri, String uriName, String schema) throws IOException {
        if(uri == null || uri.getScheme() != null && !uri.getScheme().equals(schema)) {
            throw new IOException("Expected " + schema + ":// uri scheme for " + uriName);
        }
    }

    public static URI createUri(String input) throws URISyntaxException {
        return createUri(input, true);
    }

    public static URI createUriSafe(String input) {
        try {
            return createUri(input, false);
        } catch (URISyntaxException e) {
            // Method above should never throw an exception
            throw new IllegalStateException(e);
        }
    }

    public static URI createUri(String input, boolean failOnInvalidUri) throws URISyntaxException {
        try {
            URI sourceUri;
            if (HAS_SCHEMA.matcher(input).matches()) {
                // Already a URI. Assume it is already escaped.
                // Avoid double code escaping
                sourceUri = new URI(input);
            } else {
                // Assume direct path name.
                // Escape if needed.
                sourceUri = Paths.get(input).toUri();
            }
            return sourceUri;
        } catch (URISyntaxException e) {
            if (failOnInvalidUri) {
                throw e;
            } else {
                return null;
            }
        }
    }

    public static URI createDirectoryUriSafe(String input) {
        try {
            return createDirectoryUri(input);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static URI createDirectoryUri(String input) throws URISyntaxException {
        URI uri = createUri(input);
        // If path does not ends with / , create a new URI with path + "/"
        if(!uri.getPath().endsWith("/")) {
            uri = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(),
                    "//" + uri.getPath() + "/", uri.getQuery(), uri.getFragment());
        }
        return uri;
    }

    public static String fileName(URI uri) {
        String path = uri.getPath();
        int idx = path.lastIndexOf("/");
        return idx < 0 ? path : path.substring(idx + 1);
    }

    public static String dirName(URI uri) {
        String path = uri.getPath();
        int idx2 = path.lastIndexOf('/');
        if (idx2 <= 0) {
            return "/";
        }
        int idx1 = path.lastIndexOf('/', idx2 - 1);
        return path.substring(idx1 + 1, idx2 + 1);
    }

    public static URI resolve(URI uri, String file) {
        String newPath = Paths.get(uri.getPath()).resolve(file).toString();
        return replacePath(uri, newPath);
    }

    public static URI replacePath(URI uri, String path) {
        try {
            return new URIBuilder(uri).setPath(path).build();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
