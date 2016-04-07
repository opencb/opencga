package org.opencb.opencga.core.common;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

/**
 * Created by hpccoll1 on 11/05/15.
 */
public class UriUtils {

    public static void checkUri(URI uri, String uriName, String schema) throws IOException {
        if(uri == null || uri.getScheme() != null && !uri.getScheme().equals(schema)) {
            throw new IOException("Expected file:// uri scheme for " + uriName);
        }
    }

    public static URI createUri(String input) throws URISyntaxException {
        URI sourceUri = new URI(null, input, null);
        if (sourceUri.getScheme() == null || sourceUri.getScheme().isEmpty()) {
            sourceUri = Paths.get(input).toUri();
        }
        return sourceUri;
    }

    public static URI createDirectoryUri(String input) throws URISyntaxException {
        if(!input.endsWith("/")) {
            input += "/";
        }
        return createUri(input);
    }

}
