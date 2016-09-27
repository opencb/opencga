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
        URI uri = createUri(input);
        // If path does not ends with / , create a new URI with path + "/"
        if(!uri.getPath().endsWith("/")) {
            uri = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(),
                    "//" + uri.getPath() + "/", uri.getQuery(), uri.getFragment());
        }
        return uri;
    }

}
