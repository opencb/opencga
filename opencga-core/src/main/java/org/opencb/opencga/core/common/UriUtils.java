package org.opencb.opencga.core.common;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by hpccoll1 on 11/05/15.
 */
public class UriUtils {

    public static URI createUri(String input) throws URISyntaxException {
        URI sourceUri = new URI(null, input, null);
        if (sourceUri.getScheme() == null || sourceUri.getScheme().isEmpty()) {
            sourceUri = Paths.get(input).toUri();
        }
        return sourceUri;
    }

    public static URI createDirectoryUri(String input) throws URISyntaxException {
        if(!input.endsWith("/")) {
            input = "/";
        }
        return createUri(input);
    }

}
