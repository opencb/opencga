package org.opencb.opencga.core;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by hpccoll1 on 11/05/15.
 */
public class UriUtils {

    public static URI getUri(String input) throws URISyntaxException {
        Path inputFile = Paths.get(input);
        URI sourceUri = new URI(null, input, null);
        if (sourceUri.getScheme() == null || sourceUri.getScheme().isEmpty()) {
            sourceUri = inputFile.toUri();
        }
        return sourceUri;
    }
}
