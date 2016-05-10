package org.opencb.opencga.client.rest;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;
import java.net.URI;

/**
 * Created by swaathi on 10/05/16.
 */
public class FileClient extends AbstractParentClient {
    private static final String FILES_URL = "files";

    protected FileClient(String sessionId, ClientConfiguration configuration) {
        super(sessionId, configuration);
    }

    public QueryResponse<File> list(String fileId, QueryOptions options) throws CatalogException, IOException {
        QueryResponse<File> files = execute(FILES_URL, fileId, "list", options, File.class);
        return files;
    }

    public QueryResponse<URI> getURI(String fileId, QueryOptions options) throws CatalogException, IOException {
        QueryResponse<URI> uri = execute(FILES_URL, fileId, "uri", options, URI.class);
        return uri;
    }

    public QueryResponse<File> get(String fileId, QueryOptions options) throws CatalogException, IOException {
        QueryResponse<File> files = execute(FILES_URL, fileId, "info", options, File.class);
        return files;
    }
}
