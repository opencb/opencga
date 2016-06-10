/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.client.rest;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.acls.FileAcl;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;
import java.net.URI;

/**
 * Created by swaathi on 10/05/16.
 */
public class FileClient extends AbstractParentClient<File, FileAcl> {

    private static final String FILES_URL = "files";

    protected FileClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);

        this.category = FILES_URL;
        this.clazz = File.class;
        this.aclClass = FileAcl.class;
    }

    public QueryResponse<File> createFolder(String studyId, String path, ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "studyId", studyId, "folder", path);
        return execute(FILES_URL, "create-folder", params, File.class);
    }

    public QueryResponse<File> index(String fileId, ObjectMap params) throws CatalogException, IOException {
        return execute(FILES_URL, fileId, "index", params, File.class);
    }

    public QueryResponse<File> link(String studyId, String uri, String studyPath, ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "studyId", studyId, "uri", uri, "path", studyPath);
        return execute(FILES_URL, "link", params, File.class);
    }

    public QueryResponse<File> relink(String fileId, String uri, QueryOptions options) throws CatalogException, IOException {
        ObjectMap params = new ObjectMap(options);
        params = addParamsToObjectMap(params, "uri", uri);
        return execute(FILES_URL, fileId, "relink", params, File.class);
    }

    public QueryResponse<File> unlink(String fileId, ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "fileId", fileId);
        return execute(FILES_URL, "unlink", params, File.class);
    }

    public QueryResponse<File> content(String fileId, ObjectMap params) throws CatalogException, IOException {
        return execute(FILES_URL, fileId, "content", params, File.class);
    }

    public QueryResponse<File> download(String fileId, ObjectMap params) throws CatalogException, IOException {
        return execute(FILES_URL, fileId, "download", params, File.class);
    }

    public QueryResponse<File> grep(String fileId, String pattern, ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "pattern", pattern);
        return execute(FILES_URL, fileId, "grep", params, File.class);
    }

    public QueryResponse<File> list(String fileId, QueryOptions options) throws CatalogException, IOException {
        return execute(FILES_URL, fileId, "list", options, File.class);
    }

    public QueryResponse<File> getFiles(String fileId, QueryOptions options) throws CatalogException, IOException {
        return execute(FILES_URL, fileId, "files", options, File.class);
    }

    public QueryResponse<File> update(String fileId, ObjectMap params) throws CatalogException, IOException {
        return execute(FILES_URL, fileId, "update", params, File.class);
    }

    public QueryResponse<File> delete(String fileId, ObjectMap params) throws CatalogException, IOException {
        return execute(FILES_URL, fileId, "delete", params, File.class);
    }

    public QueryResponse<File> refresh(String fileId, QueryOptions options) throws CatalogException, IOException {
        return execute(FILES_URL, fileId, "refresh", options, File.class);
    }

    /**
     * @deprecated  As of release 0.8, replaced by {@link #get(String id, QueryOptions options)}
     * @param fileId fileId.
     * @param options options.
     * @return queryResponse.
     * @throws CatalogException catalogException.
     * @throws IOException IOException.
     */
    @Deprecated
    public QueryResponse<URI> getURI(String fileId, QueryOptions options) throws CatalogException, IOException {
        QueryResponse<URI> uri = execute(FILES_URL, fileId, "uri", options, URI.class);
        return uri;
    }


}
