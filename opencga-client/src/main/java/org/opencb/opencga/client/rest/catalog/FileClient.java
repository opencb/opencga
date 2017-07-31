/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.client.rest.catalog;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.FileTree;
import org.opencb.opencga.catalog.models.acls.permissions.FileAclEntry;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

/**
 * Created by swaathi on 10/05/16.
 */
public class FileClient extends CatalogClient<File, FileAclEntry> {

    private static final String FILES_URL = "files";

    public FileClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);

        this.category = FILES_URL;
        this.clazz = File.class;
        this.aclClass = FileAclEntry.class;
    }

    public QueryResponse<File> createFolder(String studyId, String path, ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "path", path, "directory", true);
        ObjectMap myParams = new ObjectMap()
                .append("study", studyId)
                .append("body", params);
        return execute(FILES_URL, "create", myParams, POST, File.class);
    }

    public QueryResponse<File> relink(String fileId, String uri, QueryOptions options) throws CatalogException, IOException {
        ObjectMap params = new ObjectMap(options);
        params = addParamsToObjectMap(params, "uri", uri);
        return execute(FILES_URL, fileId.replace("/", ":"), "relink", params, GET, File.class);
    }

    public QueryResponse<File> unlink(String fileId, ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "fileId", fileId);
        return execute(FILES_URL, "unlink", params, GET, File.class);
    }

    public QueryResponse<File> content(String fileId, ObjectMap params) throws CatalogException, IOException {
        return execute(FILES_URL, fileId.replace("/", ":"), "content", params, GET, File.class);
    }

    public QueryResponse<File> download(String fileId, ObjectMap params) throws CatalogException, IOException {
        return execute(FILES_URL, fileId.replace("/", ":"), "download", params, GET, File.class);
    }

    public QueryResponse<File> grep(String fileId, String pattern, ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "pattern", pattern);
        return execute(FILES_URL, fileId.replace("/", ":"), "grep", params, GET, File.class);
    }

    public QueryResponse<File> list(String folderId, ObjectMap options) throws CatalogException, IOException {
        folderId = folderId.replace('/', ':');
        return execute(FILES_URL, folderId, "list", options, GET, File.class);
    }

    public QueryResponse<File> delete(String fileId, ObjectMap params) throws CatalogException, IOException {
        return execute(FILES_URL, fileId.replace("/", ":"), "delete", params, GET, File.class);
    }

    public QueryResponse<FileTree> tree(String folderId, ObjectMap params) throws CatalogException, IOException {
        return execute(FILES_URL, folderId.replace("/", ":"), "tree", params, GET, FileTree.class);
    }

    public QueryResponse<File> refresh(String fileId, ObjectMap options) throws CatalogException, IOException {
        return execute(FILES_URL, fileId.replace("/", ":"), "refresh", options, GET, File.class);
    }

    public QueryResponse<File> upload(String studyId, String filePath, ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, FileDBAdaptor.QueryParams.STUDY.key(), studyId, "file", filePath);
        return execute(FILES_URL, "upload", params, POST, File.class);
    }

    public QueryResponse<File> groupBy(String studyId, String fields, ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, FileDBAdaptor.QueryParams.STUDY.key(), studyId, "fields", fields);
        return execute(FILES_URL, "groupBy", params, GET, File.class);
    }

}
