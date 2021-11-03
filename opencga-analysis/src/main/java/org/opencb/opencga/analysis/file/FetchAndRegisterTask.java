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

package org.opencb.opencga.analysis.file;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.managers.StudyManager;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileAclEntry;
import org.opencb.opencga.core.models.file.FileFetch;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

@Tool(id = FetchAndRegisterTask.ID, resource = Enums.Resource.FILE, type = Tool.Type.OPERATION,
        description = "Download an external file and register it in OpenCGA.")
public class FetchAndRegisterTask extends OpenCgaToolScopeStudy {

    public final static String ID = "files-fetch";

    @ToolParams
    protected FileFetch toolParams;

    private String fileName;
    private String studyFqn;

    @Override
    protected void check() throws Exception {
        studyFqn = getStudyFqn();
        if (StringUtils.isEmpty(studyFqn)) {
            throw new ToolException("Missing mandatory study parameter");
        }
        if (StringUtils.isEmpty(toolParams.getUrl())) {
            throw new ToolException("Missing mandatory url");
        }

        URI uri = new URI(toolParams.getUrl());
        fileName = UriUtils.fileName(uri);

        if (!toolParams.getPath().endsWith("/")) {
            toolParams.setPath(toolParams.getPath() + "/");
        }

        try {
            Study study = catalogManager.getStudyManager().get(studyFqn, StudyManager.INCLUDE_STUDY_IDS, token).first();

            OpenCGAResult<File> parents = catalogManager.getFileManager().getParents(studyFqn, toolParams.getPath(), false,
                    QueryOptions.empty(), token);
            if (parents.getNumResults() == 0) {
                throw new ToolException("No parent folders found for " + toolParams.getPath());
            }

            if (parents.first().isExternal()) {
                throw new CatalogException("Parent path " + parents.first().getPath() + " is external. Cannot download to mounted folders");
            }

            String userId = catalogManager.getUserManager().getUserId(token);
            // Check write permissions over the path
            catalogManager.getAuthorizationManager()
                    .checkFilePermission(study.getUid(), parents.first().getUid(), userId, FileAclEntry.FilePermissions.WRITE);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
    }

    @Override
    protected void run() throws Exception {
        step("download", () -> {
            ReadableByteChannel readableByteChannel;
            FileOutputStream fileOutputStream;
            try {
                URL url = new URL(toolParams.getUrl());
                readableByteChannel = Channels.newChannel(url.openStream());
                fileOutputStream = new FileOutputStream(getOutDir().resolve(fileName).toString());
            } catch (IOException e) {
                throw new CatalogException("URL issue: " + e.getMessage(), e);
            }

            try {
                fileOutputStream.getChannel().transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
            } catch (Exception e) {
                deleteTemporaryFile();
                throw new CatalogException("Could not download file " + fileName, e);
            }
        });

        step("register", () -> {
            // Move downloaded file and register
            try {
                moveFile(studyFqn, getOutDir().resolve(fileName), null, toolParams.getPath(), token);
            } catch (Exception e) {
                deleteTemporaryFile();
                throw new CatalogException(e.getMessage(), e);
            }
        });
    }

    @Override
    public List<String> getSteps() {
        return Arrays.asList("download", "register");
    }

    @Override
    protected void onShutdown() {
        try {
            deleteTemporaryFile();
        } catch (CatalogIOException e) {
            throw new RuntimeException("Could not delete temporal file", e);
        }
    }

    private void deleteTemporaryFile() throws CatalogIOException {
        if (Files.exists(getOutDir().resolve(fileName))) {
            URI outDirUri = getOutDir().toUri();
            try {
                catalogManager.getIoManagerFactory().get(outDirUri).deleteFile(getOutDir().resolve(fileName).toUri());
            } catch (IOException e) {
                throw CatalogIOException.ioManagerException(outDirUri, e);
            }
        }
    }
}
