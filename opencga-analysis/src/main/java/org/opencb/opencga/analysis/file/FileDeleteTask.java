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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.IOManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileStatus;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.models.file.SmallFileInternal;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;

import java.io.IOException;
import java.util.*;

@Tool(id = FileDeleteTask.ID, resource = Enums.Resource.FILE, type = Tool.Type.OPERATION, description = "Delete files.")
public class FileDeleteTask extends OpenCgaTool {

    public final static String ID = "files-delete";

    private List<String> files;
    private String studyFqn;

    private boolean skipTrash;

    private String randomMark;

    public FileDeleteTask setStudy(String study) {
        this.studyFqn = study;
        return this;
    }

    public FileDeleteTask setFiles(List<String> files) {
        this.files = files;
        return this;
    }

    public FileDeleteTask setSkipTrash(boolean skipTrash) {
        this.skipTrash = skipTrash;
        return this;
    }

    @Override
    protected void check() throws Exception {
        if (StringUtils.isEmpty(studyFqn)) {
            throw new ToolException("Missing mandatory study parameter");
        }
        if (ListUtils.isEmpty(files)) {
            throw new ToolException("Missing mandatory list of files");
        }
    }

    @Override
    protected void run() throws Exception {
        FileManager fileManager = catalogManager.getFileManager();

        randomMark = "delete_" + RandomStringUtils.randomAlphanumeric(8);
        addAttribute("delete-mark", randomMark);

        step("check-can-delete", () -> {
            FileUpdateParams updateParams = new FileUpdateParams()
                    .setInternal(new SmallFileInternal(new FileStatus(FileStatus.PENDING_DELETE)))
                    .setTags(Collections.singletonList(randomMark));

            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(FileDBAdaptor.QueryParams.TAGS.key(), ParamUtils.UpdateAction.ADD.name());
            QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

            // Check and mark all the files for deletion
            for (String file : files) {
                logger.info("Checking file '{}' can be deleted...", file);

                File catalogFile;
                try {
                    catalogFile = fileManager.get(studyFqn, file, FileManager.INCLUDE_FILE_URI_PATH, token).first();
                    fileManager.checkCanDeleteFile(studyFqn, catalogFile.getUuid(), false, token);
                } catch (CatalogException e) {
                    logger.error("Error checking file '{}': {}", file, e.getMessage(), e);
                    addError(e);
                    continue;
                }

                try {
                    // Update file status to PENDING_DELETE and add tags mark
                    if (catalogFile.getType() == File.Type.FILE) {
                        fileManager.update(studyFqn, file, updateParams, options, token);
                    } else {
                        // We mark for deletion all the
                        Query query = new Query()
                                .append(FileDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), FileStatus.READY)
                                .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + catalogFile.getPath() + "*");
                        fileManager.update(studyFqn, query, updateParams, options, token);
                    }
                } catch (Exception e) {
                    logger.error("Error updating status of file '{}' to PENDING_DELETE: {}", file, e.getMessage(), e);
                    addError(e);
                }
            }
        });

        step(ID, () -> {
            // Delete the files pending for deletion
            Query query = new Query()
                    .append(FileDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), FileStatus.PENDING_DELETE)
                    .append(FileDBAdaptor.QueryParams.TAGS.key(), randomMark);
            try (DBIterator<File> iterator = fileManager.iterator(studyFqn, query, FileManager.EXCLUDE_FILE_ATTRIBUTES, token)) {
                while (iterator.hasNext()) {
                    File file = iterator.next();
                    try {
                        logger.info("Deleting file '{}'...", file.getPath());
                        QueryOptions params = new QueryOptions(Constants.SKIP_TRASH, skipTrash);
                        fileManager.delete(studyFqn, Collections.singletonList(file.getUuid()), params, token);
                    } catch (Exception e) {
                        logger.error("Error deleting file '{}': {}", file.getPath(), e.getMessage(), e);
                        logger.info("Restoring status of file '{}'", file.getPath());
                        restoreFile(file);

                        addError(new CatalogException("Could not delete file '" + file.getPath() + "': " + e.getMessage(), e));
                    }
                }
            }
        });
    }

    @Override
    public List<String> getSteps() {
        return Arrays.asList("check-can-delete", getId());
    }

    @Override
    protected void onShutdown() {
        recoverFromFatalCrash();
    }

    private void recoverFromFatalCrash() {
        // Delete the files pending for deletion
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), FileStatus.PENDING_DELETE)
                .append(FileDBAdaptor.QueryParams.TAGS.key(), randomMark);
        restoreFiles(query);

        if (skipTrash) {
            query.put(FileDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), FileStatus.DELETING);
            OpenCGAResult<File> fileResult = null;
            try {
                fileResult = catalogManager.getFileManager().search(studyFqn, query, FileManager.EXCLUDE_FILE_ATTRIBUTES,
                        token);
            } catch (CatalogException e) {
                logger.error("Critical: Could not check if there are any inconsistent files", e);
                return;
            }
            if (fileResult.getNumResults() > 0) {
                for (File file : fileResult.getResults()) {
                    // Check file is still present in disk
                    try {
                        IOManager ioManager;
                        try {
                            ioManager = catalogManager.getIoManagerFactory().get(file.getUri());
                        } catch (IOException e) {
                            throw CatalogIOException.ioManagerException(file.getUri(), e);
                        }
                        if (ioManager.exists(file.getUri())) {
                            restoreFile(file);
                        } else {
                            setToMissingFile(file);
                        }
                    } catch (CatalogIOException e) {
                        addCriticalError(e);
                    }
                }
            }
        }
    }

    private void restoreFile(File file) {
        Query query = new Query(FileDBAdaptor.QueryParams.UUID.key(), file.getUuid());
        restoreFiles(query);
    }

    private void restoreFiles(Query query) {
        // Restore non-deleted files to READY status
        FileUpdateParams updateParams = new FileUpdateParams()
                .setInternal(new SmallFileInternal(new FileStatus(FileStatus.READY)))
                .setTags(Collections.singletonList(randomMark));

        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(FileDBAdaptor.QueryParams.TAGS.key(), ParamUtils.UpdateAction.REMOVE.name());
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

        restore(query, updateParams, options);
    }

    private void setToMissingFile(File file) {
        Query query = new Query(FileDBAdaptor.QueryParams.UUID.key(), file.getUuid());

        // Restore non-deleted files to MISSING status
        FileUpdateParams updateParams = new FileUpdateParams()
                .setInternal(new SmallFileInternal(new FileStatus(FileStatus.MISSING)))
                .setTags(Collections.singletonList(randomMark));

        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(FileDBAdaptor.QueryParams.TAGS.key(), ParamUtils.UpdateAction.REMOVE.name());
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

        restore(query, updateParams, options);
    }

    private void restore(Query query, FileUpdateParams updateParams, QueryOptions options) {
        try {
            catalogManager.getFileManager().update(studyFqn, query, updateParams, options, token);
        } catch (CatalogException e) {
            addCriticalError(e);
        }
    }

    private void addCriticalError(CatalogException e) {
        CatalogException exception = new CatalogException("Critical: Could not restore status of pending files to "
                + FileStatus.READY, e);
        logger.error("{}", e.getMessage(), e);
        try {
            addError(exception);
        } catch (ToolException ex) {
            logger.error("Unexpected error occurred when reporting the previous error", ex);
        }
    }

}
