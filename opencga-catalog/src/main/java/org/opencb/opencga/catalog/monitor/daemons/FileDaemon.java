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

package org.opencb.opencga.catalog.monitor.daemons;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.managers.CatalogFileUtils;
import org.opencb.opencga.core.common.TimeUtils;

import java.io.IOException;

/**
 * Created by imedina on 16/06/16.
 */
public class FileDaemon extends MonitorParentDaemon {

    private int deleteDelay;
    private long deleteDelayMillis;

    private CatalogFileUtils catalogFileUtils;

    public FileDaemon(int period, int deleteDelay, String sessionId, CatalogManager catalogManager) {
        super(period, sessionId, catalogManager);
        this.deleteDelay = deleteDelay;
        this.deleteDelayMillis = (long) (deleteDelay * 24 * 60 * 60 * 1000);

        this.catalogFileUtils = new CatalogFileUtils(catalogManager);
    }

    @Override
    public void run() {

        while (!exit) {
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                if (!exit) {
                    e.printStackTrace();
                }
            }
            logger.info("----- FILE DAEMON -----", TimeUtils.getTimeMillis());

            try {
                checkDeletedFiles();

                checkPendingRemoveFiles();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void checkDeletedFiles() throws CatalogException {
        QueryResult<File> files = catalogManager.searchFile(
                -1,
                new Query(FileDBAdaptor.QueryParams.FILE_STATUS.key(), File.FileStatus.TRASHED),
                new QueryOptions(),
                sessionId);

        long currentTimeMillis = System.currentTimeMillis();
        for (File file: files.getResult()) {
            try {
                //TODO: skip if the file is a non-empty folder
                long deleteTimeMillis = TimeUtils.toDate(file.getStatus().getDate()).toInstant().toEpochMilli();
//                long deleteDate = new ObjectMap(file.getAttributes()).getLong(file.getName().getCreationDate(), 0);
                if ((currentTimeMillis - deleteTimeMillis) > deleteDelayMillis) {
//                            QueryResult<Study> studyQueryResult =
//                                    catalogManager.getStudy(catalogManager.getStudyIdByFileId(file.getId()), sessionId);
//                            Study study = studyQueryResult.getResult().get(0);
//                            logger.info("Deleting file {} from study {id: {}, alias: {}}", file, study.getId(), study.getAlias());
                    catalogFileUtils.delete(file, sessionId);
                } else {
                            logger.info("Don't delete file {id: {}, path: '{}', attributes: {}}}", file.getId(), file.getPath(),
                                    file.getAttributes());
                            logger.info("{}s", (currentTimeMillis - deleteTimeMillis) / 1000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void checkPendingRemoveFiles() throws CatalogException {
        QueryResult<File> files = catalogManager.searchFile(
                -1,
                new Query(FileDBAdaptor.QueryParams.FILE_STATUS.key(), File.FileStatus.DELETED),
                new QueryOptions(),
                sessionId);

        long currentTimeMillis = System.currentTimeMillis();
        for (File file: files.getResult()) {
            try {
                long deleteTimeMillis = TimeUtils.toDate(file.getStatus().getDate()).toInstant().toEpochMilli();
                if ((currentTimeMillis - deleteTimeMillis) > deleteDelayMillis) {
                    if (file.getType().equals(File.Type.FILE)) {
                        catalogManager.getFileManager().delete(Long.toString(file.getId()), null, sessionId);
                    } else {
                        System.out.println("empty block");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
