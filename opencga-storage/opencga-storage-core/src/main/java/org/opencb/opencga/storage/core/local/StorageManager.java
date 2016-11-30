/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.storage.core.local;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.DataStore;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.cache.CacheManager;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.local.models.FileInfo;
import org.opencb.opencga.storage.core.local.models.StudyInfo;
import org.opencb.opencga.storage.core.local.variant.operations.StorageOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


public abstract class StorageManager {

    protected final CatalogManager catalogManager;
    protected final CacheManager cacheManager;
    protected final StorageConfiguration storageConfiguration;
    protected final StorageManagerFactory storageManagerFactory;

    @Deprecated
    protected final String storageEngineId;

    protected final Logger logger;

    public StorageManager(CatalogManager catalogManager, StorageConfiguration storageConfiguration) {
        this(catalogManager, new CacheManager(storageConfiguration), storageConfiguration,
                storageConfiguration.getDefaultStorageEngineId(), null);
    }

    public StorageManager(CatalogManager catalogManager, StorageManagerFactory storageManagerFactory) {
        this(catalogManager, null, storageManagerFactory.getStorageConfiguration(),
                storageManagerFactory.getStorageConfiguration().getDefaultStorageEngineId(), storageManagerFactory);
    }

    public StorageManager(CatalogManager catalogManager, CacheManager cacheManager, StorageConfiguration storageConfiguration) {
        this(catalogManager, cacheManager, storageConfiguration, storageConfiguration.getDefaultStorageEngineId(), null);
    }

    @Deprecated
    public StorageManager(CatalogManager catalogManager, StorageConfiguration storageConfiguration, String storageEngineId) {
        this(catalogManager, null, storageConfiguration, storageEngineId, null);
    }


    protected StorageManager(CatalogManager catalogManager, CacheManager cacheManager, StorageConfiguration storageConfiguration,
                        String storageEngineId, StorageManagerFactory storageManagerFactory) {
        this.catalogManager = catalogManager;
        this.cacheManager = cacheManager == null ? new CacheManager(storageConfiguration) : cacheManager;
        this.storageConfiguration = storageConfiguration;
        this.storageEngineId = storageEngineId;
        this.storageManagerFactory = storageManagerFactory == null
                ? StorageManagerFactory.get(storageConfiguration)
                : storageManagerFactory;
        logger = LoggerFactory.getLogger(getClass());
    }


    public void clearCache(String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);

    }


    public void clearCache(String studyId, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);

    }


    public abstract void testConnection() throws StorageManagerException;

    protected StudyInfo getStudyInfo(@Nullable String studyIdStr, String fileIdStr, String sessionId)
            throws CatalogException, IOException {
        return getStudyInfo(studyIdStr, Collections.singletonList(fileIdStr), sessionId);
    }

    protected StudyInfo getStudyInfo(@Nullable String studyIdStr, List<String> fileIdStrs, String sessionId)
            throws CatalogException, IOException {
        StudyInfo studyInfo = new StudyInfo().setSessionId(sessionId);

        String userId = catalogManager.getUserManager().getId(sessionId);
        studyInfo.setUserId(userId);

        long studyId = 0;
        if (StringUtils.isNotEmpty(studyIdStr)) {
            studyId = catalogManager.getStudyManager().getId(userId, studyIdStr);
        }

        List<FileInfo> fileInfos = new ArrayList<>(fileIdStrs.size());
        for (String fileIdStr : fileIdStrs) {
            FileInfo fileInfo = new FileInfo();
            long fileId;
            if (studyId > 0) {
                fileId = catalogManager.getFileManager().getId(fileIdStr, studyId, sessionId);
                if (fileId <= 0) {
                    throw new CatalogException("The id of file " + fileIdStr + " could not be found under study " + studyIdStr);
                }
            } else {
                fileId = catalogManager.getFileManager().getId(userId, fileIdStr);
                if (fileId <= 0) {
                    throw new CatalogException("The id of file " + fileIdStr + " could not be found");
                }
                studyId = catalogManager.getFileManager().getStudyId(fileId);
            }
            fileInfo.setFileId(fileId);

            // Get file path
            QueryOptions fileOptions = new QueryOptions(QueryOptions.INCLUDE,
                    Arrays.asList(FileDBAdaptor.QueryParams.URI.key(), FileDBAdaptor.QueryParams.NAME.key()));
            QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(fileId, fileOptions, sessionId);

            if (fileQueryResult.getNumResults() != 1) {
                logger.error("Critical error: File {} not found in catalog.", fileId);
                throw new CatalogException("Critical error: File " + fileId + " not found in catalog");
            }

            Path path = Paths.get(fileQueryResult.first().getUri().getRawPath());
            FileUtils.checkFile(path);

            fileInfo.setPath(path);
            fileInfo.setName(fileQueryResult.first().getName());


            fileInfos.add(fileInfo);
        }
        studyInfo.setFileInfos(fileInfos);

        QueryOptions studyOptions = new QueryOptions();
//        studyOptions.put(QueryOptions.INCLUDE,
//                Arrays.asList(StudyDBAdaptor.QueryParams.URI.key(), StudyDBAdaptor.QueryParams.ALIAS.key(),
//                        StudyDBAdaptor.QueryParams.DATASTORES.key()));
        QueryResult<Study> studyQueryResult = catalogManager.getStudyManager().get(studyId, studyOptions, sessionId);
        if (studyQueryResult .getNumResults() != 1) {
            logger.error("Critical error: Study {} not found in catalog.", studyId);
            throw new CatalogException("Critical error: Study " + studyId + " not found in catalog");
        }
        Study study = studyQueryResult.first();
        studyInfo.setStudy(study);

//        Path workspace = Paths.get(study.getUri().getRawPath()).resolve(".opencga").resolve("alignments");
//        if (!workspace.toFile().exists()) {
//            Files.createDirectories(workspace);
//        }
//        studyInfo.setWorkspace(workspace);
        Map<File.Bioformat, DataStore> dataStores = new HashMap<>();
        dataStores.put(File.Bioformat.VARIANT, StorageOperation.getDataStore(catalogManager, study, File.Bioformat.VARIANT, sessionId));
        dataStores.put(File.Bioformat.ALIGNMENT, StorageOperation.getDataStore(catalogManager, study, File.Bioformat.ALIGNMENT, sessionId));
        studyInfo.setDataStores(dataStores);

        return studyInfo;
    }

    /**
     * Given the file and study string, retrieve the corresponding long ids.
     *
     * @param studyIdStr study string.
     * @param fileIdStr file string.
     * @param sessionId session id.
     * @return an objectMap containing the keys "fileId" and "studyId"
     * @throws CatalogException catalog exception.
     */
    @Deprecated
    protected ObjectMap getFileAndStudyId(@Nullable String studyIdStr, String fileIdStr, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = 0;
        if (StringUtils.isNotEmpty(studyIdStr)) {
            studyId = catalogManager.getStudyManager().getId(userId, studyIdStr);
        }

        long fileId;
        if (studyId > 0) {
            fileId = catalogManager.getFileManager().getId(userId, studyId, fileIdStr);
            if (fileId <= 0) {
                throw new CatalogException("The id of file " + fileIdStr + " could not be found under study " + studyIdStr);
            }
        } else {
            fileId = catalogManager.getFileManager().getId(userId, fileIdStr);
            if (fileId <= 0) {
                throw new CatalogException("The id of file " + fileIdStr + " could not be found");
            }
            studyId = catalogManager.getFileManager().getStudyId(fileId);
        }

        return new ObjectMap("fileId", fileId).append("studyId", studyId);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StorageManager{");
        sb.append("catalogManager=").append(catalogManager);
        sb.append(", cacheManager=").append(cacheManager);
        sb.append(", storageConfiguration=").append(storageConfiguration);
        sb.append(", storageEngineId='").append(storageEngineId).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }

    public StorageConfiguration getStorageConfiguration() {
        return storageConfiguration;
    }
}
