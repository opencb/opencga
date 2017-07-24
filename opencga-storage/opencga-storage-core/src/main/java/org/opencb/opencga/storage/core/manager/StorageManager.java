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

package org.opencb.opencga.storage.core.manager;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.DataStore;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.cache.CacheManager;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.models.FileInfo;
import org.opencb.opencga.storage.core.manager.models.StudyInfo;
import org.opencb.opencga.storage.core.manager.variant.operations.StorageOperation;
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
    protected final StorageEngineFactory storageEngineFactory;

    protected final Logger logger;

    public StorageManager(Configuration configuration, StorageConfiguration storageConfiguration) throws CatalogException {
        this(new CatalogManager(configuration), StorageEngineFactory.get(storageConfiguration));
    }

    public StorageManager(CatalogManager catalogManager, StorageEngineFactory storageEngineFactory) {
        this(catalogManager, null, storageEngineFactory.getStorageConfiguration(),
                storageEngineFactory);
    }

    protected StorageManager(CatalogManager catalogManager, CacheManager cacheManager, StorageConfiguration storageConfiguration,
                             StorageEngineFactory storageEngineFactory) {
        this.catalogManager = catalogManager;
        this.cacheManager = cacheManager == null ? new CacheManager(storageConfiguration) : cacheManager;
        this.storageConfiguration = storageConfiguration;
        this.storageEngineFactory = storageEngineFactory == null
                ? StorageEngineFactory.get(storageConfiguration)
                : storageEngineFactory;
        logger = LoggerFactory.getLogger(getClass());
    }


    public void clearCache(String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);

    }


    public void clearCache(String studyId, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);

    }


    public abstract void testConnection() throws StorageEngineException;

    protected StudyInfo getStudyInfo(@Nullable String studyIdStr, String fileIdStr, String sessionId)
            throws CatalogException, IOException {
        return getStudyInfo(studyIdStr, Collections.singletonList(fileIdStr), sessionId);
    }

    protected StudyInfo getStudyInfo(@Nullable String studyIdStr, List<String> fileIdStrs, String sessionId)
            throws CatalogException, IOException {
        StudyInfo studyInfo = new StudyInfo().setSessionId(sessionId);

        List<Long> fileIds;
        Long studyId;
        if (fileIdStrs.isEmpty()) {
            fileIds = Collections.emptyList();
            studyId = catalogManager.getStudyId(studyIdStr, sessionId);
        } else {
            AbstractManager.MyResourceIds resource = catalogManager.getFileManager().getIds(StringUtils.join(fileIdStrs, ","), studyIdStr,
                    sessionId);
            fileIds = resource.getResourceIds();
            studyId = resource.getStudyId();
        }
        List<FileInfo> fileInfos = new ArrayList<>(fileIdStrs.size());
        for (long fileId : fileIds) {
            FileInfo fileInfo = new FileInfo();
            fileInfo.setFileId(fileId);

            // Get file path
            QueryOptions fileOptions = new QueryOptions(QueryOptions.INCLUDE,
                    Arrays.asList(FileDBAdaptor.QueryParams.URI.key(), FileDBAdaptor.QueryParams.NAME.key(),
                            FileDBAdaptor.QueryParams.BIOFORMAT.key(), FileDBAdaptor.QueryParams.FORMAT.key()));
            QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(fileId, fileOptions, sessionId);

            if (fileQueryResult.getNumResults() != 1) {
                logger.error("Critical error: File {} not found in catalog.", fileId);
                throw new CatalogException("Critical error: File " + fileId + " not found in catalog");
            }

            Path path = Paths.get(fileQueryResult.first().getUri().getRawPath());
            // Do not check file! Input may be a folder in some scenarios
//            FileUtils.checkFile(path);

            fileInfo.setPath(path);
            fileInfo.setName(fileQueryResult.first().getName());
            fileInfo.setBioformat(fileQueryResult.first().getBioformat());
            fileInfo.setFormat(fileQueryResult.first().getFormat());

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
        long projectId = catalogManager.getProjectIdByStudyId(study.getId());
        Project project = catalogManager.getProject(projectId, new QueryOptions(), sessionId).first();
        studyInfo.setProjectId(project.getId());
        studyInfo.setProjectAlias(project.getAlias());
        studyInfo.setOrganism(project.getOrganism());
        String user = catalogManager.getUserIdByProjectId(project.getId());
        studyInfo.setUserId(user);

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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StorageEngine{");
        sb.append("catalogManager=").append(catalogManager);
        sb.append(", cacheManager=").append(cacheManager);
        sb.append(", storageConfiguration=").append(storageConfiguration);
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
