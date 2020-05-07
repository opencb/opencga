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

package org.opencb.opencga.analysis;

import org.opencb.biodata.models.alignment.LowCoverageRegion;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.project.DataStore;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.cache.CacheManager;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.analysis.models.FileInfo;
import org.opencb.opencga.analysis.models.StudyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.analysis.variant.manager.VariantStorageManager.getDataStore;


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
        this(catalogManager, null, storageEngineFactory.getStorageConfiguration(), storageEngineFactory);
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
        String userId = catalogManager.getUserManager().getUserId(sessionId);

    }


    public void clearCache(String studyId, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);

    }


    public abstract void testConnection() throws StorageEngineException;

    @Deprecated
    protected StudyInfo getStudyInfo(@Nullable String studyIdStr, String fileIdStr, String sessionId)
            throws CatalogException, IOException {
        return getStudyInfo(studyIdStr, Collections.singletonList(fileIdStr), sessionId);
    }

    @Deprecated
    protected StudyInfo getStudyInfo(@Nullable String studyIdStr, List<String> fileIdStrs, String sessionId)
            throws CatalogException {
        StudyInfo studyInfo = new StudyInfo().setSessionId(sessionId);

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyIdStr, userId);

        List<File> files;
        if (fileIdStrs.isEmpty()) {
            files = Collections.emptyList();
        } else {
            DataResult<File> queryResult = catalogManager.getFileManager().get(studyIdStr, fileIdStrs, null, sessionId);
            files = queryResult.getResults();
        }
        List<FileInfo> fileInfos = new ArrayList<>(fileIdStrs.size());
        for (File file : files) {
            FileInfo fileInfo = new FileInfo();
            fileInfo.setFileUid(file.getUid());

            Path path = Paths.get(file.getUri().getRawPath());
            // Do not check file! Input may be a folder in some scenarios
//            FileUtils.checkFile(path);

            fileInfo.setPath(file.getPath());
            fileInfo.setFilePath(path);
            fileInfo.setName(file.getName());
            fileInfo.setBioformat(file.getBioformat());
            fileInfo.setFormat(file.getFormat());

            fileInfos.add(fileInfo);
        }
        studyInfo.setFileInfos(fileInfos);

        studyInfo.setStudy(study);
        String projectFqn = catalogManager.getStudyManager().getProjectFqn(study.getFqn());
        Project project = catalogManager.getProjectManager().get(new Query(ProjectDBAdaptor.QueryParams.FQN.key(), projectFqn),
                new QueryOptions(), sessionId).first();
        studyInfo.setProjectUid(project.getUid());
        studyInfo.setProjectId(project.getId());
        studyInfo.setOrganism(project.getOrganism());
        String user = catalogManager.getProjectManager().getOwner(project.getUid());
        studyInfo.setUserId(user);

        Map<File.Bioformat, DataStore> dataStores = new HashMap<>();
        dataStores.put(File.Bioformat.VARIANT, getDataStore(catalogManager, study.getFqn(), File.Bioformat.VARIANT, sessionId));
        dataStores.put(File.Bioformat.ALIGNMENT, getDataStore(catalogManager, study.getFqn(), File.Bioformat.ALIGNMENT, sessionId));
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
